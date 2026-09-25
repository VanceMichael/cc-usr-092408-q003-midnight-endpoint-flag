"""处理事件接入语义、持久化协调与业务查询的应用服务。"""

from __future__ import annotations

import json
from datetime import datetime, timezone
from typing import Any

from app.errors import EventConflictError, NotFoundError, ValidationError
from app.engine import compute_impacts
from app.models import (
    CALC_VERSION,
    EVENT_CLOSED,
    EVENT_EXTENDED,
    EVENT_REOPENED,
    Airport,
    DisruptionEvent,
    Flight,
    iso_utc,
)
from app.repository import Repository
from app.validation import validate_event


class DisruptionService:
    def __init__(
        self,
        repo: Repository,
        airports: dict[str, Airport],
        flights: dict[str, Flight],
    ):
        self._repo = repo
        self._airports = airports
        self._flights = flights
        # 旧库升级后，先把历史快照按当前半开语义更正并留痕，再对外服务。
        self._backfill_legacy_projections()

    def healthy(self) -> bool:
        return self._repo.ping()

    # ------------------------------------------------------------------ #
    # Legacy projection backfill (auditable migration)
    # ------------------------------------------------------------------ #

    def _backfill_legacy_projections(self) -> None:
        """把 v1 历史快照重算为当前裁定版本，旧快照原样保留并逐差异留痕。

        v1 的跨午夜判定把“末端恰为本地午夜”的窗口误标为跨日，且开放窗口
        被写成 0（伪造了结束日）。重算按机场事件链顺序进行：v2 行与 v1 行
        并存，``events.calc_version`` 指针在更正完成后才切到 v2，因此中途
        失败回滚不会让任何当前读到半成品。
        """
        if not self._repo.projection_backfill_required():
            return
        with self._repo.transaction() as conn:
            rows = self._repo.all_event_rows()
            # 链内事件按 (机场, 版本) 顺序到达，保证墓碑能看到同链 v2 先行行。
            for row in rows:
                if int(row["calc_version"]) >= CALC_VERSION:
                    continue
                event = _row_to_event(row)
                airport = self._airports[event.airport_code]
                root = self._resolve_root(conn, event)
                verdict, impacts = compute_impacts(
                    event, root, airport, self._flights
                )
                impacts.extend(
                    self._resolved_tombstones(conn, event, root, impacts, verdict)
                )
                self._repo.insert_window_verdict(conn, verdict.to_row(event.event_id))
                if impacts:
                    self._repo.insert_impacts(conn, impacts)

                # 审计留痕：只比较活动行（墓碑不对外暴露，不参与“错误影响”）。
                legacy = {
                    r["flight_id"]: r
                    for r in self._repo.legacy_impacts(conn, event.event_id)
                    if r["impact_status"] != "resolved"
                }
                for record in impacts:
                    if record["impact_status"] == "resolved":
                        continue
                    old = legacy.get(record["flight_id"])
                    if old is None:
                        continue
                    old_flag = old["crosses_midnight"]
                    new_flag = record["crosses_midnight"]
                    if _flag_differs(old_flag, new_flag):
                        self._repo.insert_amendment(
                            conn,
                            event_id=event.event_id,
                            flight_id=record["flight_id"],
                            airport_code=event.airport_code,
                            field_name="crosses_midnight",
                            old_value=old_flag,
                            new_value=new_flag,
                            reason=(
                                "open-ended closure: cross-midnight stays "
                                "unconfirmed until an end date is known"
                                if new_flag is None
                                else "half-open window [start, end): an end "
                                     "exactly at local midnight is not inside "
                                     "the closure"
                            ),
                        )
                self._repo.mark_event_projection(conn, event.event_id, CALC_VERSION)
            self._repo.record_projection_backfill(conn)

    # ------------------------------------------------------------------ #
    # Event intake
    # ------------------------------------------------------------------ #

    def submit_event(self, payload: Any) -> dict[str, Any]:
        # Structural + basic semantic validation happens before any DB write.
        event = validate_event(payload, self._airports)

        with self._repo.transaction() as conn:
            existing = conn.execute(
                "SELECT event_version, payload_json, calc_version FROM events "
                "WHERE event_id = ?",
                (event.event_id,),
            ).fetchone()

            if existing is not None:
                return self._handle_duplicate(conn, event, existing)

            self._validate_chain(conn, event)
            root = self._resolve_root(conn, event)
            verdict, impacts = compute_impacts(
                event,
                root,
                self._airports[event.airport_code],
                self._flights,
            )
            impacts.extend(
                self._resolved_tombstones(conn, event, root, impacts, verdict)
            )
            self._repo.insert_event(conn, event.to_dict())
            self._repo.insert_window_verdict(conn, verdict.to_row(event.event_id))
            if impacts:
                self._repo.insert_impacts(conn, impacts)
            return self._result(event, impacts, replayed=False)

    def _resolved_tombstones(
        self,
        conn,
        event: DisruptionEvent,
        root: DisruptionEvent,
        impacts: list[dict],
        verdict,
    ) -> list[dict[str, Any]]:
        """返回本事件生效后不再受影响、但曾出现在同链中的航班。"""
        if event.event_type == EVENT_CLOSED:
            return []
        still_affected = {r["flight_id"] for r in impacts}
        tombstones: list[dict[str, Any]] = []
        prior_ids = self._repo.prior_chain_impact_ids(
            conn, root.event_id, verdict.calc_version
        )
        for flight_id in sorted(prior_ids - still_affected):
            flight = self._flights.get(flight_id)
            if flight is None:
                continue
            tombstones.append(
                {
                    "event_id": event.event_id,
                    "root_event_id": root.event_id,
                    "airport_code": event.airport_code,
                    "flight_id": flight.flight_id,
                    "flight_number": flight.flight_number,
                    "affected_endpoint": "none",
                    "impact_status": "resolved",
                    "overlap_minutes": 0,
                    "delay_minutes": None,
                    "proposed_departure": None,
                    "proposed_arrival": None,
                    "passenger_count": flight.passenger_count,
                    "crosses_midnight": (
                        None
                        if verdict.crosses_midnight is None
                        else int(verdict.crosses_midnight)
                    ),
                    "calc_version": verdict.calc_version,
                }
            )
        return tombstones

    def _handle_duplicate(
        self, conn, event: DisruptionEvent, existing
    ) -> dict[str, Any]:
        stored_version = existing["event_version"]
        stored_payload = json.loads(existing["payload_json"])
        projection_version = int(existing["calc_version"])

        same_body = stored_payload == event.to_dict()
        if same_body:
            # Idempotent retry: return the original result, bump the counter.
            self._repo.increment_replay(conn, event.event_id)
            impacts = self._repo.get_impacts(event.event_id, projection_version)
            return self._result(
                event, [dict(r) for r in impacts], replayed=True,
                projection_version=projection_version,
            )

        # Same identity, different content.
        if event.event_version == stored_version:
            raise EventConflictError(
                f"Event '{event.event_id}' version {stored_version} already exists "
                "with a different payload",
                {
                    "event_id": event.event_id,
                    "stored_version": stored_version,
                    "received_version": event.event_version,
                    "issue": "payload_mismatch",
                },
            )
        raise EventConflictError(
            f"Event '{event.event_id}' already exists at version {stored_version}; "
            "new versions must use a new event_id and reference the previous one "
            "via supersedes_event_id",
            {
                "event_id": event.event_id,
                "stored_version": stored_version,
                "received_version": event.event_version,
                "issue": "event_id_reuse",
            },
        )

    def _validate_chain(self, conn, event: DisruptionEvent) -> None:
        """校验必须结合数据库现状判断的事件链规则。"""
        errors: list[dict[str, str]] = []

        def prior_versions(airport: str) -> list[int]:
            rows = conn.execute(
                "SELECT event_version FROM events WHERE airport_code = ?", (airport,)
            ).fetchall()
            return [r["event_version"] for r in rows]

        if event.event_type == EVENT_CLOSED:
            versions = prior_versions(event.airport_code)
            if versions and event.event_version <= max(versions):
                errors.append(
                    {
                        "field": "event_version",
                        "issue": "must_extend_airport_history",
                        "stored_version": str(max(versions)),
                        "received_version": str(event.event_version),
                    }
                )
            return

        # extended / reopened must reference a prior event
        ref_id = event.supersedes_event_id
        ref = None
        if ref_id is not None:
            ref = conn.execute(
                "SELECT * FROM events WHERE event_id = ?", (ref_id,)
            ).fetchone()
            if ref is None:
                errors.append(
                    {
                        "field": "supersedes_event_id",
                        "issue": "unknown_event",
                        "event_id": ref_id,
                    }
                )
            elif ref["airport_code"] != event.airport_code:
                errors.append(
                    {
                        "field": "supersedes_event_id",
                        "issue": "airport_mismatch",
                        "referenced_airport": ref["airport_code"],
                        "received_airport": event.airport_code,
                    }
                )
            elif ref["event_type"] == EVENT_REOPENED:
                errors.append(
                    {
                        "field": "supersedes_event_id",
                        "issue": "chain_already_closed",
                        "event_id": ref_id,
                    }
                )

        if event.event_type == EVENT_EXTENDED and ref is not None and not errors:
            # An extension continues a still-active closure. When the previous
            # window had a known end, the extension must start no later than it
            # (no unmodelled open gap) and push the end further out. Extending
            # an open-ended closure simply supplies the newly known end.
            prev_until_raw = ref["effective_until"]
            if prev_until_raw is None:
                if event.effective_from < parse_ts(ref["effective_from"]):
                    errors.append(
                        {
                            "field": "effective_from",
                            "issue": "must_not_precede_chain_start",
                        }
                    )
            else:
                prev_until = parse_ts(prev_until_raw)
                if event.effective_from > prev_until:
                    errors.append(
                        {
                            "field": "effective_from",
                            "issue": "extension_leaves_uncovered_gap",
                        }
                    )
                if event.effective_until <= prev_until:
                    errors.append(
                        {
                            "field": "effective_until",
                            "issue": "must_extend_previous_window",
                        }
                    )
            if event.event_version <= ref["event_version"]:
                errors.append(
                    {
                        "field": "event_version",
                        "issue": "version_must_increase",
                        "stored_version": str(ref["event_version"]),
                        "received_version": str(event.event_version),
                    }
                )

        if event.event_type == EVENT_REOPENED and ref is not None and not errors:
            if event.event_version <= ref["event_version"]:
                errors.append(
                    {
                        "field": "event_version",
                        "issue": "version_must_increase",
                        "stored_version": str(ref["event_version"]),
                        "received_version": str(event.event_version),
                    }
                )

        if errors:
            raise ValidationError("Event failed chain validation", {"errors": errors})

    def _resolve_root(self, conn, event: DisruptionEvent) -> DisruptionEvent:
        if event.event_type == EVENT_CLOSED:
            return event
        # Walk the supersedes chain to the originating closed event.
        seen: set[str] = set()
        current = event
        while current.supersedes_event_id is not None:
            ref_id = current.supersedes_event_id
            if ref_id in seen:  # defensive; cycles are structurally prevented
                raise ValidationError("supersedes chain contains a cycle")
            seen.add(ref_id)
            row = conn.execute(
                "SELECT * FROM events WHERE event_id = ?", (ref_id,)
            ).fetchone()
            if row is None:  # validated earlier; defensive
                raise ValidationError(f"unknown superseded event '{ref_id}'")
            current = _row_to_event(row)
            if current.event_type == EVENT_CLOSED:
                return current
        raise ValidationError("extended/reopened event chain has no closed root")

    def _resolve_root_readonly(self, event: DisruptionEvent) -> DisruptionEvent:
        """只读路径下沿 supersede 链回溯根事件（不开写事务）。"""
        if event.event_type == EVENT_CLOSED:
            return event
        seen: set[str] = set()
        current = event
        while current.supersedes_event_id is not None:
            ref_id = current.supersedes_event_id
            if ref_id in seen:
                raise ValidationError("supersedes chain contains a cycle")
            seen.add(ref_id)
            row = self._repo.get_event_row(ref_id)
            if row is None:
                raise ValidationError(f"unknown superseded event '{ref_id}'")
            current = _row_to_event(row)
            if current.event_type == EVENT_CLOSED:
                return current
        raise ValidationError("extended/reopened event chain has no closed root")

    # ------------------------------------------------------------------ #
    # Queries
    # ------------------------------------------------------------------ #

    def event_status(
        self, event_id: str, *, projection_version: int | None = None
    ) -> dict[str, Any]:
        row = self._repo.get_event_row(event_id)
        if row is None:
            raise NotFoundError(
                f"Event '{event_id}' was not found", {"event_id": event_id}
            )
        current_version = int(row["calc_version"])
        version = current_version if projection_version is None else projection_version
        available = self._repo.available_projection_versions(event_id)
        if version not in available:
            raise ValidationError(
                f"Projection version {version} is not available for event "
                f"'{event_id}'",
                {
                    "event_id": event_id,
                    "requested_version": version,
                    "available_versions": available,
                },
            )
        impact_rows = self._repo.get_impacts(event_id, version)
        impacts = [self._impact_dict(r) for r in impact_rows]
        active = [i for i in impacts if i["impact_status"] != "resolved"]
        statuses: dict[str, int] = {}
        passengers = 0
        for imp in active:
            statuses[imp["impact_status"]] = statuses.get(imp["impact_status"], 0) + 1
            passengers += imp["passenger_count"]
        return {
            "event": json.loads(row["payload_json"]),
            "processing": {
                "state": "processed",
                "replay_count": row["replay_count"],
                "created_at": row["created_at"],
                "impact_count": len(active),
                "resolved_count": len(impacts) - len(active),
                "affected_passengers": passengers,
                "status_breakdown": statuses,
                "projection_version": version,
                "current_projection_version": current_version,
                "available_projection_versions": available,
                "window_verdict": self._window_verdict_dict(row, version, impact_rows),
            },
            "impacts": active,
        }

    def _window_verdict_dict(
        self, event_row, version: int, impact_rows: list[Any]
    ) -> dict[str, Any]:
        """返回与所选影响行同一计算版本的窗口裁定。"""
        verdict_row = self._repo.get_window_verdict(event_row["event_id"], version)
        if verdict_row is not None:
            return {
                "window_start": verdict_row["window_start"],
                "window_end": verdict_row["window_end"],
                "airport_timezone": verdict_row["airport_timezone"],
                "crosses_midnight": _nullable_flag(verdict_row["crosses_midnight"]),
                "calc_version": int(verdict_row["calc_version"]),
            }
        # v1 没有独立裁定表：标志逐行冗余在影响上（同事件取值一致），
        # 窗口边界按事件链几何重建（v1->v2 仅改变标志语义，未改变边界）。
        event = _row_to_event(event_row)
        airport = self._airports[event.airport_code]
        root = self._resolve_root_readonly(event)
        from app.engine import chain_window

        window = chain_window(event, root, airport)
        # v1 没有事件级裁定：以“活动行”的标志为准（墓碑在 v1 被硬编码为 0，
        # 不代表当时的窗口裁定）；若全部已 resolved，则无客户可见标志。
        active_flags = {
            r["crosses_midnight"] for r in impact_rows if r["impact_status"] != "resolved"
        }
        if len(active_flags) == 1:
            flag = _nullable_flag(next(iter(active_flags)))
        else:
            flag = None
        return {
            "window_start": iso_utc(window.start),
            "window_end": iso_utc(window.end) if window.end else None,
            "airport_timezone": airport.timezone,
            "crosses_midnight": flag,
            "calc_version": version,
        }

    def projection_amendments(self, event_id: str | None = None) -> list[dict[str, Any]]:
        """可审计的历史裁定更正记录（迁移留痕）。"""
        return self._repo.amendments(event_id)

    def airport_summary(
        self, airport_code: str, *, projection_version: int | None = None
    ) -> dict[str, Any]:
        if airport_code not in self._airports:
            raise NotFoundError(
                f"Unknown airport code '{airport_code}'",
                {"field": "airport_code", "received": airport_code},
            )
        rows = self._repo.events_for_airport(airport_code)
        latest = self._repo.latest_impacts(
            airport=airport_code, projection_version=projection_version
        )
        by_status: dict[str, dict[str, Any]] = {}
        total_passengers = 0
        for r in latest:
            bucket = by_status.setdefault(
                r["impact_status"],
                {"flight_count": 0, "passenger_count": 0, "flights": []},
            )
            bucket["flight_count"] += 1
            bucket["passenger_count"] += r["passenger_count"]
            total_passengers += r["passenger_count"]
            bucket["flights"].append(r["flight_id"])
        chain_roots = [r["event_id"] for r in rows if r["event_type"] == EVENT_CLOSED]
        return {
            "airport_code": airport_code,
            "airport_name": self._airports[airport_code].name,
            "event_count": len(rows),
            "active_chains": len(chain_roots)
            - sum(1 for r in rows if r["event_type"] == EVENT_REOPENED),
            "affected_flights": len(latest),
            "affected_passengers": total_passengers,
            "by_status": by_status,
            "projection_version": projection_version or CALC_VERSION,
        }

    def affected_flights(
        self,
        *,
        airport: str | None,
        status: str | None,
        limit: int,
        offset: int,
        projection_version: int | None = None,
    ) -> dict[str, Any]:
        if airport is not None and airport not in self._airports:
            raise NotFoundError(
                f"Unknown airport code '{airport}'",
                {"field": "airport", "received": airport},
            )
        allowed = {"cancelled", "delayed", "pending_confirmation"}
        if status is not None and status not in allowed:
            raise ValidationError(
                "Unsupported impact status filter",
                {"field": "status", "allowed": sorted(allowed)},
            )
        rows = self._repo.latest_impacts(
            airport=airport, status=status, projection_version=projection_version
        )
        total = len(rows)
        page = rows[offset : offset + limit]
        return {
            "pagination": {
                "limit": limit,
                "offset": offset,
                "total": total,
                "projection_version": projection_version or CALC_VERSION,
            },
            "flights": [self._impact_dict(r) for r in page],
        }

    # ------------------------------------------------------------------ #
    # Serialization helpers
    # ------------------------------------------------------------------ #

    def _result(
        self,
        event: DisruptionEvent,
        impacts: list[dict[str, Any]],
        *,
        replayed: bool,
        projection_version: int = CALC_VERSION,
    ) -> dict[str, Any]:
        active_impacts = [i for i in impacts if i["impact_status"] != "resolved"]
        serialized = [self._impact_dict(i) for i in active_impacts]
        statuses: dict[str, int] = {}
        passengers = 0
        for imp in serialized:
            statuses[imp["impact_status"]] = statuses.get(imp["impact_status"], 0) + 1
            passengers += imp["passenger_count"]
        return {
            "event_id": event.event_id,
            "event_version": event.event_version,
            "processing_state": "replayed" if replayed else "processed",
            "projection_version": projection_version,
            "impact_count": len(serialized),
            "resolved_count": len(impacts) - len(active_impacts),
            "affected_passengers": passengers,
            "status_breakdown": statuses,
            "impacts": serialized,
        }

    @staticmethod
    def _impact_dict(row: Any) -> dict[str, Any]:
        if not isinstance(row, dict):
            row = dict(row)
        return {
            "event_id": row["event_id"],
            "root_event_id": row["root_event_id"],
            "airport_code": row["airport_code"],
            "flight_id": row["flight_id"],
            "flight_number": row["flight_number"],
            "affected_endpoint": row["affected_endpoint"],
            "impact_status": row["impact_status"],
            "overlap_minutes": row["overlap_minutes"],
            "delay_minutes": row["delay_minutes"],
            "proposed_departure": row["proposed_departure"],
            "proposed_arrival": row["proposed_arrival"],
            "passenger_count": row["passenger_count"],
            # 三态：开放窗口结束日未知 -> null，绝不伪造成 true/false。
            "crosses_midnight": _nullable_flag(row["crosses_midnight"]),
            "projection_version": int(row["calc_version"]),
        }


def _nullable_flag(value: Any) -> bool | None:
    """IN NULL（开放窗口）保留为 None，其余按 0/1 转布尔。"""
    if value is None:
        return None
    return bool(value)


def _flag_differs(old: Any, new: Any) -> bool:
    """比较两代裁定的跨午夜标志（均可能为 None）。"""
    norm = lambda v: None if v is None else int(v)
    return norm(old) != norm(new)


def parse_ts(value: str) -> datetime:
    text = value[:-1] + "+00:00" if value.endswith("Z") else value
    return datetime.fromisoformat(text).astimezone(timezone.utc)


def _row_to_event(row) -> DisruptionEvent:
    return DisruptionEvent(
        event_id=row["event_id"],
        event_version=row["event_version"],
        event_type=row["event_type"],
        airport_code=row["airport_code"],
        effective_from=parse_ts(row["effective_from"]),
        effective_until=parse_ts(row["effective_until"]) if row["effective_until"] else None,
        reported_at=parse_ts(row["reported_at"]),
        supersedes_event_id=row["supersedes_event_id"],
        reason=row["reason"],
    )
