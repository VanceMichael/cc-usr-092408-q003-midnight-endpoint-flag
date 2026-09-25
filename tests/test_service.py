"""覆盖幂等、冲突、事件链、查询与重启语义的服务测试。"""

from __future__ import annotations

import json
import sqlite3
import threading
import unittest
from pathlib import Path

from app.engine import CURRENT_PROJECTION_VERSION
from app.errors import (
    AppError,
    EventConflictError,
    NotFoundError,
    ValidationError,
)
from app.repository import Repository
from app.service import DisruptionService
from tests.support import ServiceTestCase, base_event


def close(event_id="evt-close0000001", airport="APS", **kw) -> dict:
    payload = base_event(event_id=event_id, airport_code=airport)
    payload.update(kw)
    return payload


def extend(event_id, supersedes, version=2, airport="APS", **kw) -> dict:
    payload = {
        "event_id": event_id,
        "event_version": version,
        "event_type": "airport.extended",
        "airport_code": airport,
        "effective_from": kw.pop("effective_from", "2026-09-07T15:50:00Z"),
        "effective_until": kw.pop("effective_until", "2026-09-07T20:00:00Z"),
        "reported_at": "2026-09-07T14:30:00Z",
        "supersedes_event_id": supersedes,
    }
    payload.update(kw)
    return payload


def reopen(event_id, supersedes, version=3, airport="APS", **kw) -> dict:
    payload = {
        "event_id": event_id,
        "event_version": version,
        "event_type": "airport.reopened",
        "airport_code": airport,
        "effective_from": kw.pop("effective_from", "2026-09-07T16:00:00Z"),
        "effective_until": None,
        "reported_at": "2026-09-07T15:00:00Z",
        "supersedes_event_id": supersedes,
    }
    payload.update(kw)
    return payload


class IdempotencyTest(ServiceTestCase):
    def test_duplicate_submission_replays_original_result(self) -> None:
        payload = close()
        first = self.service.submit_event(payload)
        second = self.service.submit_event(dict(payload))
        self.assertEqual(first["event_id"], second["event_id"])
        self.assertEqual(first["impacts"], second["impacts"])
        self.assertEqual(first["processing_state"], "processed")
        self.assertEqual(second["processing_state"], "replayed")

        status = self.service.event_status(payload["event_id"])
        self.assertEqual(status["processing"]["replay_count"], 1)
        self.assertEqual(status["impacts"], first["impacts"])

    def test_duplicate_with_changed_payload_conflicts(self) -> None:
        payload = close()
        self.service.submit_event(payload)
        changed = dict(payload)
        changed["reason"] = "different reason text"
        with self.assertRaises(EventConflictError) as ctx:
            self.service.submit_event(changed)
        self.assertEqual(ctx.exception.code, "event_conflict")
        # Original result must be untouched.
        status = self.service.event_status(payload["event_id"])
        self.assertEqual(status["event"]["reason"], "volcanic ash")
        self.assertEqual(status["processing"]["replay_count"], 0)

    def test_reused_id_with_other_version_conflicts(self) -> None:
        payload = close()
        self.service.submit_event(payload)
        changed = dict(payload)
        changed["event_version"] = 2
        with self.assertRaises(EventConflictError):
            self.service.submit_event(changed)


class ChainTest(ServiceTestCase):
    def test_extend_requires_known_supersedes(self) -> None:
        self.service.submit_event(close())
        bad = extend("evt-extend000001", "evt-doesnotexist")
        with self.assertRaises(ValidationError) as ctx:
            self.service.submit_event(bad)
        issues = [e["issue"] for e in ctx.exception.details["errors"]]
        self.assertIn("unknown_event", issues)

    def test_extend_airport_mismatch_rejected(self) -> None:
        self.service.submit_event(close(airport="APS"))
        bad = extend("evt-extend000001", "evt-close0000001", airport="BSR")
        with self.assertRaises(ValidationError) as ctx:
            self.service.submit_event(bad)
        self.assertIn("airport_mismatch", str(ctx.exception.details))

    def test_extend_must_push_window_end(self) -> None:
        self.service.submit_event(close())
        bad = extend(
            "evt-extend000001",
            "evt-close0000001",
            effective_from="2026-09-07T15:50:00Z",
            effective_until="2026-09-07T18:00:00Z",
        )
        with self.assertRaises(ValidationError) as ctx:
            self.service.submit_event(bad)
        self.assertIn("must_extend_previous_window", str(ctx.exception.details))

    def test_extension_leaving_gap_rejected(self) -> None:
        self.service.submit_event(
            close(effective_until="2026-09-07T16:00:00Z")
        )
        bad = extend(
            "evt-extend000001",
            "evt-close0000001",
            effective_from="2026-09-07T16:30:00Z",
            effective_until="2026-09-07T19:00:00Z",
        )
        with self.assertRaises(ValidationError) as ctx:
            self.service.submit_event(bad)
        self.assertIn("extension_leaves_uncovered_gap", str(ctx.exception.details))

    def test_version_must_increase_along_chain(self) -> None:
        self.service.submit_event(close())
        bad = extend("evt-extend000001", "evt-close0000001", version=1)
        with self.assertRaises(ValidationError) as ctx:
            self.service.submit_event(bad)
        self.assertIn("version_must_increase", str(ctx.exception.details))

    def test_cannot_extend_reopened_chain(self) -> None:
        self.service.submit_event(close())
        self.service.submit_event(reopen("evt-reopen000001", "evt-close0000001"))
        bad = extend(
            "evt-extend000001", "evt-reopen000001", version=4
        )
        with self.assertRaises(ValidationError) as ctx:
            self.service.submit_event(bad)
        self.assertIn("chain_already_closed", str(ctx.exception.details))

    def test_full_chain_recompute_and_resolve(self) -> None:
        # Closure 16:20-17:00Z: AX412 (16:30 dep, cannot retime) is cancelled,
        # BY205 (16:45 arrival) needs 15 min and can retime -> delayed.
        first = self.service.submit_event(
            close(
                effective_from="2026-09-07T16:20:00Z",
                effective_until="2026-09-07T17:00:00Z",
            )
        )
        affected = {i["flight_id"] for i in first["impacts"]}
        self.assertEqual(affected, {"AX-412-20260908", "BY-205-20260908"})
        by_status = {i["flight_id"]: i["impact_status"] for i in first["impacts"]}
        self.assertEqual(by_status["AX-412-20260908"], "cancelled")
        self.assertEqual(by_status["BY-205-20260908"], "delayed")

        # Extend to 19:30Z: the union window [16:20, 19:30) is recomputed, so
        # BY205's required hold grows past its 90 min retime limit -> it flips
        # delayed -> cancelled, and KX099 (arrives 19:15) becomes delayed.
        extended = self.service.submit_event(
            extend(
                "evt-extend000001",
                "evt-close0000001",
                effective_from="2026-09-07T16:55:00Z",
                effective_until="2026-09-07T19:30:00Z",
            )
        )
        ext_status = {i["flight_id"]: i for i in extended["impacts"]}
        self.assertEqual(
            set(ext_status), {"AX-412-20260908", "BY-205-20260908", "KX-099-20260908"}
        )
        self.assertEqual(ext_status["AX-412-20260908"]["impact_status"], "cancelled")
        self.assertEqual(ext_status["BY-205-20260908"]["impact_status"], "cancelled")
        self.assertEqual(ext_status["KX-099-20260908"]["impact_status"], "delayed")
        self.assertEqual(extended["resolved_count"], 0)

        # Reopen at 16:25Z; APS resumes after its 20 min buffer -> the effective
        # window ends 16:45Z. BY205 arrives at exactly 16:45 (touching endpoint,
        # half-open interval) and KX099 at 19:15 are freed; AX412 (16:30 dep)
        # stays cancelled. Freed flights are recorded as resolved tombstones.
        reopened = self.service.submit_event(
            reopen(
                "evt-reopen000001",
                "evt-extend000001",
                effective_from="2026-09-07T16:25:00Z",
            )
        )
        self.assertEqual(
            [i["flight_id"] for i in reopened["impacts"]], ["AX-412-20260908"]
        )
        self.assertEqual(reopened["resolved_count"], 2)

        # Latest-snapshot queries must not resurrect the freed flights.
        rows = self.service.affected_flights(
            airport="APS", status=None, limit=50, offset=0
        )
        self.assertEqual(
            [f["flight_id"] for f in rows["flights"]], ["AX-412-20260908"]
        )
        summary = self.service.airport_summary("APS")
        self.assertEqual(summary["active_chains"], 0)
        self.assertEqual(summary["affected_flights"], 1)


class QueryTest(ServiceTestCase):
    def _submit_aps_close(self):
        return self.service.submit_event(close())

    def test_event_status_unknown_404(self) -> None:
        with self.assertRaises(NotFoundError):
            self.service.event_status("evt-missing0001")

    def test_airport_summary_unknown_404(self) -> None:
        with self.assertRaises(NotFoundError):
            self.service.airport_summary("ZZZ")

    def test_airport_summary_counts(self) -> None:
        self._submit_aps_close()
        summary = self.service.airport_summary("APS")
        self.assertEqual(summary["event_count"], 1)
        self.assertEqual(summary["active_chains"], 1)
        self.assertEqual(summary["affected_flights"], 3)
        self.assertEqual(summary["affected_passengers"], 441)
        self.assertEqual(summary["by_status"]["cancelled"]["flight_count"], 3)

    def test_summary_deactivates_chain_after_reopen(self) -> None:
        self._submit_aps_close()
        # Reopen 15:10Z -> resume 15:30Z after APS's 20 min buffer; AX410's
        # 15:30 departure touches (but does not cross) the half-open window end.
        self.service.submit_event(
            reopen(
                "evt-reopen000001",
                "evt-close0000001",
                effective_from="2026-09-07T15:10:00Z",
            )
        )
        summary = self.service.airport_summary("APS")
        self.assertEqual(summary["active_chains"], 0)
        self.assertEqual(summary["affected_flights"], 0)

    def test_pagination(self) -> None:
        self._submit_aps_close()
        page1 = self.service.affected_flights(
            airport=None, status=None, limit=2, offset=0
        )
        page2 = self.service.affected_flights(
            airport=None, status=None, limit=2, offset=2
        )
        self.assertEqual(page1["pagination"]["total"], 3)
        self.assertEqual(len(page1["flights"]), 2)
        self.assertEqual(len(page2["flights"]), 1)
        ids = [f["flight_id"] for f in page1["flights"]] + [
            f["flight_id"] for f in page2["flights"]
        ]
        self.assertEqual(len(ids), len(set(ids)))

    def test_status_filter(self) -> None:
        self.service.submit_event(
            close(
                event_id="evt-bsr000000001",
                airport="BSR",
                effective_from="2026-09-07T15:00:00Z",
                effective_until="2026-09-07T16:00:00Z",
            )
        )
        delayed = self.service.affected_flights(
            airport="BSR", status="delayed", limit=50, offset=0
        )
        self.assertEqual([f["flight_id"] for f in delayed["flights"]], ["BY-205-20260908"])
        cancelled = self.service.affected_flights(
            airport="BSR", status="cancelled", limit=50, offset=0
        )
        self.assertEqual(cancelled["flights"], [])
        with self.assertRaises(ValidationError):
            self.service.affected_flights(
                airport=None, status="bogus", limit=50, offset=0
            )


class PersistenceTest(ServiceTestCase):
    def test_data_survives_restart(self) -> None:
        payload = close()
        first = self.service.submit_event(payload)
        self.restart_service()
        status = self.service.event_status(payload["event_id"])
        self.assertEqual(status["event"]["event_id"], payload["event_id"])
        self.assertEqual(len(status["impacts"]), first["impact_count"])
        # Idempotency still works after restart.
        replay = self.service.submit_event(dict(payload))
        self.assertEqual(replay["processing_state"], "replayed")
        self.assertEqual(replay["impacts"], first["impacts"])

    def test_restart_preserves_subsecond_window_results(self) -> None:
        # Window ends 00:00:00.5 local: crosses midnight by half a second.
        # The sub-second endpoint must survive persistence unchanged, or the
        # flag recomputed after a DB reopen would contradict the stored one.
        payload = close(
            event_id="evt-subsec000001",
            effective_until="2026-09-07T16:00:00.500000Z",
        )
        first = self.service.submit_event(payload)
        self.assertTrue(first["impacts"])
        self.assertTrue(all(i["crosses_midnight"] for i in first["impacts"]))

        self.restart_service()
        status = self.service.event_status(payload["event_id"])
        self.assertEqual(status["impacts"], first["impacts"])
        self.assertEqual(
            status["event"]["effective_until"], "2026-09-07T16:00:00.500000Z"
        )
        # The restart must not have "corrected" anything: results were stable.
        self.assertEqual(self.repo.list_impact_corrections(), [])
        replay = self.service.submit_event(dict(payload))
        self.assertEqual(replay["processing_state"], "replayed")
        self.assertEqual(replay["impacts"], first["impacts"])

    def test_invalid_event_writes_nothing(self) -> None:
        with self.assertRaises(ValidationError):
            self.service.submit_event(close(airport_code="ZZZ"))
        rows = self.service.affected_flights(
            airport=None, status=None, limit=50, offset=0
        )
        self.assertEqual(rows["pagination"]["total"], 0)
        with self.assertRaises(NotFoundError):
            self.service.event_status("evt-close0000001")

    def test_failed_chain_event_leaves_no_partial_writes(self) -> None:
        self.service.submit_event(close())
        before = self.service.event_status("evt-close0000001")
        with self.assertRaises(ValidationError):
            self.service.submit_event(
                extend("evt-extend000001", "evt-unknown00001")
            )
        with self.assertRaises(NotFoundError):
            self.service.event_status("evt-extend000001")
        after = self.service.event_status("evt-close0000001")
        self.assertEqual(after["impacts"], before["impacts"])


class ProjectionMigrationTest(ServiceTestCase):
    """已写入的错误跨日标志通过可审计迁移更正，旧裁定版本仍可查询。"""

    LEGACY_EVENT_ID = "evt-legacy00001"

    def _plant_legacy_rows(self) -> None:
        """模拟修复前写入的裁定：23:00-00:00 本地窗口被误标为跨日。"""
        event_dict = {
            "event_id": self.LEGACY_EVENT_ID,
            "event_version": 1,
            "event_type": "airport.closed",
            "airport_code": "APS",
            "effective_from": "2026-09-07T15:00:00Z",
            "effective_until": "2026-09-07T16:00:00Z",
            "reported_at": "2026-09-07T14:00:00Z",
            "supersedes_event_id": None,
            "reason": "ash",
        }
        impact = {
            "event_id": self.LEGACY_EVENT_ID,
            "root_event_id": self.LEGACY_EVENT_ID,
            "airport_code": "APS",
            "flight_id": "AX-410-20260907",
            "flight_number": "AX410",
            "affected_endpoint": "origin",
            "impact_status": "delayed",
            "overlap_minutes": 30,
            "delay_minutes": 30,
            "proposed_departure": "2026-09-07T16:00:00Z",
            "proposed_arrival": "2026-09-07T17:40:00Z",
            "passenger_count": 168,
            "crosses_midnight": 1,  # 旧逻辑误标：午夜终点被算进窗口
            "projection_version": 1,
        }
        with self.repo.transaction() as conn:
            self.repo.insert_event(conn, event_dict)
            self.repo.insert_impacts(conn, [impact])
            # 版本化之前的库没有 service_meta 记录
            conn.execute("DELETE FROM service_meta")

    def test_migration_corrects_flag_and_preserves_old_version(self) -> None:
        self._plant_legacy_rows()
        self.restart_service()  # 重新打开数据库触发迁移

        current = self.service.event_status(self.LEGACY_EVENT_ID)
        self.assertEqual(current["projection_version"], CURRENT_PROJECTION_VERSION)
        self.assertEqual([i["crosses_midnight"] for i in current["impacts"]], [False])
        # 其余字段不得被迁移改动
        self.assertEqual(current["impacts"][0]["impact_status"], "delayed")
        self.assertEqual(current["impacts"][0]["delay_minutes"], 30)

        # 查询旧裁定版本仍能看到当时（错误）的结果
        legacy = self.service.event_status(self.LEGACY_EVENT_ID, projection_version=1)
        self.assertEqual(legacy["projection_version"], 1)
        self.assertEqual([i["crosses_midnight"] for i in legacy["impacts"]], [True])

        # 审计痕迹：一次迁移运行 + 一条行级更正
        migrations = self.repo.list_projection_migrations()
        self.assertEqual(len(migrations), 1)
        run = migrations[0]
        self.assertEqual((run["from_version"], run["to_version"]), (1, 2))
        self.assertEqual(run["rows_examined"], 1)
        self.assertEqual(run["rows_corrected"], 1)
        self.assertEqual(run["actor"], "system:projection-migration")
        self.assertTrue(run["reason"])
        corrections = self.repo.list_impact_corrections()
        self.assertEqual(len(corrections), 1)
        c = corrections[0]
        self.assertEqual(c["migration_id"], run["id"])
        self.assertEqual(
            (c["event_id"], c["flight_id"], c["field"], c["old_value"], c["new_value"]),
            (self.LEGACY_EVENT_ID, "AX-410-20260907", "crosses_midnight", "1", "0"),
        )

        # 再次重开是空操作：不重复迁移、结果不变
        self.restart_service()
        self.assertEqual(len(self.repo.list_projection_migrations()), 1)
        self.assertEqual(len(self.repo.list_impact_corrections()), 1)
        again = self.service.event_status(self.LEGACY_EVENT_ID)
        self.assertEqual(again["impacts"], current["impacts"])

    def test_legacy_schema_database_is_rebuilt_then_migrated(self) -> None:
        """版本化之前创建的库（impacts 无 projection_version 列）重开后：
        结构先重建、既有行归为版本 1，随后数据迁移更正误标。"""
        legacy_path = Path(self._tmp.name) / "legacy.db"
        conn = sqlite3.connect(str(legacy_path))
        conn.executescript(
            """
            CREATE TABLE events (
                event_id             TEXT PRIMARY KEY,
                event_version        INTEGER NOT NULL,
                event_type           TEXT NOT NULL,
                airport_code         TEXT NOT NULL,
                effective_from       TEXT NOT NULL,
                effective_until      TEXT,
                reported_at          TEXT NOT NULL,
                supersedes_event_id  TEXT,
                reason               TEXT,
                payload_json         TEXT NOT NULL,
                replay_count         INTEGER NOT NULL DEFAULT 0,
                created_at           TEXT NOT NULL
            );
            CREATE TABLE impacts (
                id                 INTEGER PRIMARY KEY AUTOINCREMENT,
                event_id           TEXT NOT NULL REFERENCES events(event_id),
                root_event_id      TEXT NOT NULL,
                airport_code       TEXT NOT NULL,
                flight_id          TEXT NOT NULL,
                flight_number      TEXT NOT NULL,
                affected_endpoint  TEXT NOT NULL,
                impact_status      TEXT NOT NULL,
                overlap_minutes    INTEGER,
                delay_minutes      INTEGER,
                proposed_departure TEXT,
                proposed_arrival   TEXT,
                passenger_count    INTEGER NOT NULL,
                crosses_midnight   INTEGER NOT NULL,
                UNIQUE(event_id, flight_id, airport_code)
            );
            """
        )
        payload = {
            "event_id": self.LEGACY_EVENT_ID,
            "event_version": 1,
            "event_type": "airport.closed",
            "airport_code": "APS",
            "effective_from": "2026-09-07T15:00:00Z",
            "effective_until": "2026-09-07T16:00:00Z",
            "reported_at": "2026-09-07T14:00:00Z",
            "supersedes_event_id": None,
            "reason": None,
        }
        import json as _json

        conn.execute(
            "INSERT INTO events (event_id, event_version, event_type, airport_code,"
            " effective_from, effective_until, reported_at, supersedes_event_id,"
            " reason, payload_json, replay_count, created_at)"
            " VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 0, ?)",
            (
                payload["event_id"],
                payload["event_version"],
                payload["event_type"],
                payload["airport_code"],
                payload["effective_from"],
                payload["effective_until"],
                payload["reported_at"],
                None,
                None,
                _json.dumps(payload, sort_keys=True),
                "2026-09-07T14:00:00Z",
            ),
        )
        conn.execute(
            "INSERT INTO impacts (event_id, root_event_id, airport_code, flight_id,"
            " flight_number, affected_endpoint, impact_status, overlap_minutes,"
            " delay_minutes, proposed_departure, proposed_arrival, passenger_count,"
            " crosses_midnight) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (
                self.LEGACY_EVENT_ID,
                self.LEGACY_EVENT_ID,
                "APS",
                "AX-410-20260907",
                "AX410",
                "origin",
                "delayed",
                30,
                30,
                "2026-09-07T16:00:00Z",
                "2026-09-07T17:40:00Z",
                168,
                1,
            ),
        )
        conn.commit()
        conn.close()

        repo = Repository(legacy_path)
        try:
            service = DisruptionService(repo, self.airports, self.flights)
            current = service.event_status(self.LEGACY_EVENT_ID)
            self.assertEqual(current["projection_version"], CURRENT_PROJECTION_VERSION)
            self.assertEqual(
                [i["crosses_midnight"] for i in current["impacts"]], [False]
            )
            legacy = service.event_status(self.LEGACY_EVENT_ID, projection_version=1)
            self.assertEqual(
                [i["crosses_midnight"] for i in legacy["impacts"]], [True]
            )
            self.assertEqual(len(repo.list_projection_migrations()), 1)
            self.assertEqual(len(repo.list_impact_corrections()), 1)
        finally:
            repo.close()


class ProjectionVersionConsistencyTest(ServiceTestCase):
    """事件详情、机场汇总与航班分页引用同一计算版本。"""

    def test_all_queries_reference_same_projection_version(self) -> None:
        self.service.submit_event(close())
        current = self.repo.current_projection_version()
        self.assertEqual(current, CURRENT_PROJECTION_VERSION)
        status = self.service.event_status("evt-close0000001")
        summary = self.service.airport_summary("APS")
        flights = self.service.affected_flights(
            airport="APS", status=None, limit=50, offset=0
        )
        versions = {
            status["projection_version"],
            summary["projection_version"],
            flights["projection_version"],
        }
        self.assertEqual(versions, {current})

    def test_out_of_range_projection_version_rejected(self) -> None:
        self.service.submit_event(close())
        with self.assertRaises(ValidationError):
            self.service.event_status("evt-close0000001", projection_version=0)
        with self.assertRaises(ValidationError):
            self.service.airport_summary("APS", projection_version=99)
        with self.assertRaises(ValidationError):
            self.service.affected_flights(
                airport=None, status=None, limit=50, offset=0, projection_version=-1
            )


class ConcurrencyTest(ServiceTestCase):
    """并发提交相邻窗口不得产生互相矛盾的当前标志。"""

    def test_concurrent_adjacent_windows_yield_consistent_flags(self) -> None:
        # 四条首尾相接的窗口（半开区间，端点相接不重叠）并发提交。
        # 版本必须递增：先提交的高版本会让后到的低版本被拒绝，
        # 但无论提交顺序如何，当前视图中的标志都必须与各自窗口一致。
        windows = [
            # (event_id, version, from, until, 期望跨日标志)
            ("evt-conc-a-00001", 1, "2026-09-07T15:00:00Z", "2026-09-07T16:00:00Z", False),
            ("evt-conc-b-00002", 2, "2026-09-07T16:00:00Z", "2026-09-07T17:00:00Z", False),
            ("evt-conc-c-00003", 3, "2026-09-07T17:00:00Z", "2026-09-07T18:00:00Z", False),
            ("evt-conc-d-00004", 4, "2026-09-07T18:00:00Z", "2026-09-08T16:00:00.500000Z", True),
        ]
        outcomes: dict[str, tuple[str, object]] = {}

        def submit(spec) -> None:
            event_id, version, start, until, _ = spec
            payload = close(
                event_id=event_id,
                event_version=version,
                effective_from=start,
                effective_until=until,
            )
            try:
                outcomes[event_id] = ("ok", self.service.submit_event(payload))
            except AppError as exc:
                outcomes[event_id] = ("rejected", exc)

        threads = [threading.Thread(target=submit, args=(spec,)) for spec in windows]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()

        committed = set()
        for event_id, (kind, result) in outcomes.items():
            if kind == "ok":
                committed.add(event_id)
            else:
                # 只允许“版本未推进”的链校验拒绝，不得出现冲突或内部错误
                self.assertIsInstance(result, ValidationError)
                self.assertIn("must_extend_airport_history", str(result.details))
        # 最高版本无论到达顺序如何都一定会被接受
        self.assertIn("evt-conc-d-00004", committed)

        expected_flag = {spec[0]: spec[4] for spec in windows}
        page = self.service.affected_flights(
            airport="APS", status=None, limit=50, offset=0
        )
        self.assertTrue(page["flights"])
        for flight in page["flights"]:
            self.assertIn(flight["event_id"], committed)
            self.assertIs(
                flight["crosses_midnight"], expected_flag[flight["event_id"]]
            )

        # 三个查询路径引用同一计算版本，且汇总与分页内容一致
        summary = self.service.airport_summary("APS")
        self.assertEqual(summary["projection_version"], page["projection_version"])
        self.assertEqual(summary["affected_flights"], page["pagination"]["total"])
        for event_id in committed:
            status = self.service.event_status(event_id)
            self.assertEqual(status["projection_version"], page["projection_version"])


if __name__ == "__main__":
    unittest.main()
