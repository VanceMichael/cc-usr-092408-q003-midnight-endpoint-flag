"""裁定版本、可审计迁移、三面一致性、并发与重启稳定性测试。"""

from __future__ import annotations

import json
import sqlite3
import threading
from datetime import datetime, timezone
from pathlib import Path

from app.engine import airport_tz, chain_window, classify_flight
from app.models import EVENT_REOPENED
from app.repository import Repository
from app.service import DisruptionService
from tests.support import ServiceTestCase

# v1（修复前）的结构与跨日公式：直接比较本地日期，午夜整也被算作跨日；
# 开放窗口被写成 0（伪造结束日）。
V1_SCHEMA = """
CREATE TABLE events (
  event_id TEXT PRIMARY KEY, event_version INTEGER NOT NULL, event_type TEXT NOT NULL,
  airport_code TEXT NOT NULL, effective_from TEXT NOT NULL, effective_until TEXT,
  reported_at TEXT NOT NULL, supersedes_event_id TEXT, reason TEXT,
  payload_json TEXT NOT NULL, replay_count INTEGER NOT NULL DEFAULT 0,
  created_at TEXT NOT NULL);
CREATE TABLE impacts (
  id INTEGER PRIMARY KEY AUTOINCREMENT, event_id TEXT NOT NULL,
  root_event_id TEXT NOT NULL, airport_code TEXT NOT NULL, flight_id TEXT NOT NULL,
  flight_number TEXT NOT NULL, affected_endpoint TEXT NOT NULL, impact_status TEXT NOT NULL,
  overlap_minutes INTEGER, delay_minutes INTEGER, proposed_departure TEXT,
  proposed_arrival TEXT, passenger_count INTEGER NOT NULL,
  crosses_midnight INTEGER NOT NULL, UNIQUE(event_id, flight_id, airport_code));
CREATE INDEX idx_events_airport ON events(airport_code, event_version);
"""


def _parse(value: str | None) -> datetime | None:
    if value is None:
        return None
    v = value[:-1] + "+00:00" if value.endswith("Z") else value
    return datetime.fromisoformat(v).astimezone(timezone.utc)


def _v1_flag(start: datetime, end: datetime | None, tz) -> int:
    if end is None:
        return 0  # v1 把开放窗口伪造成 0
    ls, le = start.astimezone(tz), end.astimezone(tz)
    return int(ls.date() != le.date())


def seed_v1_event(
    conn: sqlite3.Connection,
    payload: dict,
    root_id: str,
    airports,
    flights,
    prior_active: set[str],
) -> None:
    """按 v1 语义写入一条事件及其影响行（标志用 v1 的错误公式）。"""
    airport = airports[payload["airport_code"]]
    tz = airport_tz(airport)

    class _E:
        pass

    event = _E()
    event.event_id = payload["event_id"]
    event.event_type = payload["event_type"]
    event.airport_code = payload["airport_code"]
    event.effective_from = _parse(payload["effective_from"])
    event.effective_until = _parse(payload.get("effective_until"))

    root = _E()
    root.event_id = root_id
    root.effective_from = _parse(payload["effective_from"])
    if root_id != payload["event_id"]:
        # 链根开始时间由调用方在 payload 之外传入，简化为显式字段。
        root.effective_from = _parse(payload["_root_from"])

    window = chain_window(event, root, airport)  # type: ignore[arg-type]
    flag = _v1_flag(window.start, window.end, tz)

    conn.execute(
        "INSERT INTO events (event_id,event_version,event_type,airport_code,"
        "effective_from,effective_until,reported_at,supersedes_event_id,reason,"
        "payload_json,replay_count,created_at) VALUES (?,?,?,?,?,?,?,?,?,?,0,?)",
        (
            payload["event_id"], payload["event_version"], payload["event_type"],
            payload["airport_code"], payload["effective_from"],
            payload.get("effective_until"), payload["reported_at"],
            payload.get("supersedes_event_id"), payload.get("reason"),
            json.dumps({k: v for k, v in payload.items() if not k.startswith("_")}),
            "2026-09-07T14:00:01Z",
        ),
    )

    still: set[str] = set()
    for flight in sorted(flights.values(), key=lambda f: f.flight_id):
        record = classify_flight(flight, window)
        if record is None:
            continue
        still.add(flight.flight_id)
        conn.execute(
            "INSERT INTO impacts (event_id,root_event_id,airport_code,flight_id,"
            "flight_number,affected_endpoint,impact_status,overlap_minutes,"
            "delay_minutes,proposed_departure,proposed_arrival,passenger_count,"
            "crosses_midnight) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)",
            (
                payload["event_id"], root_id, payload["airport_code"],
                record["flight_id"], record["flight_number"],
                record["affected_endpoint"], record["impact_status"],
                record["overlap_minutes"], record["delay_minutes"],
                record["proposed_departure"], record["proposed_arrival"],
                record["passenger_count"], flag,
            ),
        )
    # v1 的恢复事件同样写 resolved 墓碑。
    if payload["event_type"] == EVENT_REOPENED:
        for fid in sorted(prior_active - still):
            flight = flights[fid]
            conn.execute(
                "INSERT INTO impacts (event_id,root_event_id,airport_code,flight_id,"
                "flight_number,affected_endpoint,impact_status,overlap_minutes,"
                "delay_minutes,proposed_departure,proposed_arrival,passenger_count,"
                "crosses_midnight) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)",
                (
                    payload["event_id"], root_id, payload["airport_code"], fid,
                    flight.flight_number, "none", "resolved", 0, None, None, None,
                    flight.passenger_count, 0,
                ),
            )


class MigrationTest(ServiceTestCase):
    def _build_legacy_db(self) -> None:
        self.repo.close()
        # 丢弃 setUp 建立的当前版本库，模拟一个部署卷上真实存在的 v1 旧库。
        for suffix in ("", "-wal", "-shm"):
            p = Path(str(self.db_path) + suffix)
            if p.exists():
                p.unlink()
        conn = sqlite3.connect(self.db_path)
        conn.executescript(V1_SCHEMA)
        airports = self.airports
        flights = self.flights

        # APS 23:00->00:00 本地（恰为午夜）：v1 误标 1。
        close_exact = {
            "event_id": "evt-mig-exact1", "event_version": 1,
            "event_type": "airport.closed", "airport_code": "APS",
            "effective_from": "2026-09-07T15:00:00Z",
            "effective_until": "2026-09-07T16:00:00Z",
            "reported_at": "2026-09-07T14:00:00Z",
        }
        seed_v1_event(conn, close_exact, close_exact["event_id"], airports, flights, set())

        # BSR 开放窗口：v1 伪造标志 0。
        close_open = {
            "event_id": "evt-mig-open01", "event_version": 1,
            "event_type": "airport.closed", "airport_code": "BSR",
            "effective_from": "2026-09-07T15:00:00Z", "effective_until": None,
            "reported_at": "2026-09-07T14:00:00Z",
        }
        seed_v1_event(conn, close_open, close_open["event_id"], airports, flights, set())

        # APS 真正跨日 23:00->03:00：v1 标志 1 正确，不应产生更正。
        close_cross = {
            "event_id": "evt-mig-cross1", "event_version": 1,
            "event_type": "airport.closed", "airport_code": "APS",
            "effective_from": "2026-09-07T17:00:00Z",
            "effective_until": "2026-09-07T19:00:00Z",
            "reported_at": "2026-09-07T16:00:00Z",
        }
        seed_v1_event(conn, close_cross, close_cross["event_id"], airports, flights, set())

        conn.commit()
        conn.close()

    def _reopen_service(self) -> DisruptionService:
        self.repo = Repository(self.db_path)
        self.service = DisruptionService(
            self.repo, self.airports, self.flights
        )
        return self.service

    def test_backfill_corrects_flags_and_keeps_history(self) -> None:
        self._build_legacy_db()
        service = self._reopen_service()

        exact = service.event_status("evt-mig-exact1")
        self.assertEqual(
            exact["processing"]["current_projection_version"], 2
        )
        self.assertTrue(
            all(i["crosses_midnight"] is False for i in exact["impacts"])
        )
        self.assertFalse(exact["processing"]["window_verdict"]["crosses_midnight"])

        # 查询旧裁定版本仍能看到当时的错误结果。
        exact_v1 = service.event_status("evt-mig-exact1", projection_version=1)
        self.assertEqual(exact_v1["processing"]["projection_version"], 1)
        self.assertTrue(
            all(i["crosses_midnight"] is True for i in exact_v1["impacts"])
        )
        self.assertTrue(exact_v1["processing"]["window_verdict"]["crosses_midnight"])
        self.assertEqual(
            exact_v1["processing"]["available_projection_versions"], [1, 2]
        )

        # 开放窗口：当前为未确认 null；v1 历史仍为 false。
        opened = service.event_status("evt-mig-open01")
        self.assertTrue(
            all(i["crosses_midnight"] is None for i in opened["impacts"])
        )
        self.assertIsNone(opened["processing"]["window_verdict"]["window_end"])
        opened_v1 = service.event_status("evt-mig-open01", projection_version=1)
        self.assertFalse(opened_v1["impacts"][0]["crosses_midnight"])

        # 真正跨日的窗口不被改动。
        crossed = service.event_status("evt-mig-cross1")
        self.assertTrue(
            all(i["crosses_midnight"] is True for i in crossed["impacts"])
        )

    def test_amendments_are_recorded_per_changed_verdict(self) -> None:
        self._build_legacy_db()
        service = self._reopen_service()
        amendments = service.projection_amendments()
        keys = {(a["event_id"], a["flight_id"], a["field_name"]) for a in amendments}
        self.assertIn(
            ("evt-mig-exact1", "AX-410-20260907", "crosses_midnight"), keys
        )
        self.assertIn(
            ("evt-mig-open01", "BY-205-20260908", "crosses_midnight"), keys
        )
        for a in amendments:
            self.assertEqual(a["migration_version"], 2)
            self.assertTrue(a["reason"])
        # 正确的跨日窗口没有任何更正。
        self.assertFalse(
            any(a["event_id"] == "evt-mig-cross1" for a in amendments)
        )
        # 按事件过滤。
        self.assertTrue(
            all(
                a["event_id"] == "evt-mig-exact1"
                for a in service.projection_amendments("evt-mig-exact1")
            )
        )

    def test_backfill_is_idempotent_across_reopen(self) -> None:
        self._build_legacy_db()
        service = self._reopen_service()
        first = service.projection_amendments()
        self.repo.close()
        service = self._reopen_service()
        second = service.projection_amendments()
        self.assertEqual(len(first), len(second))
        # 再迁移一次不会重复插入 v2 快照。
        exact = service.event_status("evt-mig-exact1")
        self.assertEqual(
            exact["processing"]["available_projection_versions"], [1, 2]
        )
        self.assertEqual(self.repo.current_flag_inconsistencies(), [])


class CrossSurfaceVersionTest(ServiceTestCase):
    def _seed(self) -> None:
        self.service.submit_event(
            {
                "event_id": "evt-surface001", "event_version": 1,
                "event_type": "airport.closed", "airport_code": "APS",
                "effective_from": "2026-09-07T15:00:00Z",
                "effective_until": "2026-09-07T16:00:00Z",
                "reported_at": "2026-09-07T14:00:00Z",
            }
        )

    def test_three_faces_reference_same_version_and_flag(self) -> None:
        self._seed()
        detail = self.service.event_status("evt-surface001")
        summary = self.service.airport_summary("APS")
        page = self.service.affected_flights(
            airport="APS", status=None, limit=50, offset=0
        )
        dv = detail["processing"]["projection_version"]
        self.assertEqual(summary["projection_version"], dv)
        self.assertEqual(page["pagination"]["projection_version"], dv)
        detail_flag = {i["flight_id"]: i["crosses_midnight"] for i in detail["impacts"]}
        page_flag = {f["flight_id"]: f["crosses_midnight"] for f in page["flights"]}
        self.assertEqual(detail_flag, page_flag)
        self.assertTrue(all(v is False for v in detail_flag.values()))
        self.assertEqual(self.repo.current_flag_inconsistencies(), [])


class RestartStabilityTest(ServiceTestCase):
    def test_microsecond_endpoint_stable_across_restart(self) -> None:
        payload = {
            "event_id": "evt-microrest01", "event_version": 1,
            "event_type": "airport.closed", "airport_code": "APS",
            # 末端越过本地午夜 0.5 秒：必须稳定地标为跨日。
            "effective_from": "2026-09-07T15:00:00Z",
            "effective_until": "2026-09-07T16:00:00.5Z",
            "reported_at": "2026-09-07T14:00:00Z",
        }
        first = self.service.submit_event(payload)
        before = {
            i["flight_id"]: (i["crosses_midnight"], i["projection_version"])
            for i in first["impacts"]
        }
        service = self.restart_service()
        status = service.event_status("evt-microrest01")
        after = {
            i["flight_id"]: (i["crosses_midnight"], i["projection_version"])
            for i in status["impacts"]
        }
        self.assertEqual(before, after)
        self.assertTrue(all(v[0] is True for v in after.values()))
        self.assertEqual(
            status["processing"]["window_verdict"]["window_end"],
            "2026-09-07T16:00:00.5Z",
        )
        # 幂等重放仍返回完全一致的结果。
        replay = service.submit_event(dict(payload))
        self.assertEqual(replay["impacts"], first["impacts"])


class ConcurrencyTest(ServiceTestCase):
    def _add_madrid_airport_and_flight(self) -> None:
        from app.models import Airport, Flight

        self.airports["MAD"] = Airport(
            code="MAD",
            name="Madrid DST Probe",
            timezone="Europe/Madrid",
            reopen_buffer_minutes=10,
        )
        self.flights["ZZ-900-20260328"] = Flight(
            flight_id="ZZ-900-20260328",
            flight_number="ZZ900",
            origin="MAD",
            destination="APS",
            scheduled_departure=_parse("2026-03-28T22:30:00Z"),
            scheduled_arrival=_parse("2026-03-29T00:30:00Z"),
            passenger_count=12,
            can_retime=False,
            max_delay_minutes=0,
        )

    def test_concurrent_windows_have_consistent_flags(self) -> None:
        self._add_madrid_airport_and_flight()
        barrier = threading.Barrier(4)
        results: list[dict] = []
        errors: list[Exception] = []

        def submit(event_id, airport, start, end):
            try:
                barrier.wait(timeout=5)
                results.append(
                    self.service.submit_event(
                        {
                            "event_id": event_id, "event_version": 1,
                            "event_type": "airport.closed", "airport_code": airport,
                            "effective_from": start, "effective_until": end,
                            "reported_at": "2026-09-07T14:00:00Z",
                        }
                    )
                )
            except Exception as exc:  # noqa: BLE001
                errors.append(exc)

        threads = [
            # 23:00->00:00 Makassar：末端恰为午夜，false
            threading.Thread(target=submit, args=(
                "evt-conc-aaaa01", "APS",
                "2026-09-07T15:00:00Z", "2026-09-07T16:00:00Z")),
            # 22:00->22:30 Jakarta：同日，false
            threading.Thread(target=submit, args=(
                "evt-conc-aaaa02", "BSR",
                "2026-09-07T15:00:00Z", "2026-09-07T15:30:00Z")),
            # 23:30->01:00 Jakarta：真正跨午夜，true
            threading.Thread(target=submit, args=(
                "evt-conc-aaaa03", "KTA",
                "2026-09-07T16:30:00Z", "2026-09-07T18:00:00Z")),
            # 马德里春令时前夜 23:00->00:00：末端恰为午夜，false（DST）
            threading.Thread(target=submit, args=(
                "evt-conc-aaaa04", "MAD",
                "2026-03-28T22:00:00Z", "2026-03-28T23:00:00Z")),
        ]
        for t in threads:
            t.start()
        for t in threads:
            t.join(timeout=10)

        self.assertEqual(errors, [])
        self.assertEqual(len(results), 4)
        # 数据库内不存在互相矛盾的当前标志（影响行 vs 窗口裁定）。
        self.assertEqual(self.repo.current_flag_inconsistencies(), [])

        expected = {
            "evt-conc-aaaa01": False,
            "evt-conc-aaaa02": False,
            "evt-conc-aaaa03": True,
            "evt-conc-aaaa04": False,
        }
        # 每个事件的窗口裁定、影响行与期望一致。
        for eid, want in expected.items():
            detail = self.service.event_status(eid)
            verdict = detail["processing"]["window_verdict"]
            self.assertIs(verdict["crosses_midnight"], want, eid)
            self.assertTrue(detail["impacts"], f"{eid} should impact at least one flight")
            for impact in detail["impacts"]:
                self.assertIs(impact["crosses_midnight"], want, eid)

    def test_adjacent_windows_at_one_airport_serialize_consistently(self) -> None:
        # 同一机场两个独立关闭链：第一个 [23:00,00:00) 末端恰为午夜（false）；
        # 第二个是次日 [23:00,03:00) 真正跨午夜（true）。两链并存时各自的
        # 当前标志都不能被对方污染。
        self.service.submit_event(
            {
                "event_id": "evt-adj-aaaa01", "event_version": 1,
                "event_type": "airport.closed", "airport_code": "APS",
                "effective_from": "2026-09-07T15:00:00Z",
                "effective_until": "2026-09-07T16:00:00Z",
                "reported_at": "2026-09-07T14:00:00Z",
            }
        )
        self.service.submit_event(
            {
                "event_id": "evt-adj-aaaa02", "event_version": 2,
                "event_type": "airport.closed", "airport_code": "APS",
                "effective_from": "2026-09-08T15:00:00Z",
                "effective_until": "2026-09-08T19:00:00Z",
                "reported_at": "2026-09-07T14:05:00Z",
            }
        )
        self.assertFalse(
            self.service.event_status("evt-adj-aaaa01")
            ["processing"]["window_verdict"]["crosses_midnight"]
        )
        self.assertIs(
            self.service.event_status("evt-adj-aaaa02")
            ["processing"]["window_verdict"]["crosses_midnight"],
            True,
        )
        self.assertEqual(self.repo.current_flag_inconsistencies(), [])

    def test_concurrent_duplicate_submission_yields_single_snapshot(self) -> None:
        barrier = threading.Barrier(5)
        payload = {
            "event_id": "evt-conc-dup0001", "event_version": 1,
            "event_type": "airport.closed", "airport_code": "APS",
            "effective_from": "2026-09-07T15:00:00Z",
            "effective_until": "2026-09-07T16:00:00Z",
            "reported_at": "2026-09-07T14:00:00Z",
        }
        states: list[str] = []
        lock = threading.Lock()

        def submit() -> None:
            barrier.wait(timeout=5)
            res = self.service.submit_event(dict(payload))
            with lock:
                states.append(res["processing_state"])

        threads = [threading.Thread(target=submit) for _ in range(5)]
        for t in threads:
            t.start()
        for t in threads:
            t.join(timeout=10)

        self.assertEqual(sorted(states).count("processed"), 1)
        self.assertEqual(sorted(states).count("replayed"), 4)
        # 当前版本只有一组影响行，且标志零矛盾。
        page = self.service.affected_flights(
            airport="APS", status=None, limit=50, offset=0
        )
        self.assertEqual(
            [f["event_id"] for f in page["flights"] if f["flight_id"] == "AX-410-20260907"],
            ["evt-conc-dup0001"],
        )
        self.assertEqual(self.repo.current_flag_inconsistencies(), [])


if __name__ == "__main__":
    unittest.main()
