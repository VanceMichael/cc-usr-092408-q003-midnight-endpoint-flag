"""半开区间跨午夜裁定测试。

覆盖：末端恰为本地午夜、带秒/微秒的端点、不同 UTC 偏移描述同一瞬间、
夏令时春/秋切换、开放窗口保持未确认、延长/恢复重算时标志随有效窗口更新。
"""

from __future__ import annotations

import unittest
from datetime import datetime
from zoneinfo import ZoneInfo

from app.timeutil import crosses_local_midnight


def dt(value: str) -> datetime:
    return datetime.fromisoformat(value)


MAKASSAR = ZoneInfo("Asia/Makassar")  # UTC+8，全年不切换
JAKARTA = ZoneInfo("Asia/Jakarta")  # UTC+7，全年不切换
MADRID = ZoneInfo("Europe/Madrid")  # 春 03-29、秋 10-25 切换（2026）


class HalfOpenMidnightTest(unittest.TestCase):
    def check(self, start, end, tz, expected, label):
        got = crosses_local_midnight(dt(start), None if end is None else dt(end), tz)
        self.assertEqual(got, expected, f"{label}: got {got}, want {expected}")

    def test_end_exactly_at_local_midnight_is_not_cross_day(self) -> None:
        # 23:00->00:00 本地（UTC 15:00->16:00）：午夜这一刻是开区间端点。
        self.check(
            "2026-09-07T15:00:00+00:00", "2026-09-07T16:00:00+00:00",
            MAKASSAR, False, "end exactly at local 00:00",
        )
        # 用本地 +08:00 偏移描述同一窗口，结论必须一致。
        self.check(
            "2026-09-07T23:00:00+08:00", "2026-09-08T00:00:00+08:00",
            MAKASSAR, False, "local +08 end at midnight",
        )

    def test_end_one_second_or_microsecond_past_midnight_crosses(self) -> None:
        self.check(
            "2026-09-07T15:00:00+00:00", "2026-09-07T16:00:01+00:00",
            MAKASSAR, True, "one second past midnight",
        )
        self.check(
            "2026-09-07T15:00:00+00:00", "2026-09-07T16:00:00.000001+00:00",
            MAKASSAR, True, "one microsecond past midnight",
        )
        self.check(
            "2026-09-07T15:00:00+00:00", "2026-09-07T16:00:00.5+00:00",
            MAKASSAR, True, "half a second past midnight",
        )

    def test_end_one_microsecond_before_midnight_stays_same_day(self) -> None:
        self.check(
            "2026-09-07T15:30:00+00:00", "2026-09-07T15:59:59.999999+00:00",
            MAKASSAR, False, "end just before local midnight",
        )

    def test_truly_spanning_midnight_is_flagged(self) -> None:
        # 23:00->03:00 本地
        self.check(
            "2026-09-07T15:00:00+00:00", "2026-09-07T19:00:00+00:00",
            MAKASSAR, True, "23:00->03:00 spans midnight",
        )
        # 00:00->03:00 本地：单一自然日
        self.check(
            "2026-09-07T16:00:00+00:00", "2026-09-07T19:00:00+00:00",
            MAKASSAR, False, "00:00->03:00 single local date",
        )

    def test_offset_independent_verdict(self) -> None:
        # 同一对 UTC 瞬间，用 +09:00 与 +00:00 两种输入偏移表达，机场仍按
        # 自己的时区裁定，结论相同。
        s1, e1 = "2026-09-08T00:00:00+09:00", "2026-09-08T01:00:00+09:00"
        s2, e2 = "2026-09-07T15:00:00+00:00", "2026-09-07T16:00:00+00:00"
        self.assertEqual(
            crosses_local_midnight(dt(s1), dt(e1), MAKASSAR),
            crosses_local_midnight(dt(s2), dt(e2), MAKASSAR),
        )
        self.assertFalse(crosses_local_midnight(dt(s2), dt(e2), MAKASSAR))

    def test_dst_spring_forward_night(self) -> None:
        # 马德里 2026-03-28 23:00 CET -> 03-29 00:00 CET（= 22:00Z->23:00Z）：
        # 末端恰为切换日午夜，但午夜本身不属于窗口。
        self.check(
            "2026-03-28T22:00:00+00:00", "2026-03-28T23:00:00+00:00",
            MADRID, False, "spring-DST eve end exactly midnight",
        )
        # 越过午夜一毫秒：跨日。
        self.check(
            "2026-03-28T22:00:00+00:00", "2026-03-28T23:00:00.001+00:00",
            MADRID, True, "spring-DST eve past midnight",
        )
        # 跨整个不存在的 02:00->03:00 时段：23:00 本地 -> 04:00 本地。
        self.check(
            "2026-03-28T22:00:00+00:00", "2026-03-29T03:00:00+00:00",
            MADRID, True, "window crosses the spring-forward gap",
        )

    def test_dst_fall_back_night(self) -> None:
        # 2026-10-24 23:00 CEST -> 10-25 00:00 CEST（= 21:00Z->22:00Z）。
        self.check(
            "2026-10-24T21:00:00+00:00", "2026-10-24T22:00:00+00:00",
            MADRID, False, "fall-DST eve end exactly midnight",
        )
        # 窗口跨过 03:00 回拨点（21:00Z -> 次日 00:30Z == 本地 01:30 CET）。
        self.check(
            "2026-10-24T21:00:00+00:00", "2026-10-25T00:30:00+00:00",
            MADRID, True, "window crosses the fall-back point",
        )

    def test_open_ended_window_is_unconfirmed(self) -> None:
        self.assertIsNone(
            crosses_local_midnight(
                dt("2026-09-07T23:00:00+08:00"), None, MAKASSAR
            )
        )


from tests.support import ServiceTestCase, base_event  # noqa: E402


class MidnightServiceTest(ServiceTestCase):
    def _close(self, event_id, start, end, airport="APS", **kw):
        payload = base_event(
            event_id=event_id,
            airport_code=airport,
            effective_from=start,
            effective_until=end,
        )
        payload.update(kw)
        return self.service.submit_event(payload)

    def test_service_exact_midnight_end_not_flagged(self) -> None:
        # APS UTC+8，窗口 23:00->00:00 本地，AX410 15:30Z 起飞仍受影响，
        # 但跨午夜标志必须为 False（修复前被误标为 True）。
        result = self._close(
            "evt-exactmidnt1", "2026-09-07T15:00:00Z", "2026-09-07T16:00:00Z"
        )
        self.assertTrue(result["impacts"])
        self.assertTrue(
            all(i["crosses_midnight"] is False for i in result["impacts"])
        )

    def test_service_end_with_microseconds_flagged(self) -> None:
        result = self._close(
            "evt-micromidnt1",
            "2026-09-07T15:00:00Z",
            "2026-09-07T16:00:00.5Z",
        )
        self.assertTrue(result["impacts"])
        self.assertTrue(all(i["crosses_midnight"] is True for i in result["impacts"]))

    def test_open_ended_closure_flag_is_null_not_fabricated(self) -> None:
        result = self._close(
            "evt-openmidnt01", "2026-09-07T15:00:00Z", None, airport="BSR"
        )
        self.assertTrue(result["impacts"])
        self.assertTrue(
            all(i["crosses_midnight"] is None for i in result["impacts"])
        )
        status = self.service.event_status("evt-openmidnt01")
        self.assertIsNone(status["processing"]["window_verdict"]["crosses_midnight"])
        self.assertIsNone(status["processing"]["window_verdict"]["window_end"])

    def test_extend_moves_flag_with_effective_window(self) -> None:
        # 初始 23:00->00:00 本地：不跨日。
        self._close(
            "evt-extflag001", "2026-09-07T15:00:00Z", "2026-09-07T16:00:00Z"
        )
        # 延长到 03:00 本地：有效窗口 [23:00, 03:00) 跨日，重算后标志翻转。
        extended = self.service.submit_event(
            {
                "event_id": "evt-extflag002",
                "event_version": 2,
                "event_type": "airport.extended",
                "airport_code": "APS",
                "effective_from": "2026-09-07T15:50:00Z",
                "effective_until": "2026-09-07T19:00:00Z",
                "reported_at": "2026-09-07T14:30:00Z",
                "supersedes_event_id": "evt-extflag001",
            }
        )
        self.assertTrue(extended["impacts"])
        self.assertTrue(all(i["crosses_midnight"] is True for i in extended["impacts"]))
        verdict = self.service.event_status("evt-extflag002")["processing"]["window_verdict"]
        self.assertTrue(verdict["crosses_midnight"] is True)

    def test_reopen_moves_flag_with_effective_window(self) -> None:
        # 初始 23:00->03:00 本地：跨日。
        self._close(
            "evt-reoflag001", "2026-09-07T15:00:00Z", "2026-09-07T19:00:00Z"
        )
        # 15:10Z 恢复，APS 20 分钟缓冲 -> 有效窗口终于 15:30Z（23:30 本地），
        # 不再跨午夜。AX410 15:30 起飞恰好触及半开端点，应被释放。
        reopened = self.service.submit_event(
            {
                "event_id": "evt-reoflag002",
                "event_version": 2,
                "event_type": "airport.reopened",
                "airport_code": "APS",
                "effective_from": "2026-09-07T15:10:00Z",
                "effective_until": None,
                "reported_at": "2026-09-07T15:12:00Z",
                "supersedes_event_id": "evt-reoflag001",
            }
        )
        verdict = self.service.event_status("evt-reoflag002")["processing"]["window_verdict"]
        self.assertFalse(verdict["crosses_midnight"] is True)
        self.assertEqual(verdict["window_end"], "2026-09-07T15:30:00Z")
        self.assertEqual(reopened["impact_count"], 0)


if __name__ == "__main__":
    unittest.main()
