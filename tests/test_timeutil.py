"""半开区间跨午夜判定与时间序列化测试。"""

from __future__ import annotations

import unittest
from datetime import timedelta
from zoneinfo import ZoneInfo

from app.models import iso_utc
from app.timeutil import (
    crosses_local_midnight,
    parse_event_datetime,
    parse_stored_datetime,
)

APS_TZ = ZoneInfo("Asia/Makassar")  # UTC+8，无夏令时
NY_TZ = ZoneInfo("America/New_York")  # 有夏令时切换


def utc(text: str):
    return parse_event_datetime(text, "t")


class CrossesLocalMidnightTest(unittest.TestCase):
    def test_window_ending_exactly_at_local_midnight_does_not_cross(self) -> None:
        # 15:00-16:00Z == 23:00-00:00 local：零点这一刻不属于窗口（左闭右开）
        start = utc("2026-09-07T15:00:00Z")
        end = utc("2026-09-07T16:00:00Z")
        self.assertFalse(crosses_local_midnight(start, end, APS_TZ))

    def test_end_one_microsecond_after_midnight_crosses(self) -> None:
        start = utc("2026-09-07T15:00:00Z")
        end = utc("2026-09-07T16:00:00.000001Z")
        self.assertTrue(crosses_local_midnight(start, end, APS_TZ))

    def test_end_one_microsecond_before_midnight_does_not_cross(self) -> None:
        start = utc("2026-09-07T15:00:00Z")
        end = utc("2026-09-07T15:59:59.999999Z")
        self.assertFalse(crosses_local_midnight(start, end, APS_TZ))

    def test_end_with_seconds_after_midnight_crosses(self) -> None:
        start = utc("2026-09-07T15:00:00Z")
        end = utc("2026-09-07T16:00:30Z")
        self.assertTrue(crosses_local_midnight(start, end, APS_TZ))

    def test_same_window_with_different_offsets_agrees(self) -> None:
        # 同一物理窗口分别用 Z 与 +08:00 表示，判定必须一致
        start_z = utc("2026-09-07T15:00:00Z")
        end_z = utc("2026-09-07T16:00:00Z")
        start_off = utc("2026-09-07T23:00:00+08:00")
        end_off = utc("2026-09-08T00:00:00+08:00")
        self.assertEqual(start_z, start_off)
        self.assertEqual(end_z, end_off)
        self.assertEqual(
            crosses_local_midnight(start_z, end_z, APS_TZ),
            crosses_local_midnight(start_off, end_off, APS_TZ),
        )
        self.assertFalse(crosses_local_midnight(start_off, end_off, APS_TZ))

    def test_multi_day_window_crosses(self) -> None:
        self.assertTrue(
            crosses_local_midnight(
                utc("2026-09-07T15:00:00Z"), utc("2026-09-09T16:00:00Z"), APS_TZ
            )
        )

    def test_same_day_window_does_not_cross(self) -> None:
        self.assertFalse(
            crosses_local_midnight(
                utc("2026-09-07T01:00:00Z"), utc("2026-09-07T05:00:00Z"), APS_TZ
            )
        )

    def test_start_exactly_at_local_midnight_same_day_end(self) -> None:
        # 16:00-19:00Z == 00:00-03:00 local：单一自然日
        self.assertFalse(
            crosses_local_midnight(
                utc("2026-09-07T16:00:00Z"), utc("2026-09-07T19:00:00Z"), APS_TZ
            )
        )

    def test_spring_forward_end_exactly_at_local_midnight(self) -> None:
        # 纽约 2026-03-08 02:00 拨快一小时，当天只有 23 小时；
        # 本地 00:00 -> 次日 00:00 的窗口终点被排除，仍属同一自然日。
        start = utc("2026-03-08T05:00:00Z")  # 2026-03-08 00:00 EST
        end = utc("2026-03-09T04:00:00Z")  # 2026-03-09 00:00 EDT
        self.assertEqual(end - start, timedelta(hours=23))
        self.assertFalse(crosses_local_midnight(start, end, NY_TZ))

    def test_fall_back_end_exactly_at_local_midnight(self) -> None:
        # 纽约 2026-11-01 02:00 拨回一小时，当天有 25 小时。
        start = utc("2026-11-01T04:00:00Z")  # 2026-11-01 00:00 EDT
        end = utc("2026-11-02T05:00:00Z")  # 2026-11-02 00:00 EST
        self.assertEqual(end - start, timedelta(hours=25))
        self.assertFalse(crosses_local_midnight(start, end, NY_TZ))

    def test_window_spanning_dst_jump_still_detects_crossing(self) -> None:
        # 23:00 EST -> 03:00 EDT，跨过拨快时刻，覆盖两个自然日
        start = utc("2026-03-08T04:00:00Z")
        end = utc("2026-03-08T08:00:00Z")
        self.assertTrue(crosses_local_midnight(start, end, NY_TZ))

    def test_empty_window_never_crosses(self) -> None:
        moment = utc("2026-09-07T16:00:00Z")
        self.assertFalse(crosses_local_midnight(moment, moment, APS_TZ))


class IsoUtcTest(unittest.TestCase):
    def test_whole_seconds_format_unchanged(self) -> None:
        self.assertEqual(iso_utc(utc("2026-09-07T16:00:00Z")), "2026-09-07T16:00:00Z")

    def test_microseconds_preserved(self) -> None:
        text = iso_utc(utc("2026-09-07T16:00:00.500000Z"))
        self.assertEqual(text, "2026-09-07T16:00:00.500000Z")
        # 序列化往返不丢失精度
        self.assertEqual(parse_stored_datetime(text), utc("2026-09-07T16:00:00.500000Z"))

    def test_offset_input_normalized_with_microseconds(self) -> None:
        text = iso_utc(utc("2026-09-08T00:00:00.500000+08:00"))
        self.assertEqual(text, "2026-09-07T16:00:00.500000Z")


if __name__ == "__main__":
    unittest.main()
