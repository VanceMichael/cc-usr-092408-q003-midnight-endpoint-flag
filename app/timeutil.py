"""时间处理工具。

事件输入可以携带不同 UTC 偏移，区间比较前统一转换为带时区的 UTC 时间。
机场本地日期只用于判断是否跨午夜。
"""

from __future__ import annotations

import re
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

from app.errors import ValidationError

# Accept ISO 8601 date-time with Z, +HH:MM or explicit timezone.
# Naive timestamps are rejected: a disruption window without a zone is ambiguous.
_OFFSET_RE = re.compile(r"(Z|[+-]\d{2}:\d{2})$")


def parse_event_datetime(value: object, field: str) -> datetime:
    if not isinstance(value, str) or not value:
        raise ValidationError(
            f"Field '{field}' must be an ISO 8601 date-time string",
            {"field": field},
        )
    text = value.strip()
    if not _OFFSET_RE.search(text):
        raise ValidationError(
            f"Field '{field}' must include a timezone designator (Z or ±HH:MM)",
            {"field": field, "received": value},
        )
    normalized = text[:-1] + "+00:00" if text.endswith("Z") else text
    try:
        dt = datetime.fromisoformat(normalized)
    except ValueError:
        raise ValidationError(
            f"Field '{field}' is not a valid ISO 8601 date-time",
            {"field": field, "received": value},
        ) from None
    if dt.tzinfo is None:  # pragma: no cover - guarded by regex above
        raise ValidationError(
            f"Field '{field}' must include a timezone designator (Z or ±HH:MM)",
            {"field": field, "received": value},
        )
    return dt.astimezone(timezone.utc)


def to_utc(dt: datetime) -> datetime:
    if dt.tzinfo is None:
        raise ValueError("datetime must be timezone-aware")
    return dt.astimezone(timezone.utc)


def load_timezone(name: str) -> ZoneInfo:
    try:
        return ZoneInfo(name)
    except (ZoneInfoNotFoundError, ValueError):
        raise ValidationError(
            f"Airport timezone '{name}' is not a valid IANA timezone",
            {"timezone": name},
        ) from None


def overlaps(start_a: datetime, end_a: datetime, start_b: datetime, end_b: datetime) -> bool:
    """计算左闭右开区间的重叠，端点相接不算重叠。"""
    return start_a < end_b and start_b < end_a


def overlap_minutes(
    start_a: datetime, end_a: datetime, start_b: datetime, end_b: datetime
) -> int:
    start = max(start_a, start_b)
    end = min(end_a, end_b)
    return max(0, int((end - start).total_seconds() // 60))


def parse_stored_datetime(value: str) -> datetime:
    """解析持久化的 UTC 时间串（兼容 Z 后缀与小数秒）。"""
    text = value[:-1] + "+00:00" if value.endswith("Z") else value
    return datetime.fromisoformat(text).astimezone(timezone.utc)


def crosses_local_midnight(
    start: datetime, end: datetime, tz: ZoneInfo
) -> bool:
    """判断左闭右开区间 [start, end) 在指定时区内是否覆盖两个自然日。

    终点本身不属于窗口：覆盖的最后时刻是 ``end`` 前一微秒。因此恰好止于
    本地午夜的窗口不跨日，而止于午夜之后任何可分辨时刻（哪怕一微秒）的
    窗口跨日。回退在 UTC 域内进行、再换算本地日期，因此不同 UTC 偏移的
    输入与夏令时切换（23/25 小时的自然日）都遵循同一半开区间语义。
    空区间（end <= start）不覆盖任何时刻，不跨日。
    """
    if end <= start:
        return False
    local_start = start.astimezone(tz)
    last_covered = (end - timedelta(microseconds=1)).astimezone(tz)
    return local_start.date() != last_covered.date()


def minutes_until(start: datetime, end: datetime) -> int:
    return int((end - start).total_seconds() // 60)
