"""时间处理工具。

事件输入可以携带不同 UTC 偏移，区间比较前统一转换为带时区的 UTC 时间。
机场本地日期只用于判断是否跨午夜。
"""

from __future__ import annotations

import re
from datetime import datetime, time, timedelta, timezone
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


def crosses_local_midnight(
    start: datetime, end: datetime | None, tz
) -> bool | None:
    """判断左闭右开窗口 [start, end) 在指定时区内是否跨越本地午夜。

    返回三态：

    * ``True``  —— 末端严格晚于起点本地日的下一个午夜（窗口包含午夜那一秒）；
    * ``False`` —— 窗口只覆盖单个本地自然日。末端恰好是 00:00:00.000000 时
      不算跨日：午夜这一刻是右侧开区间端点，并不属于关闭时段；
    * ``None``  —— 开放窗口（``end is None``）。结束日未知，不能伪造跨日结论。

    时间先统一转到机场本地时区再比较，因此结论与输入 UTC 偏移无关；比较
    使用完整时间戳（含秒与微秒），并由 zoneinfo 正确处理夏令时切换。
    """
    if end is None:
        return None
    local_start = start.astimezone(tz)
    local_end = end.astimezone(tz)
    start_date = local_start.date()
    if local_end.date() == start_date:
        return False
    if local_end.date() > start_date + timedelta(days=1):
        return True
    # 末端落在下一个本地自然日：仅当它严格晚于该日 00:00:00.000000 时，
    # 窗口才真正包含午夜那一刻。末端自身的墙挂分量来自真实 UTC 瞬间的
    # 转换，因此在任何 DST 切换规则下结论都一致，无需构造“本地午夜”。
    return local_end.time() != time.min


def minutes_until(start: datetime, end: datetime) -> int:
    return int((end - start).total_seconds() // 60)
