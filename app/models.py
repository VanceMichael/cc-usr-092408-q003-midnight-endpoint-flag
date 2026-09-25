"""领域模型类型。"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Any, Mapping

from app.timeutil import parse_stored_datetime, to_utc

# Event type constants (mirrors contracts/disruption-event.schema.json)
EVENT_CLOSED = "airport.closed"
EVENT_EXTENDED = "airport.extended"
EVENT_REOPENED = "airport.reopened"

EVENT_TYPES = (EVENT_CLOSED, EVENT_EXTENDED, EVENT_REOPENED)

# Impact statuses
IMPACT_CANCELLED = "cancelled"
IMPACT_DELAYED = "delayed"
IMPACT_PENDING = "pending_confirmation"
# Internal lifecycle marker: a flight that an earlier event of the same chain
# impacted but the current chain state no longer affects. Kept out of client
# facing summaries; used so "latest snapshot" queries cannot resurrect stale
# cancelled/delayed rows after a reopening.
IMPACT_RESOLVED = "resolved"

ACTIVE_IMPACT_STATUSES = (IMPACT_CANCELLED, IMPACT_DELAYED, IMPACT_PENDING)

# Event processing states
STATE_PROCESSED = "processed"
STATE_REPLAYED = "replayed"

CLOSED_TYPES = (EVENT_CLOSED, EVENT_EXTENDED)


@dataclass(frozen=True)
class Airport:
    code: str
    name: str
    timezone: str
    reopen_buffer_minutes: int


@dataclass(frozen=True)
class Flight:
    flight_id: str
    flight_number: str
    origin: str
    destination: str
    scheduled_departure: datetime  # always timezone-aware UTC
    scheduled_arrival: datetime  # always timezone-aware UTC
    passenger_count: int
    can_retime: bool
    max_delay_minutes: int

    @property
    def duration_minutes(self) -> int:
        return int((self.scheduled_arrival - self.scheduled_departure).total_seconds() // 60)


@dataclass(frozen=True)
class DisruptionEvent:
    """已经校验的中断事件，所有时间均为带时区的 UTC。"""

    event_id: str
    event_version: int
    event_type: str
    airport_code: str
    effective_from: datetime
    effective_until: datetime | None
    reported_at: datetime
    supersedes_event_id: str | None
    reason: str | None

    def to_dict(self) -> dict[str, Any]:
        return {
            "event_id": self.event_id,
            "event_version": self.event_version,
            "event_type": self.event_type,
            "airport_code": self.airport_code,
            "effective_from": iso_utc(self.effective_from),
            "effective_until": iso_utc(self.effective_until) if self.effective_until else None,
            "reported_at": iso_utc(self.reported_at),
            "supersedes_event_id": self.supersedes_event_id,
            "reason": self.reason,
        }


def iso_utc(dt: datetime) -> str:
    """将带时区时间输出为规范的 UTC ISO 8601 字符串。

    整秒时刻保持 ``...T%H:%M:%SZ`` 形式；带微秒的时刻保留六位小数。
    亚秒精度会影响半开区间端点（例如恰好止于本地午夜的窗口），持久化
    时不得截断，否则数据库重开后重算结果会与写入时不一致。
    """
    dt = to_utc(dt)
    if dt.microsecond:
        return dt.strftime("%Y-%m-%dT%H:%M:%S.%fZ")
    return dt.strftime("%Y-%m-%dT%H:%M:%SZ")


def event_from_record(record: Mapping[str, Any]) -> DisruptionEvent:
    """从持久化行重建事件；时间列必须是规范 UTC 串。"""
    return DisruptionEvent(
        event_id=record["event_id"],
        event_version=record["event_version"],
        event_type=record["event_type"],
        airport_code=record["airport_code"],
        effective_from=parse_stored_datetime(record["effective_from"]),
        effective_until=(
            parse_stored_datetime(record["effective_until"])
            if record["effective_until"]
            else None
        ),
        reported_at=parse_stored_datetime(record["reported_at"]),
        supersedes_event_id=record["supersedes_event_id"],
        reason=record["reason"],
    )
