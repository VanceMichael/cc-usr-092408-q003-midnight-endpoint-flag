"""SQLite 持久化层。

单一数据库文件同时保存事件和计算结果，因此一次提交可以原子写入事件及其
全部影响，失败时也不会留下部分数据。数据库位于挂载卷时，WAL 模式可在
容器重启后继续保留数据。

影响行带有投影（计算）版本：修正计算规则时，旧版本行保持不动，更正后的
行写入新版本，查询可以回看任一历史裁定版本。每次更正迁移在
``projection_migrations`` / ``impact_corrections`` 中留下审计痕迹。
"""

from __future__ import annotations

import json
import sqlite3
import threading
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable

_IMPACTS_DDL = """
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
    projection_version INTEGER NOT NULL DEFAULT 1,
    UNIQUE(event_id, flight_id, airport_code, projection_version)
"""

# Columns copied verbatim when a legacy (pre-versioning) table is rebuilt or a
# correction row is derived from an existing row.
_IMPACTS_COPY_COLUMNS = (
    "id, event_id, root_event_id, airport_code, flight_id, flight_number, "
    "affected_endpoint, impact_status, overlap_minutes, delay_minutes, "
    "proposed_departure, proposed_arrival, passenger_count, crosses_midnight"
)

SCHEMA = f"""
CREATE TABLE IF NOT EXISTS events (
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

CREATE TABLE IF NOT EXISTS impacts (
{_IMPACTS_DDL}
);

CREATE INDEX IF NOT EXISTS idx_impacts_root    ON impacts(root_event_id);
CREATE INDEX IF NOT EXISTS idx_impacts_airport ON impacts(airport_code, impact_status);
CREATE INDEX IF NOT EXISTS idx_impacts_flight  ON impacts(flight_id);
CREATE INDEX IF NOT EXISTS idx_events_airport  ON events(airport_code, event_version);

CREATE TABLE IF NOT EXISTS service_meta (
    key   TEXT PRIMARY KEY,
    value TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS projection_migrations (
    id             INTEGER PRIMARY KEY AUTOINCREMENT,
    applied_at     TEXT NOT NULL,
    from_version   INTEGER NOT NULL,
    to_version     INTEGER NOT NULL,
    reason         TEXT NOT NULL,
    actor          TEXT NOT NULL,
    rows_examined  INTEGER NOT NULL,
    rows_corrected INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS impact_corrections (
    id             INTEGER PRIMARY KEY AUTOINCREMENT,
    migration_id   INTEGER NOT NULL REFERENCES projection_migrations(id),
    event_id       TEXT NOT NULL,
    root_event_id  TEXT NOT NULL,
    airport_code   TEXT NOT NULL,
    flight_id      TEXT NOT NULL,
    field          TEXT NOT NULL,
    old_value      TEXT,
    new_value      TEXT
);
"""

# 版本化之前的数据库中，impacts 没有 projection_version 列，唯一约束也不含
# 版本。重建表并把全部既有行标记为版本 1（旧计算规则的裁定结果）。
LEGACY_IMPACTS_REBUILD = f"""
BEGIN IMMEDIATE;
ALTER TABLE impacts RENAME TO impacts_legacy;
CREATE TABLE impacts (
{_IMPACTS_DDL}
);
INSERT INTO impacts ({_IMPACTS_COPY_COLUMNS}, projection_version)
SELECT {_IMPACTS_COPY_COLUMNS}, 1 FROM impacts_legacy;
DROP TABLE impacts_legacy;
COMMIT;
"""


def utcnow_iso() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


class Repository:
    """对单一 SQLite 连接提供线程安全封装。"""

    def __init__(self, db_path: Path):
        db_path.parent.mkdir(parents=True, exist_ok=True)
        self._lock = threading.RLock()
        self._conn = sqlite3.connect(
            str(db_path), check_same_thread=False, isolation_level=None
        )
        self._conn.row_factory = sqlite3.Row
        with self._lock:
            self._conn.execute("PRAGMA journal_mode=WAL")
            self._conn.execute("PRAGMA foreign_keys=ON")
            self._conn.execute("PRAGMA synchronous=FULL")
            columns = {row[1] for row in self._conn.execute("PRAGMA table_info(impacts)")}
            if columns and "projection_version" not in columns:
                # 版本化之前创建的库：先重建 impacts，把既有行全部归为版本 1。
                self._conn.executescript(LEGACY_IMPACTS_REBUILD)
            self._conn.executescript(SCHEMA)

    def close(self) -> None:
        with self._lock:
            self._conn.close()

    def ping(self) -> bool:
        with self._lock:
            row = self._conn.execute("SELECT 1").fetchone()
            return row is not None and row[0] == 1

    # ------------------------------------------------------------------ #
    # Metadata (projection versioning)
    # ------------------------------------------------------------------ #

    def get_meta(self, key: str) -> str | None:
        with self._lock:
            row = self._conn.execute(
                "SELECT value FROM service_meta WHERE key = ?", (key,)
            ).fetchone()
            return row["value"] if row else None

    def set_meta(self, conn: sqlite3.Connection, key: str, value: str) -> None:
        conn.execute(
            "INSERT INTO service_meta (key, value) VALUES (?, ?) "
            "ON CONFLICT(key) DO UPDATE SET value = excluded.value",
            (key, value),
        )

    def current_projection_version(self) -> int:
        """当前投影版本；缺少元数据说明是版本化之前的库，视为版本 1。"""
        raw = self.get_meta("projection_version")
        return int(raw) if raw is not None else 1

    def has_impacts(self) -> bool:
        with self._lock:
            row = self._conn.execute("SELECT EXISTS(SELECT 1 FROM impacts)").fetchone()
            return bool(row[0])

    # ------------------------------------------------------------------ #
    # Reads
    # ------------------------------------------------------------------ #

    def get_event_row(self, event_id: str) -> sqlite3.Row | None:
        with self._lock:
            return self._conn.execute(
                "SELECT * FROM events WHERE event_id = ?", (event_id,)
            ).fetchone()

    def get_impacts(
        self, event_id: str, projection_version: int
    ) -> list[sqlite3.Row]:
        """返回事件在指定投影版本下的影响快照。

        同一航班/机场组合取不超过请求版本的最高版本行：未更正在新版本中的
        行回退到旧版本，保证任何历史裁定版本都能完整重建。
        """
        sql = """
        SELECT * FROM (
            SELECT i.*,
                   ROW_NUMBER() OVER (
                       PARTITION BY i.flight_id, i.airport_code
                       ORDER BY i.projection_version DESC
                   ) AS vn
            FROM impacts i
            WHERE i.event_id = ? AND i.projection_version <= ?
        )
        WHERE vn = 1
        ORDER BY flight_id
        """
        with self._lock:
            return list(self._conn.execute(sql, (event_id, projection_version)))

    def events_for_airport(self, airport_code: str) -> list[sqlite3.Row]:
        with self._lock:
            return list(
                self._conn.execute(
                    "SELECT * FROM events WHERE airport_code = ? "
                    "ORDER BY event_version, effective_from",
                    (airport_code,),
                )
            )

    def count_replays(self, event_id: str) -> int:
        with self._lock:
            row = self._conn.execute(
                "SELECT replay_count FROM events WHERE event_id = ?", (event_id,)
            ).fetchone()
            return int(row["replay_count"]) if row else 0

    def latest_impacts(
        self,
        *,
        projection_version: int,
        airport: str | None = None,
        status: str | None = None,
    ) -> list[dict[str, Any]]:
        """返回每个航班与机场组合在指定投影版本下的最新影响。

        先在每个事件内部取不超过请求版本的最高版本行（更正视图叠加在原始
        裁定之上），再按事件提交顺序（events.rowid，单写者下严格递增）取
        每个航班/机场组合的最新快照。`resolved` 墓碑参与排序，使恢复开放
        后释放的航班不再出现在结果中。最终结果按 flight_id 稳定排序。
        """
        where = ["v.airport_code = ?"] if airport else []
        params: list[Any] = [projection_version]
        if airport:
            params.append(airport)
        where_sql = ("AND " + " AND ".join(where)) if where else ""

        outer = ["rn = 1", "impact_status != 'resolved'"]
        outer_params: list[Any] = []
        if status:
            outer.append("impact_status = ?")
            outer_params.append(status)

        sql = f"""
        WITH versioned AS (
            SELECT i.*,
                   ROW_NUMBER() OVER (
                       PARTITION BY i.event_id, i.flight_id, i.airport_code
                       ORDER BY i.projection_version DESC
                   ) AS vn
            FROM impacts i
            WHERE i.projection_version <= ?
        ),
        ranked AS (
            SELECT v.*,
                   ROW_NUMBER() OVER (
                       PARTITION BY v.flight_id, v.airport_code
                       ORDER BY e.rowid DESC
                   ) AS rn
            FROM versioned v
            JOIN events e ON e.event_id = v.event_id
            WHERE v.vn = 1 {where_sql}
        )
        SELECT * FROM ranked
        WHERE {' AND '.join(outer)}
        ORDER BY flight_id, airport_code
        """
        with self._lock:
            return [dict(r) for r in self._conn.execute(sql, params + outer_params)]

    def prior_chain_impact_ids(self, conn, root_event_id: str) -> set[str]:
        rows = conn.execute(
            "SELECT DISTINCT flight_id FROM impacts WHERE root_event_id = ? "
            "AND impact_status != 'resolved'",
            (root_event_id,),
        ).fetchall()
        return {r["flight_id"] for r in rows}

    def list_projection_migrations(self) -> list[dict[str, Any]]:
        """按应用顺序返回全部投影迁移记录（审计线索）。"""
        with self._lock:
            return [
                dict(r)
                for r in self._conn.execute(
                    "SELECT * FROM projection_migrations ORDER BY id"
                )
            ]

    def list_impact_corrections(self) -> list[dict[str, Any]]:
        """返回全部行级更正记录（审计线索）。"""
        with self._lock:
            return [
                dict(r)
                for r in self._conn.execute("SELECT * FROM impact_corrections ORDER BY id")
            ]

    # ------------------------------------------------------------------ #
    # Writes (all callers run inside ``transaction``)
    # ------------------------------------------------------------------ #

    def transaction(self):
        return _Transaction(self._conn, self._lock)

    def insert_event(self, conn: sqlite3.Connection, event_dict: dict[str, Any]) -> None:
        conn.execute(
            """
            INSERT INTO events (event_id, event_version, event_type, airport_code,
                                effective_from, effective_until, reported_at,
                                supersedes_event_id, reason, payload_json,
                                replay_count, created_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 0, ?)
            """,
            (
                event_dict["event_id"],
                event_dict["event_version"],
                event_dict["event_type"],
                event_dict["airport_code"],
                event_dict["effective_from"],
                event_dict["effective_until"],
                event_dict["reported_at"],
                event_dict["supersedes_event_id"],
                event_dict["reason"],
                json.dumps(event_dict, sort_keys=True, ensure_ascii=False),
                utcnow_iso(),
            ),
        )

    def insert_impacts(
        self, conn: sqlite3.Connection, impacts: Iterable[dict[str, Any]]
    ) -> None:
        conn.executemany(
            """
            INSERT INTO impacts (event_id, root_event_id, airport_code, flight_id,
                                 flight_number, affected_endpoint, impact_status,
                                 overlap_minutes, delay_minutes, proposed_departure,
                                 proposed_arrival, passenger_count, crosses_midnight,
                                 projection_version)
            VALUES (:event_id, :root_event_id, :airport_code, :flight_id,
                    :flight_number, :affected_endpoint, :impact_status,
                    :overlap_minutes, :delay_minutes, :proposed_departure,
                    :proposed_arrival, :passenger_count, :crosses_midnight,
                    :projection_version)
            """,
            list(impacts),
        )

    def increment_replay(self, conn: sqlite3.Connection, event_id: str) -> None:
        conn.execute(
            "UPDATE events SET replay_count = replay_count + 1 WHERE event_id = ?",
            (event_id,),
        )


class _Transaction:
    """管理 BEGIN IMMEDIATE、COMMIT 与 ROLLBACK 的事务上下文。"""

    def __init__(self, conn: sqlite3.Connection, lock: threading.RLock):
        self._conn = conn
        self._lock = lock

    def __enter__(self) -> sqlite3.Connection:
        self._lock.acquire()
        self._conn.execute("BEGIN IMMEDIATE")
        return self._conn

    def __exit__(self, exc_type, exc, tb) -> None:
        try:
            if exc_type is None:
                self._conn.execute("COMMIT")
            else:
                self._conn.execute("ROLLBACK")
        finally:
            self._lock.release()
