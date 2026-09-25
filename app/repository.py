"""SQLite 持久化层。

单一数据库文件同时保存事件和计算结果，因此一次提交可以原子写入事件及其
全部影响，失败时也不会留下部分数据。数据库位于挂载卷时，WAL 模式可在
容器重启后继续保留数据。

结果带裁定版本（``calc_version``）：每次影响语义变更都会抬升
``CALC_VERSION``，旧版本快照原样保留，可通过 projection_version 查询；
当前读取始终跟随每个事件自身的当前版本指针，因此事件详情、机场汇总与
航班分页引用的是同一计算版本，绝不会把不同裁定版本的行混在一起。
"""

from __future__ import annotations

import json
import sqlite3
import threading
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable

from app.models import CALC_VERSION

# 当前 schema（全新数据库直接建到该版本）。
SCHEMA = """
CREATE TABLE IF NOT EXISTS schema_migrations (
    version     INTEGER PRIMARY KEY,
    name        TEXT NOT NULL UNIQUE,
    applied_at  TEXT NOT NULL,
    description TEXT NOT NULL
);

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
    created_at           TEXT NOT NULL,
    calc_version         INTEGER NOT NULL DEFAULT {cv}
);

CREATE TABLE IF NOT EXISTS impacts (
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
    crosses_midnight   INTEGER,
    calc_version       INTEGER NOT NULL,
    UNIQUE(event_id, flight_id, airport_code, calc_version)
);

-- 每个事件在每个裁定版本下只有一条窗口裁定；跨午夜标志以这里为准，
-- impacts.crosses_midnight 只做行级冗余，二者在同一事务内写入。
CREATE TABLE IF NOT EXISTS window_verdicts (
    event_id         TEXT NOT NULL REFERENCES events(event_id),
    calc_version     INTEGER NOT NULL,
    root_event_id    TEXT NOT NULL,
    airport_code     TEXT NOT NULL,
    window_start     TEXT NOT NULL,
    window_end       TEXT,
    airport_timezone TEXT NOT NULL,
    crosses_midnight INTEGER,
    computed_at      TEXT NOT NULL,
    PRIMARY KEY (event_id, calc_version)
);

-- 可审计的历史更正：迁移把旧裁定改成新裁定时逐字段留痕。
CREATE TABLE IF NOT EXISTS projection_amendments (
    id                INTEGER PRIMARY KEY AUTOINCREMENT,
    migration_version INTEGER NOT NULL,
    event_id          TEXT NOT NULL,
    flight_id         TEXT,
    airport_code      TEXT NOT NULL,
    field_name        TEXT NOT NULL,
    old_value         TEXT,
    new_value         TEXT,
    reason            TEXT NOT NULL,
    applied_at        TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_impacts_root
    ON impacts(root_event_id, calc_version);
CREATE INDEX IF NOT EXISTS idx_impacts_airport
    ON impacts(airport_code, impact_status, calc_version);
CREATE INDEX IF NOT EXISTS idx_impacts_flight
    ON impacts(flight_id, calc_version);
CREATE INDEX IF NOT EXISTS idx_impacts_event_ver
    ON impacts(event_id, calc_version);
CREATE INDEX IF NOT EXISTS idx_events_airport
    ON events(airport_code, event_version);
""".format(cv=CALC_VERSION)

PROJECTION_V2 = 2
PROJECTION_BACKFILL_MARKER = "projection_v2_backfill"


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
            # 多进程（如部署侧脚本）并发打开同一卷上的库时，让写者等待
            # 而不是立即抛 SQLITE_BUSY；进程内并发仍由 self._lock 串行化。
            self._conn.execute("PRAGMA busy_timeout=5000")
            # 结构升级（v1 旧库 -> 当前版本）；幂等。
            self._conn.executescript(self._migrate_structure(self._conn))
            self._conn.executescript(SCHEMA)

    # ------------------------------------------------------------------ #
    # Schema migration
    # ------------------------------------------------------------------ #

    @staticmethod
    def _migrate_structure(conn: sqlite3.Connection) -> str:
        """返回需要在当前库上执行的结构 DDL（旧库升级），新库返回空串。"""
        tables = {
            r[0]
            for r in conn.execute(
                "SELECT name FROM sqlite_master WHERE type='table'"
            )
        }
        if "events" not in tables:
            return ""  # 全新数据库，由 SCHEMA 直接建当前版本。
        cols = {r[1] for r in conn.execute("PRAGMA table_info(events)")}
        if "calc_version" in cols:
            return ""  # 已是 v2 结构。

        # v1 -> v2：事件/影响增加裁定版本列，impacts 重建为版本复合唯一键，
        # 历史 v1 行原样保留（calc_version 默认 1）。语义回填由服务层完成。
        return """
        ALTER TABLE events ADD COLUMN calc_version INTEGER NOT NULL DEFAULT 1;
        ALTER TABLE impacts ADD COLUMN calc_version INTEGER NOT NULL DEFAULT 1;

        CREATE TABLE impacts_v2 (
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
            crosses_midnight   INTEGER,
            calc_version       INTEGER NOT NULL,
            UNIQUE(event_id, flight_id, airport_code, calc_version)
        );
        INSERT INTO impacts_v2
            SELECT id, event_id, root_event_id, airport_code, flight_id,
                   flight_number, affected_endpoint, impact_status,
                   overlap_minutes, delay_minutes, proposed_departure,
                   proposed_arrival, passenger_count, crosses_midnight,
                   calc_version
            FROM impacts;
        DROP TABLE impacts;
        ALTER TABLE impacts_v2 RENAME TO impacts;
        CREATE INDEX IF NOT EXISTS idx_impacts_root
            ON impacts(root_event_id, calc_version);
        CREATE INDEX IF NOT EXISTS idx_impacts_airport
            ON impacts(airport_code, impact_status, calc_version);
        CREATE INDEX IF NOT EXISTS idx_impacts_flight
            ON impacts(flight_id, calc_version);
        CREATE INDEX IF NOT EXISTS idx_impacts_event_ver
            ON impacts(event_id, calc_version);
        """

    def projection_backfill_required(self) -> bool:
        """旧库结构已升级、但 v2 语义快照尚未回填完成时为 True。"""
        with self._lock:
            row = self._conn.execute(
                "SELECT 1 FROM schema_migrations WHERE name = ?",
                (PROJECTION_BACKFILL_MARKER,),
            ).fetchone()
            if row is not None:
                return False
            legacy = self._conn.execute(
                "SELECT 1 FROM events WHERE calc_version = 1 LIMIT 1"
            ).fetchone()
            return legacy is not None

    def record_projection_backfill(self, conn) -> None:
        conn.execute(
            "INSERT OR IGNORE INTO schema_migrations "
            "(version, name, applied_at, description) VALUES (?, ?, ?, ?)",
            (
                PROJECTION_V2,
                PROJECTION_BACKFILL_MARKER,
                utcnow_iso(),
                "Recompute impacts under half-open midnight semantics; "
                "keep v1 snapshots queryable and log every changed verdict.",
            ),
        )

    # ------------------------------------------------------------------ #
    # Reads
    # ------------------------------------------------------------------ #

    def close(self) -> None:
        with self._lock:
            self._conn.close()

    def ping(self) -> bool:
        with self._lock:
            row = self._conn.execute("SELECT 1").fetchone()
            return row is not None and row[0] == 1

    def get_event_row(self, event_id: str) -> sqlite3.Row | None:
        with self._lock:
            return self._conn.execute(
                "SELECT * FROM events WHERE event_id = ?", (event_id,)
            ).fetchone()

    def get_impacts(self, event_id: str, calc_version: int) -> list[sqlite3.Row]:
        with self._lock:
            return list(
                self._conn.execute(
                    "SELECT * FROM impacts WHERE event_id = ? AND calc_version = ? "
                    "ORDER BY flight_id",
                    (event_id, calc_version),
                )
            )

    def get_window_verdict(self, event_id: str, calc_version: int):
        with self._lock:
            return self._conn.execute(
                "SELECT * FROM window_verdicts "
                "WHERE event_id = ? AND calc_version = ?",
                (event_id, calc_version),
            ).fetchone()

    def available_projection_versions(self, event_id: str) -> list[int]:
        with self._lock:
            rows = self._conn.execute(
                "SELECT DISTINCT calc_version FROM impacts WHERE event_id = ? "
                "UNION SELECT DISTINCT calc_version FROM window_verdicts "
                "WHERE event_id = ? ORDER BY calc_version",
                (event_id, event_id),
            ).fetchall()
            return [int(r[0]) for r in rows]

    def events_for_airport(self, airport_code: str) -> list[sqlite3.Row]:
        with self._lock:
            return list(
                self._conn.execute(
                    "SELECT * FROM events WHERE airport_code = ? "
                    "ORDER BY event_version, effective_from",
                    (airport_code,),
                )
            )

    def all_event_rows(self) -> list[sqlite3.Row]:
        """按机场、版本顺序返回全部事件（迁移回填用）。"""
        with self._lock:
            return list(
                self._conn.execute(
                    "SELECT * FROM events ORDER BY airport_code, event_version, event_id"
                )
            )

    def legacy_impacts(self, conn, event_id: str) -> list[sqlite3.Row]:
        return list(
            conn.execute(
                "SELECT * FROM impacts WHERE event_id = ? AND calc_version = 1 "
                "ORDER BY flight_id",
                (event_id,),
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
        airport: str | None = None,
        status: str | None = None,
        projection_version: int | None = None,
    ) -> list[dict[str, Any]]:
        """返回每个航班与机场组合在指定裁定版本下的最新影响。

        ``projection_version`` 为 None 时采用每个事件自身的当前版本指针
        （``i.calc_version = e.calc_version``），因此不同事件的行也必然
        来自同一代语义；指定版本时只取该版本快照，事件在该版本没有快照
        则不参与。同一机场内每条事件链采用最新事件的快照；航班同时出现
        在多条链时，采用最后生成的快照。`resolved` 墓碑参与排序，使恢复
        开放后释放的航班不再出现在结果中。最终按 flight_id 稳定排序。
        """
        where = ["i.airport_code = ?"] if airport else []
        params: list[Any] = [airport] if airport else []
        if projection_version is None:
            where.append("i.calc_version = e.calc_version")
        else:
            where.append("i.calc_version = ?")
            params.append(projection_version)
        where_sql = "WHERE " + " AND ".join(where)

        outer = ["rn = 1", "impact_status != 'resolved'"]
        outer_params: list[Any] = []
        if status:
            outer.append("impact_status = ?")
            outer_params.append(status)

        sql = f"""
        WITH ranked AS (
            SELECT i.*,
                   ROW_NUMBER() OVER (
                       PARTITION BY i.flight_id, i.airport_code
                       ORDER BY e.created_at DESC,
                                i.id DESC
                   ) AS rn
            FROM impacts i
            JOIN events e ON e.event_id = i.event_id
            {where_sql}
        )
        SELECT * FROM ranked
        WHERE {' AND '.join(outer)}
        ORDER BY flight_id, airport_code
        """
        with self._lock:
            return [dict(r) for r in self._conn.execute(sql, params + outer_params)]

    def prior_chain_impact_ids(
        self, conn, root_event_id: str, calc_version: int
    ) -> set[str]:
        rows = conn.execute(
            "SELECT DISTINCT flight_id FROM impacts "
            "WHERE root_event_id = ? AND calc_version = ? "
            "AND impact_status != 'resolved'",
            (root_event_id, calc_version),
        ).fetchall()
        return {r["flight_id"] for r in rows}

    def current_flag_inconsistencies(self) -> list[dict[str, Any]]:
        """返回当前版本指针下互相矛盾的跨午夜标志（健康检查/并发断言用）。

        矛盾有两类：

        * 同一事件当前版本的影响行标志与其窗口裁定不一致；
        * 同一事件当前版本的各影响行之间标志不一致。
        """
        with self._lock:
            rows = self._conn.execute(
                """
                SELECT i.event_id, i.flight_id, i.crosses_midnight AS impact_flag,
                       w.crosses_midnight AS verdict_flag
                FROM impacts i
                JOIN events e ON e.event_id = i.event_id AND e.calc_version = i.calc_version
                JOIN window_verdicts w
                    ON w.event_id = i.event_id AND w.calc_version = i.calc_version
                -- SQLite 的 IS NOT 对 NULL 也成立：NULL IS NOT 0 为真。
                WHERE i.crosses_midnight IS NOT w.crosses_midnight
                UNION ALL
                SELECT i.event_id, i.flight_id, i.crosses_midnight AS impact_flag,
                       NULL AS verdict_flag
                FROM impacts i
                JOIN (
                    SELECT event_id, calc_version
                    FROM impacts GROUP BY event_id, calc_version
                    HAVING COUNT(DISTINCT crosses_midnight) > 1
                ) d ON d.event_id = i.event_id AND d.calc_version = i.calc_version
                """
            ).fetchall()
            return [dict(r) for r in rows]

    def amendments(self, event_id: str | None = None) -> list[dict[str, Any]]:
        with self._lock:
            if event_id is None:
                rows = self._conn.execute(
                    "SELECT * FROM projection_amendments ORDER BY id"
                ).fetchall()
            else:
                rows = self._conn.execute(
                    "SELECT * FROM projection_amendments WHERE event_id = ? ORDER BY id",
                    (event_id,),
                ).fetchall()
            return [dict(r) for r in rows]

    # ------------------------------------------------------------------ #
    # Writes (all callers run inside ``transaction``)
    # ------------------------------------------------------------------ #

    def transaction(self):
        return _Transaction(self._conn, self._lock)

    def insert_event(
        self, conn: sqlite3.Connection, event_dict: dict[str, Any]
    ) -> None:
        conn.execute(
            """
            INSERT INTO events (event_id, event_version, event_type, airport_code,
                                effective_from, effective_until, reported_at,
                                supersedes_event_id, reason, payload_json,
                                replay_count, created_at, calc_version)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 0, ?, ?)
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
                CALC_VERSION,
            ),
        )

    def insert_window_verdict(self, conn, row: dict[str, Any]) -> None:
        conn.execute(
            """
            INSERT INTO window_verdicts (event_id, calc_version, root_event_id,
                                         airport_code, window_start, window_end,
                                         airport_timezone, crosses_midnight,
                                         computed_at)
            VALUES (:event_id, :calc_version, :root_event_id, :airport_code,
                    :window_start, :window_end, :airport_timezone,
                    :crosses_midnight, :computed_at)
            """,
            {**row, "computed_at": utcnow_iso()},
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
                                 calc_version)
            VALUES (:event_id, :root_event_id, :airport_code, :flight_id,
                    :flight_number, :affected_endpoint, :impact_status,
                    :overlap_minutes, :delay_minutes, :proposed_departure,
                    :proposed_arrival, :passenger_count, :crosses_midnight,
                    :calc_version)
            """,
            list(impacts),
        )

    def insert_amendment(
        self,
        conn,
        *,
        event_id: str,
        flight_id: str | None,
        airport_code: str,
        field_name: str,
        old_value: Any,
        new_value: Any,
        reason: str,
    ) -> None:
        conn.execute(
            """
            INSERT INTO projection_amendments (migration_version, event_id, flight_id,
                                               airport_code, field_name, old_value,
                                               new_value, reason, applied_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                PROJECTION_V2,
                event_id,
                flight_id,
                airport_code,
                field_name,
                None if old_value is None else str(old_value),
                None if new_value is None else str(new_value),
                reason,
                utcnow_iso(),
            ),
        )

    def mark_event_projection(self, conn, event_id: str, calc_version: int) -> None:
        conn.execute(
            "UPDATE events SET calc_version = ? WHERE event_id = ?",
            (calc_version, event_id),
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
