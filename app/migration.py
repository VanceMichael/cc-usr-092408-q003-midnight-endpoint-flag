"""投影版本迁移：以可审计方式更正已写入影响的跨日标志。

版本 1 的跨日判定把窗口终点所在自然日也算进窗口，恰好止于本地午夜的
左闭右开窗口被误标为跨日，夜航统计因此多算一天。版本 2 改为比较窗口
覆盖的最后时刻（``end`` 前一微秒）的本地日期。

迁移用当前引擎对每个事件的有效窗口重算标志，为被误标的行写入版本 2
的更正视图，并在 ``projection_migrations`` / ``impact_corrections``
中留下完整审计痕迹。旧版本行保持不动，历史裁定版本仍可查询。迁移按
元数据中的投影版本推进，重复执行（例如容器重启）是空操作。
"""

from __future__ import annotations

from typing import Any

from app.engine import CURRENT_PROJECTION_VERSION, airport_tz, chain_window
from app.models import EVENT_CLOSED, Airport, event_from_record
from app.repository import Repository, utcnow_iso
from app.timeutil import crosses_local_midnight

META_KEY = "projection_version"
MIGRATION_ACTOR = "system:projection-migration"
REASON_V1_TO_V2 = (
    "crosses_midnight half-open fix: a closure window ending exactly at local "
    "midnight no longer counts as crossing into the next day"
)


def ensure_current_projection(
    repo: Repository, airports: dict[str, Airport]
) -> None:
    """把数据库投影推进到当前计算版本；已是最新时为空操作。"""
    stored = repo.get_meta(META_KEY)
    if stored is None:
        if repo.has_impacts():
            # 版本化之前的库：既有行全部属于版本 1 的计算规则。
            stored = "1"
        else:
            with repo.transaction() as conn:
                repo.set_meta(conn, META_KEY, str(CURRENT_PROJECTION_VERSION))
            return
    version = int(stored)
    while version < CURRENT_PROJECTION_VERSION:
        if version == 1:
            _migrate_v1_to_v2(repo, airports)
        else:  # pragma: no cover - 尚无后续迁移步骤
            raise RuntimeError(f"No migration path from projection version {version}")
        version += 1


def _migrate_v1_to_v2(repo: Repository, airports: dict[str, Airport]) -> None:
    """重算每个事件有效窗口的跨日标志，更正误标行并留痕。"""
    with repo.transaction() as conn:
        event_rows = conn.execute("SELECT * FROM events ORDER BY rowid").fetchall()
        by_id = {r["event_id"]: r for r in event_rows}
        corrections: list[tuple[dict[str, Any], int]] = []
        examined = 0
        for row in event_rows:
            event = event_from_record(row)
            root = (
                event
                if event.event_type == EVENT_CLOSED
                else event_from_record(_root_row(row, by_id))
            )
            airport = airports[event.airport_code]
            window = chain_window(event, root, airport)
            # 开放式关闭没有结束时刻，不伪造结束日，标志保持 0。
            flag = 0
            if window.end is not None:
                flag = 1 if crosses_local_midnight(
                    window.start, window.end, airport_tz(airport)
                ) else 0
            stored = conn.execute(
                "SELECT * FROM impacts WHERE event_id = ? "
                "AND projection_version = 1 AND impact_status != 'resolved' "
                "ORDER BY flight_id",
                (event.event_id,),
            ).fetchall()
            for imp in stored:
                examined += 1
                if int(imp["crosses_midnight"]) != flag:
                    corrections.append((dict(imp), flag))

        cursor = conn.execute(
            """
            INSERT INTO projection_migrations (applied_at, from_version, to_version,
                                               reason, actor, rows_examined,
                                               rows_corrected)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            (
                utcnow_iso(),
                1,
                2,
                REASON_V1_TO_V2,
                MIGRATION_ACTOR,
                examined,
                len(corrections),
            ),
        )
        migration_id = cursor.lastrowid
        for old, flag in corrections:
            new_row = dict(old)
            new_row.pop("id")
            new_row["crosses_midnight"] = flag
            new_row["projection_version"] = 2
            repo.insert_impacts(conn, [new_row])
            conn.execute(
                """
                INSERT INTO impact_corrections (migration_id, event_id,
                                                root_event_id, airport_code,
                                                flight_id, field,
                                                old_value, new_value)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    migration_id,
                    old["event_id"],
                    old["root_event_id"],
                    old["airport_code"],
                    old["flight_id"],
                    "crosses_midnight",
                    str(old["crosses_midnight"]),
                    str(flag),
                ),
            )
        repo.set_meta(conn, META_KEY, "2")


def _root_row(row, by_id: dict[str, Any]):
    """沿 supersedes 链找到链首的 closed 事件行。"""
    seen: set[str] = set()
    current = row
    while current["supersedes_event_id"] is not None:
        ref = current["supersedes_event_id"]
        if ref in seen:
            raise RuntimeError("supersedes chain contains a cycle")
        seen.add(ref)
        current = by_id[ref]
        if current["event_type"] == EVENT_CLOSED:
            return current
    raise RuntimeError("extended/reopened event chain has no closed root")
