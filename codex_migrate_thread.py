#!/usr/bin/env python3
"""Safely append one paginated Codex thread to another CODEX_HOME.

The source databases and rollout are read-only.  The destination is backed up
before its two SQLite files are changed; existing rows are never replaced.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import shutil
import sqlite3
import sys
import time
import uuid
from pathlib import Path
from typing import Any


class MigrationError(Exception):
    pass


def connect(path: Path, readonly: bool) -> sqlite3.Connection:
    if readonly:
        return sqlite3.connect(path.resolve().as_uri() + "?mode=ro", uri=True)
    return sqlite3.connect(path)


def columns(db: sqlite3.Connection, table: str) -> list[str]:
    return [row[1] for row in db.execute(f'PRAGMA table_info("{table}")')]


def rows(db: sqlite3.Connection, table: str, thread_id: str) -> list[tuple[Any, ...]]:
    cols = columns(db, table)
    if not cols:
        raise MigrationError(f"数据库缺少表 {table}。")
    if "thread_id" not in cols:
        raise MigrationError(f"表 {table} 缺少 thread_id 列。")
    return db.execute(
        f'SELECT {",".join(chr(34) + c + chr(34) for c in cols)} FROM "{table}" WHERE "thread_id"=?',
        (thread_id,),
    ).fetchall()


def append_rows(src: sqlite3.Connection, dst: sqlite3.Connection, table: str, thread_id: str) -> int:
    src_cols, dst_cols = columns(src, table), columns(dst, table)
    if src_cols != dst_cols:
        raise MigrationError(f"{table} 的源/目标 schema 不一致。")
    source = rows(src, table, thread_id)
    target = rows(dst, table, thread_id)
    target_set = set(target)
    # A destination may contain newer turns, but a same-key row with changed
    # content is a fork and must never be silently overwritten.
    key_len = 3 if table == "thread_items" else 2 if table == "thread_turns" else 1
    source_by_key = {row[:key_len]: row for row in source}
    for row in target:
        if row[:key_len] in source_by_key and row != source_by_key[row[:key_len]]:
            raise MigrationError(f"{table} 存在内容冲突，拒绝覆盖目标分叉。")
    target_set = set(target)
    missing = [row for row in source if row not in target_set]
    if missing:
        placeholders = ",".join("?" for _ in dst_cols)
        dst.executemany(f'INSERT INTO "{table}" ({",".join(dst_cols)}) VALUES ({placeholders})', missing)
    return len(missing)


def validate_rollout(src_row: sqlite3.Row, src_home: Path, dst_home: Path, projection: tuple[Any, ...] | None) -> Path:
    raw = Path(str(src_row["rollout_path"])).expanduser()
    try:
        relative = raw.resolve().relative_to(src_home.resolve())
    except ValueError:
        raise MigrationError("rollout_path 不在源 CODEX_HOME 下，拒绝迁移。") from None
    source_path, target_path = raw, dst_home / relative
    if not source_path.is_file():
        raise MigrationError(f"源 rollout 不存在：{relative}")
    if not target_path.is_file():
        target_path.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(source_path, target_path)
    if source_path.stat().st_size != target_path.stat().st_size:
        raise MigrationError(f"目标 rollout 与源快照长度不同：{relative}")
    digest = lambda p: hashlib.sha256(p.read_bytes()).digest()
    if digest(source_path) != digest(target_path):
        raise MigrationError(f"目标 rollout 与源快照内容不同：{relative}")
    max_ordinal = -1
    with source_path.open(encoding="utf-8") as stream:
        for line_no, line in enumerate(stream, 1):
            try:
                item = json.loads(line)
            except json.JSONDecodeError:
                raise MigrationError(f"rollout 第 {line_no} 行不是有效 JSON。") from None
            if not isinstance(item, dict):
                raise MigrationError(f"rollout 第 {line_no} 行不是对象。")
            if isinstance(item.get("ordinal"), int):
                max_ordinal = max(max_ordinal, item["ordinal"])
    if projection is not None:
        offset, ordinal = projection
        if offset != source_path.stat().st_size or (max_ordinal >= 0 and ordinal < max_ordinal + 1):
            raise MigrationError("thread_history_projection_state 与 rollout 快照不一致。")
    return target_path


def migrate(src_home: Path, dst_home: Path, thread_id: str, backup_dir: Path | None) -> dict[str, Any]:
    try:
        thread_id = str(uuid.UUID(thread_id))
    except ValueError:
        raise MigrationError("thread 必须是完整 UUID。") from None
    src_state_path, dst_state_path = src_home / "state_5.sqlite", dst_home / "state_5.sqlite"
    src_history_path, dst_history_path = src_home / "thread_history_1.sqlite", dst_home / "thread_history_1.sqlite"
    for path in (src_state_path, src_history_path, dst_state_path, dst_history_path):
        if not path.is_file():
            raise MigrationError(f"找不到数据库：{path}")
    if backup_dir is None:
        backup_dir = dst_home / f"codex-migrate-backup-{time.strftime('%Y%m%d-%H%M%S')}"
    backup_dir = backup_dir.expanduser().resolve()
    backup_dir.mkdir(parents=True, exist_ok=False)
    try:
        # Snapshot before opening a writable connection.  A failed migration can
        # therefore always be restored without relying on SQLite rollback.
        for path in (dst_state_path, dst_history_path):
            shutil.copy2(path, backup_dir / path.name)
    except BaseException:
        shutil.rmtree(backup_dir, ignore_errors=True)
        raise
    with connect(src_state_path, True) as src_state, connect(dst_state_path, False) as dst_state, \
            connect(src_history_path, True) as src_history, connect(dst_history_path, False) as dst_history:
        src_state.row_factory = sqlite3.Row
        dst_state.row_factory = sqlite3.Row
        source = src_state.execute('SELECT * FROM threads WHERE id=?', (thread_id,)).fetchone()
        if source is None:
            raise MigrationError("源库没有该 thread。")
        target = dst_state.execute('SELECT * FROM threads WHERE id=?', (thread_id,)).fetchone()
        state_cols = columns(src_state, "threads")
        if state_cols != columns(dst_state, "threads"):
            raise MigrationError("threads 的源/目标 schema 不一致。")
        projection = None
        if "history_mode" in source.keys() and source["history_mode"] == "paginated":
            projection_row = src_history.execute(
                'SELECT next_rollout_byte_offset,next_rollout_ordinal FROM thread_history_projection_state WHERE thread_id=?',
                (thread_id,),
            ).fetchone()
            projection = tuple(projection_row) if projection_row else None
        target_rollout = validate_rollout(source, src_home, dst_home, projection)
        if target is not None:
            source_values, target_values = list(source), list(target)
            rollout_index = state_cols.index("rollout_path")
            source_values[rollout_index] = str(target_rollout)
            if target_values != source_values:
                raise MigrationError("目标 threads 行内容不同，拒绝覆盖目标分叉。")
        state_missing = 0
        if target is None:
            values = list(source)
            values[state_cols.index("rollout_path")] = str(target_rollout)
            placeholders = ",".join("?" for _ in state_cols)
            dst_state.execute(f'INSERT INTO threads ({",".join(state_cols)}) VALUES ({placeholders})', values)
            state_missing = 1
        copied = {"threads": state_missing}
        for table in ("thread_turns", "thread_items"):
            copied[table] = append_rows(src_history, dst_history, table, thread_id)
        if projection is not None:
            copied["projection"] = append_rows(src_history, dst_history, "thread_history_projection_state", thread_id)
        dst_state.commit()
        dst_history.commit()
    return copied | {"backup": str(backup_dir), "thread": thread_id}


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--src-home", type=Path, required=True)
    parser.add_argument("--dst-home", type=Path, required=True)
    parser.add_argument("--thread", required=True)
    parser.add_argument("--backup-dir", type=Path)
    args = parser.parse_args(argv)
    try:
        print(json.dumps(migrate(args.src_home.expanduser().resolve(), args.dst_home.expanduser().resolve(), args.thread, args.backup_dir), ensure_ascii=False, indent=2))
        return 0
    except (MigrationError, OSError, sqlite3.Error) as exc:
        print(f"codex-migrate-thread: {exc if isinstance(exc, MigrationError) else type(exc).__name__}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
