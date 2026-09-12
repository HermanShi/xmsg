from __future__ import annotations

import json
import sqlite3
import tempfile
import unittest
from pathlib import Path

import sys

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))
import codex_migrate_thread as migrate


TID = "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa"


def make_home(root: Path, name: str) -> Path:
    home = root / name
    home.mkdir()
    (home / "sessions/2026/09/12").mkdir(parents=True)
    with sqlite3.connect(home / "state_5.sqlite") as db:
        db.execute("CREATE TABLE threads (id TEXT PRIMARY KEY, rollout_path TEXT NOT NULL, created_at INTEGER NOT NULL, updated_at INTEGER NOT NULL, source TEXT NOT NULL, model_provider TEXT NOT NULL, cwd TEXT NOT NULL, title TEXT NOT NULL, history_mode TEXT NOT NULL)")
    with sqlite3.connect(home / "thread_history_1.sqlite") as db:
        db.execute("CREATE TABLE thread_turns (thread_id TEXT NOT NULL, turn_id TEXT NOT NULL, rollout_ordinal INTEGER NOT NULL, status TEXT NOT NULL, PRIMARY KEY(thread_id, turn_id))")
        db.execute("CREATE TABLE thread_items (thread_id TEXT NOT NULL, turn_id TEXT NOT NULL, item_id TEXT NOT NULL, rollout_ordinal INTEGER NOT NULL, item_json TEXT NOT NULL, PRIMARY KEY(thread_id, turn_id, item_id))")
        db.execute("CREATE TABLE thread_history_projection_state (thread_id TEXT PRIMARY KEY, next_rollout_byte_offset INTEGER NOT NULL, next_rollout_ordinal INTEGER NOT NULL)")
    return home


class MigrateThreadTest(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp = tempfile.TemporaryDirectory(prefix="codex-migrate-thread-")
        self.addCleanup(self.tmp.cleanup)
        root = Path(self.tmp.name)
        self.src, self.dst = make_home(root, "src"), make_home(root, "dst")
        rollout = self.src / "sessions/2026/09/12/rollout.jsonl"
        lines = [json.dumps({"ordinal": i, "type": "item", "text": str(i)}) for i in range(2)]
        rollout.write_text("\n".join(lines) + "\n")
        self.rollout = rollout
        with sqlite3.connect(self.src / "state_5.sqlite") as db:
            db.execute("INSERT INTO threads VALUES (?,?,?,?,?,?,?,?,?)", (TID, str(rollout), 1, 2, "cli", "codex", "/tmp", "title", "paginated"))
        with sqlite3.connect(self.src / "thread_history_1.sqlite") as db:
            db.executemany("INSERT INTO thread_turns VALUES (?,?,?,?)", [(TID, "turn-a", 0, "completed")])
            db.executemany("INSERT INTO thread_items VALUES (?,?,?,?,?)", [(TID, "turn-a", "item-a", 0, "{}")])
            db.execute("INSERT INTO thread_history_projection_state VALUES (?,?,?)", (TID, rollout.stat().st_size, 2))

    def test_copies_rollout_rows_and_creates_backup(self) -> None:
        backup = Path(self.tmp.name) / "backup"
        result = migrate.migrate(self.src, self.dst, TID, backup)
        self.assertEqual(result["threads"], 1)
        self.assertEqual(result["thread_items"], 1)
        self.assertTrue((backup / "state_5.sqlite").is_file())
        with sqlite3.connect(self.dst / "state_5.sqlite") as db:
            row = db.execute("SELECT rollout_path FROM threads WHERE id=?", (TID,)).fetchone()
        self.assertEqual(Path(row[0]).read_bytes(), self.rollout.read_bytes())

    def test_second_run_is_idempotent(self) -> None:
        migrate.migrate(self.src, self.dst, TID, Path(self.tmp.name) / "backup1")
        result = migrate.migrate(self.src, self.dst, TID, Path(self.tmp.name) / "backup2")
        self.assertEqual(result["threads"], 0)
        self.assertEqual(result["thread_items"], 0)

    def test_existing_different_thread_is_rejected(self) -> None:
        with sqlite3.connect(self.dst / "state_5.sqlite") as db:
            db.execute("INSERT INTO threads VALUES (?,?,?,?,?,?,?,?,?)", (TID, "/different", 1, 2, "cli", "codex", "/tmp", "title", "paginated"))
        with self.assertRaisesRegex(migrate.MigrationError, "分叉"):
            migrate.migrate(self.src, self.dst, TID, Path(self.tmp.name) / "backup")

    def test_source_is_read_only(self) -> None:
        before = (self.src / "state_5.sqlite").read_bytes()
        migrate.migrate(self.src, self.dst, TID, Path(self.tmp.name) / "backup")
        self.assertEqual(before, (self.src / "state_5.sqlite").read_bytes())


if __name__ == "__main__":
    unittest.main()
