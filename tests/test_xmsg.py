#!/usr/bin/env python3
"""Tests for xmsg. Run: python3 -m unittest discover -s ~/agent-msg/tests -v

Each test gets its own database file, so nothing here touches the real queue.
"""

from __future__ import annotations

import importlib.util
import json
import os
import shutil
import socket
import sqlite3
import subprocess
import sys
import tempfile
import threading
import time
import unittest
from unittest import mock
from pathlib import Path

HERE = Path(__file__).resolve().parent
IMPL = HERE.parent / "xmsg.py"
HOOK_SH = HERE.parent / "xmsg-hook.sh"
FIND_SH = HERE.parent / "bin" / "xmsg-find-session"


def load_impl(db_path: Path):
    """Import xmsg.py fresh, with XMSG_DB pointed at a scratch file.

    DB_PATH is read at import time, so a reload is how the test picks its own
    database rather than the operator's real one.
    """
    os.environ["XMSG_DB"] = str(db_path)
    for key in (
        "XMSG_FROM",
        "XMSG_FROM_TOOL",
        "XMSG_FROM_SESSION",
        "CLAUDE_CODE_SESSION_ID",
        "CODEX_SESSION_ID",
        "CODEX_THREAD_ID",
        "CLAUDECODE",
        "XMSG_REMOTE",
        "XMSG_REMOTE_UP",
    ):
        os.environ.pop(key, None)
    spec = importlib.util.spec_from_file_location(f"xmsg_{db_path.stem}", IMPL)
    assert spec and spec.loader
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


PAYLOAD = {
    "session_id": "01a04cbe-1d17-72c2-9b49-54a4515f1a41",
    "turn_id": "01a04cbe-1d35-7de0-a3c6-5cd9f5fa9334",
    "transcript_path": None,
    "cwd": "/tmp/cxtest",
    "hook_event_name": "PreToolUse",
    "model": "gpt-5.6-terra",
    "permission_mode": "bypassPermissions",
    "tool_name": "Bash",
    "tool_input": {"command": "ls"},
    "tool_use_id": "exec-7eeb03e4",
}

# Antigravity (agy) native shape: camelCase fields, no event name, no model,
# no permission mode. Field names per the 2026-07-30 four-tool schema survey
# and the production lifecycle hook; Stop additionally carries
# terminationReason/fullyIdle, which xmsg does not need.
AGY_PAYLOAD = {
    "conversationId": "b9126cf9-099b-46a0-95b4-4774a1eb52be",
    "workspacePaths": ["/home/llm/agent-msg", "/home/llm"],
    "transcriptPath": (
        "/home/llm/.gemini/antigravity-cli/brain/"
        "b9126cf9-099b-46a0-95b4-4774a1eb52be/.system_generated/logs/transcript.jsonl"
    ),
    "artifactDirectoryPath": "/tmp/agy-artifacts",
}


class Base(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp = Path(tempfile.mkdtemp(prefix="xmsg-test-"))
        self.db = self.tmp / "messages.sqlite3"
        self.x = load_impl(self.db)
        self.sid = PAYLOAD["session_id"]
        # live_claude_sessions() looks at SOCK_DIRS; keep tests off the real
        # host sockets or `send all` / list would pick up this machine's sessions.
        self.sockdir = self.tmp / "cc-socks"
        self.sockdir.mkdir()
        self.x.SOCK_DIRS = [self.sockdir]
        # The arrival banner writes to the controlling terminal; redirect it
        # into tmp so delivery tests never paint the real terminal of whoever
        # is running the suite (and tests can assert on the bytes).
        self.x.TTY_PATH = str(self.tmp / "tty-sink")
        self.x.CODEX_HOME = self.tmp / "codex"
        # Both name indexes are module constants resolved at import, so pointing
        # CODEX_HOME at tmp does not move them. local_session_name() reads them
        # on every send, and a test left on the real ones would answer from this
        # machine's own session titles.
        self.x.CODEX_SESSION_INDEX = self.tmp / "codex" / "session_index.jsonl"
        self.x.CLAUDE_PROJECTS = self.tmp / "claude-projects"
        # agy wake isolation. The agy fixtures use real conversation ids, so
        # without this any module-level send to an agy peer finds the real
        # ~/.local/bin/agy and actually resumes a real conversation: empty
        # presence/proc dirs read as "never attached", and no binary degrades
        # the wake to a plain queue. TestAgyWake puts the pieces back.
        self.x.AGY_PRESENCE_DIR = self.tmp / "agy-presence"
        self.x.PROC_ROOT = self.tmp / "agy-proc"
        self.x.agy_bin = lambda: ""

    def tearDown(self) -> None:
        shutil.rmtree(self.tmp, ignore_errors=True)

    def queue(self, body: str, *, to: str | None = None, ttl: int | None = None) -> int:
        conn = self.x.connect()
        try:
            t = self.x.now()
            cur = conn.execute(
                "INSERT INTO messages (created_at, expires_at, from_label, from_tool, from_session, "
                "to_session, body) VALUES (?,?,?,?,?,?,?)",
                (t, t + (ttl if ttl is not None else 3600), "peer-a", "claude", "sess-a", to or self.sid, body),
            )
            return int(cur.lastrowid or 0)
        finally:
            conn.close()

    def queue_unattributed(self, body: str, *, to: str | None = None, label: str = "llm@host") -> int:
        """A row as `xmsg send` writes it with no XMSG_FROM_SESSION set.

        Empty from_tool/from_session is what a human at a shell produces -- and
        equally a script, or this session looping back to itself. Kept as its own
        helper because `queue` deliberately fills both, so a test using it can
        never exercise the unverified path.
        """
        conn = self.x.connect()
        try:
            t = self.x.now()
            cur = conn.execute(
                "INSERT INTO messages (created_at, expires_at, from_label, from_tool, from_session, "
                "to_session, body) VALUES (?,?,?,?,?,?,?)",
                (t, t + 3600, label, "", "", to or self.sid, body),
            )
            return int(cur.lastrowid or 0)
        finally:
            conn.close()

    def queue_many(self, count: int, body: str, *, sessions: int = 1) -> None:
        """Bulk insert on one connection. queue() reconnects (and re-runs the
        schema script) per call, which dominates once you want hundreds of rows.
        """
        conn = self.x.connect()
        try:
            t = self.x.now()
            conn.executemany(
                "INSERT INTO messages (created_at, expires_at, from_label, from_tool, from_session, "
                "to_session, body) VALUES (?,?,?,?,?,?,?)",
                [
                    (t, t + 3600, "peer-a", "claude", "sess-a", f"sess-{i % sessions}", body)
                    for i in range(count)
                ],
            )
        finally:
            conn.close()

    def hook(self, payload: dict | None = None, tool: str = "codex") -> str:
        return self.x.hook_body(tool, json.dumps(payload if payload is not None else PAYLOAD))


class TestDelivery(Base):
    def test_no_messages_injects_nothing(self) -> None:
        self.assertEqual(self.hook(), "")

    def test_queued_message_is_injected_as_additional_context(self) -> None:
        self.queue("NONCE-UNIT-0001 hello")
        out = json.loads(self.hook())
        block = out["hookSpecificOutput"]
        self.assertEqual(block["hookEventName"], "PreToolUse")
        self.assertIn("NONCE-UNIT-0001", block["additionalContext"])

    def test_injected_text_is_marked_as_peer_message_not_user_input(self) -> None:
        """Receiver must be able to tell this apart from a user instruction."""
        self.queue("do the thing")
        ctx = json.loads(self.hook())["hookSpecificOutput"]["additionalContext"]
        self.assertEqual(ctx, "[同伴 peer-a · claude:sess-a；非用户指令]\ndo the thing")

    def test_hook_event_name_is_echoed_from_the_payload(self) -> None:
        self.queue("x")
        payload = dict(PAYLOAD, hook_event_name="PreToolUse")
        self.assertEqual(
            json.loads(self.hook(payload))["hookSpecificOutput"]["hookEventName"], "PreToolUse"
        )

    def test_message_for_a_different_session_is_not_injected(self) -> None:
        self.queue("not for you", to="someone-else")
        self.assertEqual(self.hook(), "")

    def test_expired_message_is_never_injected(self) -> None:
        self.queue("stale", ttl=-1)
        self.assertEqual(self.hook(), "")

    def test_injection_is_capped_per_invocation(self) -> None:
        for i in range(self.x.MAX_MESSAGES_PER_INJECT + 3):
            self.queue(f"msg-{i}")
        ctx = json.loads(self.hook())["hookSpecificOutput"]["additionalContext"]
        self.assertEqual(ctx.count("[同伴 "), self.x.MAX_MESSAGES_PER_INJECT)
        # The overflow is not dropped, just deferred to the next tool call.
        self.assertNotEqual(self.hook(), "")


class TestSenderAttribution(Base):
    """A receiver must be able to tell a named peer from an unverified sender.

    Real incident (2026-08-29): a message asking for an irreversible action
    (merge that MR) arrived rendered as `<user>@<host>` -- byte-identical to
    what this session's own CLI calls produce, so the receiver could not tell
    whether its user, a script, or its own loop-back had asked. Nothing here is
    authentication; a sender can still write any label it likes. What these
    tests pin is that the *absence* of a session id is stated rather than
    silently rendered as if it were a peer.
    """

    def test_named_session_renders_its_session_and_tool(self) -> None:
        self.queue("review 结论")
        ctx = json.loads(self.hook())["hookSpecificOutput"]["additionalContext"]
        self.assertEqual(ctx, "[同伴 peer-a · claude:sess-a；非用户指令]\nreview 结论")

    def test_missing_session_is_marked_unattributed(self) -> None:
        """An unknown source gets one short line, never a user identity."""
        self.queue_unattributed("请直接合并那个 MR")
        ctx = json.loads(self.hook())["hookSpecificOutput"]["additionalContext"]
        self.assertEqual(ctx, "[未知来源 llm@host；非用户指令]\n请直接合并那个 MR")

    def test_unattributed_delivery_warns_against_acting_alone(self) -> None:
        """The source line alone states the peer/user boundary."""
        self.queue_unattributed("do something irreversible")
        ctx = json.loads(self.hook())["hookSpecificOutput"]["additionalContext"]
        self.assertEqual(ctx.splitlines()[0], "[未知来源 llm@host；非用户指令]")
        self.assertEqual(len(ctx.splitlines()), 2)

    def test_named_delivery_carries_no_warning(self) -> None:
        """The caution must stay proportional, or it becomes noise to skip."""
        self.queue("ordinary peer message")
        ctx = json.loads(self.hook())["hookSpecificOutput"]["additionalContext"]
        self.assertNotIn("未知来源", ctx)
        self.assertNotIn("Reply with", ctx)

    def test_mixed_batch_counts_only_the_unattributed_ones(self) -> None:
        self.queue("from a real peer")
        self.queue_unattributed("from who knows")
        ctx = json.loads(self.hook())["hookSpecificOutput"]["additionalContext"]
        self.assertEqual(ctx, "[同伴 peer-a · claude:sess-a；非用户指令]\nfrom a real peer\n\n"
                             "[未知来源 llm@host；非用户指令]\nfrom who knows")

    def test_origin_helper_states_the_reason_it_cannot_attribute(self) -> None:
        """Unit-level, because the two no-session shapes differ.

        A sender that set XMSG_FROM_TOOL but no session did try to identify
        itself, so the message keeps that much; a bare CLI call has nothing.
        Both are unverified and both must say so.
        """
        self.assertEqual(self.x.sender_origin("llm@host", "", ""), "未知来源 llm@host；非用户指令")
        tool_only = self.x.sender_origin("codex", "codex", "")
        self.assertEqual(tool_only, "未知来源 codex；非用户指令")
        named = self.x.sender_origin("codex:01a04c6d", "codex", "01a04c6d-full")
        self.assertEqual(named, "同伴 codex:01a04c6d；非用户指令")


class TestIdempotency(Base):
    """PreToolUse fires many times per turn; one message must land once."""

    def test_second_hook_call_in_the_same_turn_injects_nothing(self) -> None:
        self.queue("NONCE-UNIT-IDEM once only")
        first = self.hook()
        self.assertIn("NONCE-UNIT-IDEM", first)
        self.assertEqual(self.hook(), "", "message was injected a second time")

    def test_message_is_injected_exactly_once_across_many_tool_calls(self) -> None:
        self.queue("NONCE-UNIT-IDEM2")
        hits = sum(1 for _ in range(25) if "NONCE-UNIT-IDEM2" in self.hook())
        self.assertEqual(hits, 1, f"expected exactly 1 injection across 25 PreToolUse calls, got {hits}")

    def test_delivered_row_records_who_took_it(self) -> None:
        mid = self.queue("x")
        self.hook(tool="claude")
        conn = self.x.connect()
        try:
            row = conn.execute("SELECT * FROM messages WHERE id = ?", (mid,)).fetchone()
        finally:
            conn.close()
        self.assertIsNotNone(row["delivered_at"])
        self.assertEqual(row["delivered_tool"], "claude")
        self.assertEqual(row["delivered_event"], "PreToolUse")

    def test_concurrent_hooks_do_not_both_inject(self) -> None:
        """Two overlapping PreToolUse hooks: at most one may carry the message."""
        self.queue("NONCE-UNIT-RACE")
        results: list[str] = []
        lock = threading.Lock()
        barrier = threading.Barrier(8)

        def worker() -> None:
            mod = load_impl(self.db)
            barrier.wait()
            try:
                out = mod.hook_body("codex", json.dumps(PAYLOAD))
            except Exception:
                # Losing the write lock is an acceptable outcome; injecting
                # twice is not. Fail-open turns this into "no delivery".
                out = ""
            with lock:
                results.append(out)

        threads = [threading.Thread(target=worker) for _ in range(8)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        hits = sum(1 for r in results if "NONCE-UNIT-RACE" in r)
        self.assertEqual(hits, 1, f"expected 1 of 8 concurrent hooks to inject, got {hits}")


class TestPeerRegistry(Base):
    def test_tool_call_registers_the_session_as_a_delivery_target(self) -> None:
        self.hook()
        conn = self.x.connect()
        try:
            row = conn.execute("SELECT * FROM peers WHERE session_id = ?", (self.sid,)).fetchone()
        finally:
            conn.close()
        self.assertIsNotNone(row)
        self.assertEqual(row["tool"], "codex")
        self.assertEqual(row["cwd"], "/tmp/cxtest")
        self.assertEqual(row["model"], "gpt-5.6-terra")

    def test_injected_count_tracks_deliveries_not_hook_calls(self) -> None:
        self.queue("one")
        for _ in range(5):
            self.hook()
        conn = self.x.connect()
        try:
            row = conn.execute("SELECT injected_count FROM peers WHERE session_id = ?", (self.sid,)).fetchone()
        finally:
            conn.close()
        self.assertEqual(row["injected_count"], 1)

    def test_prefix_addressing_resolves_a_unique_peer(self) -> None:
        self.hook()
        conn = self.x.connect()
        try:
            self.assertEqual(self.x.resolve_target(conn, "01a04cbe", allow_unknown=False), [self.sid])
        finally:
            conn.close()

    def test_ambiguous_prefix_is_refused_rather_than_guessed(self) -> None:
        self.hook()
        self.hook(dict(PAYLOAD, session_id="01a04cbe-ffff-0000-0000-000000000000"))
        conn = self.x.connect()
        try:
            with self.assertRaises(SystemExit):
                self.x.resolve_target(conn, "01a04cbe", allow_unknown=False)
        finally:
            conn.close()

    def test_unknown_target_is_refused_without_force(self) -> None:
        conn = self.x.connect()
        try:
            with self.assertRaises(SystemExit):
                self.x.resolve_target(conn, "never-seen-this", allow_unknown=False)
            self.assertEqual(
                self.x.resolve_target(conn, "never-seen-this", allow_unknown=True), ["never-seen-this"]
            )
        finally:
            conn.close()

    def test_broadcast_targets_every_live_peer(self) -> None:
        self.hook()
        self.hook(dict(PAYLOAD, session_id="sess-second"))
        conn = self.x.connect()
        try:
            self.assertEqual(set(self.x.resolve_target(conn, "all", allow_unknown=False)), {self.sid, "sess-second"})
        finally:
            conn.close()

    def test_quiet_peer_drops_out_of_the_target_list(self) -> None:
        self.hook()
        conn = self.x.connect()
        try:
            conn.execute(
                "UPDATE peers SET last_seen_at = ? WHERE session_id = ?",
                (self.x.now() - self.x.PEER_STALE_SECONDS - 60, self.sid),
            )
            with self.assertRaises(SystemExit):
                self.x.resolve_target(conn, "all", allow_unknown=False)
        finally:
            conn.close()

    def test_exact_session_id_wins_over_a_same_named_session(self) -> None:
        self.hook()
        title = self.tmp / "titles" / "project" / "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa" / "custom-title.json"
        title.parent.mkdir(parents=True)
        title.write_text('{"customTitle":"target"}')
        self.x.CLAUDE_PROJECTS = title.parents[2]
        conn = self.x.connect()
        try:
            self.assertEqual(self.x.resolve_target(conn, self.sid, allow_unknown=False), [self.sid])
        finally:
            conn.close()

    def test_unique_custom_name_resolves_without_a_peer(self) -> None:
        title = self.tmp / "titles" / "project" / "bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb" / "custom-title.json"
        title.parent.mkdir(parents=True)
        title.write_text('{"customTitle":"unique-target"}')
        self.x.CLAUDE_PROJECTS = title.parents[2]
        conn = self.x.connect()
        try:
            self.assertEqual(self.x.resolve_target(conn, "unique-target", allow_unknown=False), ["bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb"])
        finally:
            conn.close()

    def test_ambiguous_historical_custom_name_is_refused(self) -> None:
        root = self.tmp / "titles"
        for sid in ("cccccccc-cccc-4ccc-8ccc-cccccccccccc", "dddddddd-dddd-4ddd-8ddd-dddddddddddd"):
            title = root / "project" / sid / "custom-title.json"
            title.parent.mkdir(parents=True)
            title.write_text('{"customTitle":"same-name"}')
        self.x.CLAUDE_PROJECTS = root
        conn = self.x.connect()
        try:
            with self.assertRaises(SystemExit):
                self.x.resolve_target(conn, "same-name", allow_unknown=False)
        finally:
            conn.close()

    # The label the sender puts in `from` is the string the receiver retypes to
    # answer. Nothing checked that it resolves back, which is how `claude:<id>`
    # -- resolvable by none of the three rules -- survived into delivery. These
    # close the loop: they fail if the sender ever emits an unaddressable label.
    def _titled_claude_session(self, sid: str, name: str) -> None:
        title = self.tmp / "titles" / "project" / sid / "custom-title.json"
        title.parent.mkdir(parents=True, exist_ok=True)
        title.write_text(json.dumps({"customTitle": name}), encoding="utf-8")
        self.x.CLAUDE_PROJECTS = title.parents[2]

    def _round_trip(self, label: str, sid: str) -> None:
        conn = self.x.connect()
        try:
            self.assertEqual(self.x.resolve_target(conn, label, allow_unknown=False), [sid])
        finally:
            conn.close()

    def test_claude_sender_label_resolves_back_to_the_sending_session(self) -> None:
        sid = "eeeeeeee-eeee-4eee-8eee-eeeeeeeeeeee"
        self._titled_claude_session(sid, "leader-x")
        with mock.patch.dict(os.environ, {"CLAUDE_CODE_SESSION_ID": sid}, clear=True):
            label, tool, session = self.x.default_sender_label()
        self.assertEqual((label, tool, session), ("leader-x", "claude", sid))
        self._round_trip(label, sid)

    def test_codex_sender_label_resolves_back_to_the_sending_thread(self) -> None:
        sid = "codex-thread-1"
        index = self.tmp / "codex-index.jsonl"
        index.write_text(
            '{"id":"codex-thread-1","thread_name":"old-name","updated_at":"2026-09-12T00:00:00Z"}\n'
            '{"id":"codex-thread-1","thread_name":"new-name","updated_at":"2026-09-13T00:00:00Z"}\n',
            encoding="utf-8",
        )
        self.x.CODEX_SESSION_INDEX = index
        with mock.patch.dict(os.environ, {"CODEX_SESSION_ID": sid}, clear=True):
            label, _, _ = self.x.default_sender_label()
        # Last occurrence wins, same as the resume picker and discover_sessions.
        self.assertEqual(label, "new-name")
        self._round_trip(label, sid)

    def test_unnamed_sender_falls_back_to_a_resolvable_id_prefix(self) -> None:
        self.hook()
        with mock.patch.dict(os.environ, {"CLAUDE_CODE_SESSION_ID": self.sid}, clear=True):
            label, _, _ = self.x.default_sender_label()
        self.assertEqual(label, self.sid[:8])
        self._round_trip(label, self.sid)

    def test_the_label_that_reaches_the_wire_is_still_an_address(self) -> None:
        """Asserts on the value delivery actually emits, not on the helper.

        `_deliver_one` used to prepend the sending tool *after*
        `default_sender_label` had produced a good name, so a correct label was
        never what went out. A test that stops at the helper cannot see that, so
        this one follows the same string through delivery into the envelope and
        back through resolution.
        """
        sid = "bbbbcccc-dddd-4eee-8fff-aaaabbbbcccc"
        self._titled_claude_session(sid, "leader-z")
        with mock.patch.dict(os.environ, {"CLAUDE_CODE_SESSION_ID": sid}, clear=True):
            label, tool, session = self.x.default_sender_label()
        conn = self.x.connect()
        try:
            with mock.patch.object(self.x, "direct_send", return_value=(True, "ok")) as direct:
                self.x._deliver_one(conn, {"tool": "claude"}, 1, "target", "body", label, tool, session)
            sent = direct.call_args.kwargs["label"]
            self.assertIn(f'from="{sent}"', self.x.envelope("body", label=sent, from_session=session, from_mode="bypass"))
            self.assertEqual(self.x.resolve_target(conn, sent, allow_unknown=False), [sid])
        finally:
            conn.close()

    def test_a_tool_prefixed_label_is_unaddressable(self) -> None:
        """The regression itself: pin that the old shape resolves to nothing.

        Without this the fix could be reverted and every other test would stay
        green, because they all assert on strings rather than on addressability.
        """
        sid = "ffffffff-ffff-4fff-8fff-ffffffffffff"
        self._titled_claude_session(sid, "leader-y")
        conn = self.x.connect()
        try:
            for bad in (f"claude:{sid[:8]}", "claude:leader-y"):
                with self.subTest(label=bad), self.assertRaises(SystemExit):
                    self.x.resolve_target(conn, bad, allow_unknown=False)
        finally:
            conn.close()

    def test_non_ascii_session_name_still_reaches_the_receiver_as_an_address(self) -> None:
        """A Chinese title is legal on both hosts but outside the attribute class.

        End to end: the sender keeps the readable name for its own records while
        the envelope carries the id prefix, so the receiver still has something
        it can answer instead of an unattributed message.
        """
        sid = "abcdefab-cdef-4bcd-8bcd-efabcdefabcd"
        self._titled_claude_session(sid, "审查-会话")
        with mock.patch.dict(os.environ, {"CLAUDE_CODE_SESSION_ID": sid}, clear=True):
            label, _, session = self.x.default_sender_label()
        self.assertEqual(label, "审查-会话")
        env = self.x.envelope("hi", label=label, from_session=session, from_mode="bypass")
        self.assertIn(f'from="{sid[:8]}"', env)
        self._round_trip(sid[:8], sid)

    def test_codex_session_index_is_discoverable_by_name(self) -> None:
        index = self.tmp / "session_index.jsonl"
        index.write_text('{"id":"codex-session","thread_name":"codex-target","updated_at":"2026-09-12T00:00:00Z"}\n')
        self.x.CODEX_SESSION_INDEX = index
        conn = self.x.connect()
        try:
            self.assertEqual(self.x.resolve_target(conn, "codex-target", allow_unknown=False), ["codex-session"])
        finally:
            conn.close()

    def test_codex_queue_reader_degrades_for_missing_or_corrupt_db(self) -> None:
        self.x.CODEX_QUEUE_DB = self.tmp / "missing.sqlite"
        self.assertEqual(self.x.codex_queue_rows(), [])
        broken = self.tmp / "broken.sqlite"
        broken.write_text("not sqlite")
        self.x.CODEX_QUEUE_DB = broken
        self.assertEqual(self.x.codex_queue_rows(), [])

    def test_codex_queue_rows_include_name_and_payload_summary(self) -> None:
        queue = self.tmp / "queue.sqlite"
        db = sqlite3.connect(queue)
        try:
            db.execute(
                "CREATE TABLE queued_items (id TEXT, thread_id TEXT, payload_json TEXT, "
                "queue_order INTEGER, created_at_ms INTEGER, updated_at_ms INTEGER)"
            )
            db.execute(
                "INSERT INTO queued_items VALUES (?,?,?,?,?,?)",
                ("item-1", "codex-session", '{"UserInput":{"content":[{"type":"text","text":"hello queue"}]}}', 0, 1000, 1000),
            )
            db.commit()
        finally:
            db.close()
        self.x.CODEX_QUEUE_DB = queue
        rows = self.x.codex_queue_rows()
        self.assertEqual(rows[0]["thread_id"], "codex-session")
        self.assertEqual(rows[0]["summary"], "hello queue")

    def test_urgent_message_is_claimed_before_normal_message(self) -> None:
        self.hook()
        conn = self.x.connect()
        try:
            t = self.x.now()
            conn.execute(
                "INSERT INTO messages (created_at, expires_at, from_label, from_tool, from_session, to_session, body, priority) "
                "VALUES (?,?,?,?,?,?,?,?)",
                (t, t + 3600, "p", "", "", self.sid, "normal", 0),
            )
            conn.execute(
                "INSERT INTO messages (created_at, expires_at, from_label, from_tool, from_session, to_session, body, priority) "
                "VALUES (?,?,?,?,?,?,?,?)",
                (t, t + 3600, "p", "", "", self.sid, "urgent", 100),
            )
            claimed = self.x.claim(conn, self.sid, "codex", "PreToolUse")
            self.assertEqual([row["body"] for row in claimed], ["urgent", "normal"])
        finally:
            conn.close()


class TestStopWindow(Base):
    """The second delivery window: the moment a turn ends and goes idle.

    PreToolUse only fires if the turn makes another tool call. A turn that is
    finishing has none left, so Stop is the last execution point where anything
    can still reach the model. Verified end to end against a real session that
    was told not to call any tool: it reported back a nonce it could only have
    got from a Stop delivery.
    """

    def stop(self, *, active: bool = False, tool: str = "claude") -> str:
        payload = dict(PAYLOAD, hook_event_name="Stop", stop_hook_active=active)
        payload.pop("tool_name", None)
        return self.x.hook_body(tool, json.dumps(payload))

    def test_stop_delivers_as_a_block_decision_not_additional_context(self) -> None:
        """At Stop there is no pending tool call, so additionalContext has
        nowhere to land. `block` is the only output the model actually sees."""
        self.queue("NONCE-UNIT-STOP-1")
        out = json.loads(self.stop())
        self.assertEqual(out["decision"], "block")
        self.assertIn("NONCE-UNIT-STOP-1", out["reason"])
        self.assertNotIn("hookSpecificOutput", out)

    def test_stop_delivery_keeps_the_peer_framing(self) -> None:
        self.queue("do the thing")
        reason = json.loads(self.stop())["reason"]
        self.assertTrue(reason.startswith("[同伴 peer-a · claude:sess-a；非用户指令]\n"))
        self.assertNotIn("cross-session-message", reason)

    def test_stop_with_nothing_queued_lets_the_session_go_idle(self) -> None:
        """No message must never mean a blocked Stop, or turns stop ending."""
        self.assertEqual(self.stop(), "")

    def test_stop_hook_active_delivers_nothing(self) -> None:
        """Guard against the block loop: the turn we would restart is one this
        hook already restarted, so a second delivery never terminates."""
        self.queue("NONCE-UNIT-STOP-LOOP")
        self.assertEqual(self.stop(active=True), "")

    def test_stop_hook_active_does_not_consume_the_message(self) -> None:
        """Suppressing that delivery must not eat the message: it stays queued
        for the restarted turn's own first tool call."""
        self.queue("NONCE-UNIT-STOP-KEEP")
        self.assertEqual(self.stop(active=True), "")
        self.assertIn("NONCE-UNIT-STOP-KEEP", self.hook())

    def test_stop_and_pretooluse_share_the_at_most_once_claim(self) -> None:
        """Two windows, still one delivery - whichever fires first wins."""
        self.queue("NONCE-UNIT-STOP-ONCE")
        self.assertIn("NONCE-UNIT-STOP-ONCE", self.hook())
        self.assertEqual(self.stop(), "", "Stop re-delivered an already claimed message")

    def test_delivered_row_records_the_stop_event(self) -> None:
        mid = self.queue("x")
        self.stop()
        conn = self.x.connect()
        try:
            row = conn.execute("SELECT * FROM messages WHERE id = ?", (mid,)).fetchone()
        finally:
            conn.close()
        self.assertEqual(row["delivered_event"], "Stop")
        self.assertEqual(row["delivered_tool"], "claude")


class TestAntigravityHook(Base):
    """Antigravity (agy) receive path: camelCase payload, event name via argv.

    agy payloads carry no hook_event_name, so the hook config passes the event
    as an argument (xmsg-hook.sh agy PreInvocation). Output contracts:
    PreInvocation -> injectSteps/ephemeralMessage, Stop -> decision "continue".
    """

    def setUp(self) -> None:
        super().setUp()
        self.sid = AGY_PAYLOAD["conversationId"]

    def agy(self, event: str = "PreInvocation", payload: dict | None = None) -> str:
        return self.x.hook_body(
            "agy",
            json.dumps(payload if payload is not None else AGY_PAYLOAD),
            event,
        )

    def peer_row(self):
        conn = self.x.connect()
        try:
            return conn.execute(
                "SELECT * FROM peers WHERE session_id = ?", (self.sid,)
            ).fetchone()
        finally:
            conn.close()

    def test_preinvocation_injects_via_inject_steps(self) -> None:
        self.queue("NONCE-AGY-0001 hello")
        out = json.loads(self.agy())
        steps = out["injectSteps"]
        self.assertEqual(len(steps), 1)
        self.assertIn("NONCE-AGY-0001", steps[0]["ephemeralMessage"])
        self.assertNotIn("hookSpecificOutput", out)

    def test_preinvocation_with_nothing_queued_outputs_nothing(self) -> None:
        self.assertEqual(self.agy(), "")

    def test_injected_text_keeps_peer_framing(self) -> None:
        """The receiver must be able to tell a peer message from user input,
        whatever the host's injection wrapper is called."""
        self.queue("do the thing")
        out = json.loads(self.agy())
        self.assertTrue(
            out["injectSteps"][0]["ephemeralMessage"].startswith(
                "[同伴 peer-a · claude:sess-a；非用户指令]\n"
            )
        )

    def test_stop_restarts_with_continue_decision(self) -> None:
        """agy's Stop contract is decision "continue" (Claude/Codex use
        "block" for the same restart-the-turn effect), reason becomes the
        restarted turn's input."""
        self.queue("NONCE-AGY-STOP-1")
        out = json.loads(self.agy("Stop"))
        self.assertEqual(out["decision"], "continue")
        self.assertIn("NONCE-AGY-STOP-1", out["reason"])
        self.assertNotIn("injectSteps", out)

    def test_stop_with_nothing_queued_lets_the_session_go_idle(self) -> None:
        self.assertEqual(self.agy("Stop"), "")

    def test_restarted_turns_stop_does_not_loop(self) -> None:
        """agy has no stop_hook_active field, so the anti-loop guarantee is the
        at-most-once claim: after a Stop delivery restarts the session, that
        turn's own Stop finds nothing and must stay silent."""
        self.queue("NONCE-AGY-LOOP")
        self.assertEqual(json.loads(self.agy("Stop"))["decision"], "continue")
        self.assertEqual(self.agy("Stop"), "")

    def test_preinvocation_and_stop_share_the_at_most_once_claim(self) -> None:
        self.queue("NONCE-AGY-ONCE")
        self.assertIn("NONCE-AGY-ONCE", self.agy())
        self.assertEqual(
            self.agy("Stop"), "", "Stop re-delivered an already claimed message"
        )

    def test_missing_event_argument_claims_nothing(self) -> None:
        """A misconfigured hook (no event argument) must not claim: the output
        would be in a shape agy cannot read, and a claimed-but-unreadable
        message is a lost one. It stays queued for a correctly-wired event."""
        self.queue("NONCE-AGY-NOEVENT")
        self.assertEqual(self.x.hook_body("agy", json.dumps(AGY_PAYLOAD)), "")
        out = json.loads(self.agy())
        self.assertIn("NONCE-AGY-NOEVENT", out["injectSteps"][0]["ephemeralMessage"])

    def test_conversation_id_registers_as_the_peer_session(self) -> None:
        self.agy()
        row = self.peer_row()
        self.assertIsNotNone(row)
        self.assertEqual(row["tool"], "agy")
        self.assertEqual(row["session_id"], self.sid)

    def test_session_id_fallback_when_conversation_id_is_missing(self) -> None:
        payload = {"session_id": "agy-legacy-1", "workspacePaths": ["/tmp/w"]}
        self.queue("NONCE-AGY-FALLBACK", to="agy-legacy-1")
        out = json.loads(self.agy(payload=payload))
        self.assertIn(
            "NONCE-AGY-FALLBACK", out["injectSteps"][0]["ephemeralMessage"]
        )

    def test_first_workspace_path_becomes_the_peer_cwd(self) -> None:
        self.agy()
        self.assertEqual(
            self.peer_row()["cwd"], AGY_PAYLOAD["workspacePaths"][0]
        )

    def test_no_host_coordinates_are_recorded(self) -> None:
        """agy has no direct-delivery channel; even when this test process runs
        inside a Claude terminal, the outer host's pid/socket must not be
        mis-attributed to the agy peer row."""
        self.agy()
        row = self.peer_row()
        self.assertIsNone(row["pid"])
        self.assertEqual(row["sock_path"], "")

    def test_delivered_row_records_event_and_tool(self) -> None:
        mid = self.queue("x")
        self.agy()
        conn = self.x.connect()
        try:
            row = conn.execute(
                "SELECT * FROM messages WHERE id = ?", (mid,)
            ).fetchone()
        finally:
            conn.close()
        self.assertEqual(row["delivered_event"], "PreInvocation")
        self.assertEqual(row["delivered_tool"], "agy")


class TestAgyWake(Base):
    """The wake fallback for agy conversations nobody is attached to.

    Liveness is an open presence-lock fd, not the lock file -- stale locks
    persist forever, so a file-only check would treat every dead conversation
    as busy and never wake one. A dead conversation gets resumed headlessly;
    the wake carries no body, because the resumed process's PreInvocation hook
    injects the queued rows through the normal at-most-once claim.
    """

    def setUp(self) -> None:
        super().setUp()
        self.sid = AGY_PAYLOAD["conversationId"]
        self.presence = self.tmp / "presence"
        self.presence.mkdir()
        self.x.AGY_PRESENCE_DIR = self.presence
        self.proc = self.tmp / "proc"
        self.proc.mkdir()
        self.x.PROC_ROOT = self.proc
        self.spawned: list[list[str]] = []
        patcher = mock.patch.object(
            self.x.subprocess, "Popen", side_effect=self._fake_popen
        )
        patcher.start()
        self.addCleanup(patcher.stop)
        # Base neutralized agy_bin so stray sends can never wake a real
        # conversation; this class is about the wake, so hand it a fake binary.
        self.fake_agy = self.tmp / "fake-agy"
        self.fake_agy.write_text("#!/bin/sh\n")
        self.x.agy_bin = lambda: str(self.fake_agy)

    def _fake_popen(self, cmd, **kwargs):
        self.spawned.append(list(cmd))
        return mock.Mock()

    def lock(self, conversation_id: str | None = None) -> Path:
        """A presence lock with no holder: attached once, nobody home now."""
        lock = self.presence / f"{conversation_id or self.sid}.lock"
        lock.write_text("")
        return lock

    def make_live(self, conversation_id: str | None = None) -> None:
        """Same lock, but a running process holds it open -- the live signal."""
        lock = self.lock(conversation_id)
        fd = self.proc / "424242" / "fd"
        fd.mkdir(parents=True, exist_ok=True)
        os.symlink(str(lock), str(fd / "7"))

    def register_agy_peer(self) -> None:
        self.x.hook_body("agy", json.dumps(AGY_PAYLOAD), "PreInvocation")

    def deliver(self, body: str, mid: int) -> tuple[bool, str]:
        conn = self.x.connect()
        try:
            out = self.x.direct_deliver(
                conn, [mid], [self.sid], body, "peer-a", "claude", "sess-a"
            )
        finally:
            conn.close()
        return out[mid]

    def queued_state(self, mid: int) -> tuple:
        conn = self.x.connect()
        try:
            row = conn.execute(
                "SELECT delivered_at, delivered_event FROM messages WHERE id = ?",
                (mid,),
            ).fetchone()
            return (row["delivered_at"], row["delivered_event"])
        finally:
            conn.close()

    def test_stale_lock_alone_is_not_live(self) -> None:
        self.lock()
        self.assertFalse(self.x.agy_conversation_live(self.sid))

    def test_open_lock_fd_is_live(self) -> None:
        self.make_live()
        self.assertTrue(self.x.agy_conversation_live(self.sid))

    def test_conversation_never_attached_is_not_live(self) -> None:
        self.assertFalse(self.x.agy_conversation_live("no-such-conversation"))

    def test_dead_conversation_send_dispatches_a_wake(self) -> None:
        self.register_agy_peer()
        self.lock()
        mid = self.queue("NONCE-AGY-WAKE-1")
        ok, detail = self.deliver("NONCE-AGY-WAKE-1", mid)
        self.assertFalse(ok, "a wake is a kick, not a delivery")
        self.assertIn("wake dispatched", detail)
        self.assertEqual(len(self.spawned), 1)
        cmd = self.spawned[0]
        self.assertEqual(cmd[0], str(self.fake_agy))
        self.assertEqual(cmd[cmd.index("--conversation") + 1], self.sid)
        self.assertIn("-p", cmd)
        # The bare-number trap that made the channel look broken: agy parses
        # --print-timeout as a Go duration and rejects "600" without a unit.
        self.assertRegex(cmd[cmd.index("--print-timeout") + 1], r"^\d+[smh]$")
        self.assertEqual(
            self.queued_state(mid), (None, None),
            "the wake must not mark the row delivered; the claim is the hook's job",
        )

    def test_live_conversation_is_left_to_its_own_hooks(self) -> None:
        """Resuming an attached conversation would fight the running process
        over its presence lock; a live session's hooks already have windows."""
        self.register_agy_peer()
        self.make_live()
        mid = self.queue("NONCE-AGY-WAKE-2")
        ok, detail = self.deliver("NONCE-AGY-WAKE-2", mid)
        self.assertFalse(ok)
        self.assertIn("hook-only", detail)
        self.assertEqual(self.spawned, [])
        self.assertEqual(self.queued_state(mid), (None, None))

    def test_cooldown_prevents_a_wake_storm(self) -> None:
        """Burst sends must not spawn a pile of resume processes racing for the
        same lock; one kick per cooldown window is enough for all queued rows."""
        self.register_agy_peer()
        self.lock()
        mid1 = self.queue("NONCE-AGY-WAKE-3")
        self.deliver("NONCE-AGY-WAKE-3", mid1)
        mid2 = self.queue("NONCE-AGY-WAKE-4")
        ok, detail = self.deliver("NONCE-AGY-WAKE-4", mid2)
        self.assertFalse(ok)
        self.assertIn("cooldown", detail)
        self.assertEqual(len(self.spawned), 1)
        self.assertEqual(self.queued_state(mid2), (None, None))

    def test_missing_binary_reports_hook_only_and_queues(self) -> None:
        self.register_agy_peer()
        self.lock()
        with mock.patch.object(self.x, "agy_bin", return_value=""):
            mid = self.queue("NONCE-AGY-WAKE-5")
            ok, detail = self.deliver("NONCE-AGY-WAKE-5", mid)
        self.assertFalse(ok)
        self.assertIn("no agy binary", detail)
        self.assertEqual(self.spawned, [])
        self.assertEqual(self.queued_state(mid), (None, None))

    def test_spawn_failure_is_not_fatal_and_keeps_the_row(self) -> None:
        self.register_agy_peer()
        self.lock()
        with mock.patch.object(
            self.x.subprocess, "Popen", side_effect=OSError("boom")
        ):
            mid = self.queue("NONCE-AGY-WAKE-6")
            ok, detail = self.deliver("NONCE-AGY-WAKE-6", mid)
        self.assertFalse(ok)
        self.assertIn("wake failed", detail)
        self.assertEqual(self.queued_state(mid), (None, None))


class TestNameAndLabel(Base):
    """`xmsg name`: the naming path for hosts without a name index.

    agy has no custom-title/thread-name file, so the peer row's label is the
    only persistent session name it can have. These cover both directions the
    name has to work: addressing the session (`xmsg send <name>`) and the
    sender signing with it automatically so receivers can reply by name.
    """

    def setUp(self) -> None:
        super().setUp()
        self.agy_sid = AGY_PAYLOAD["conversationId"]

    def agy_hook(self, event: str = "PreInvocation") -> str:
        return self.x.hook_body("agy", json.dumps(AGY_PAYLOAD), event)

    def peer_label(self) -> str:
        conn = self.x.connect()
        try:
            row = conn.execute(
                "SELECT label FROM peers WHERE session_id = ?", (self.agy_sid,)
            ).fetchone()
        finally:
            conn.close()
        return str(row["label"]) if row is not None else None

    def test_named_session_is_addressable_by_name(self) -> None:
        self.agy_hook()
        self.x.main(["name", self.agy_sid, "agy-main"])
        conn = self.x.connect()
        try:
            resolved = self.x.resolve_target(conn, "agy-main", allow_unknown=False)
        finally:
            conn.close()
        self.assertEqual(resolved, [self.agy_sid])

    def test_name_survives_subsequent_hook_registration(self) -> None:
        """Hook registration deliberately never touches the label column, so
        the name must outlive every following turn's re-registration."""
        self.agy_hook()
        self.x.main(["name", self.agy_sid, "agy-main"])
        self.agy_hook()
        self.agy_hook("Stop")
        self.assertEqual(self.peer_label(), "agy-main")

    def test_send_by_name_delivers_to_the_named_session(self) -> None:
        self.agy_hook()
        self.x.main(["name", self.agy_sid, "agy-main"])
        self.x.main(["send", "agy-main", "NONCE-NAME-1 by name"])
        out = json.loads(self.agy_hook())
        self.assertIn("NONCE-NAME-1", out["injectSteps"][0]["ephemeralMessage"])

    def test_discovery_exposes_the_name(self) -> None:
        self.agy_hook()
        self.x.main(["name", self.agy_sid, "agy-main"])
        conn = self.x.connect()
        try:
            names = self.x._queue_name_map(conn)
        finally:
            conn.close()
        self.assertEqual(names.get(self.agy_sid), "agy-main")

    def test_empty_label_clears_the_name(self) -> None:
        self.agy_hook()
        self.x.main(["name", self.agy_sid, "agy-main"])
        self.x.main(["name", "agy-main", ""])
        self.assertEqual(self.peer_label(), "")

    def test_naming_an_unregistered_session_is_refused(self) -> None:
        with self.assertRaises(SystemExit):
            self.x.main(["name", "no-such-session", "whatever"])

    def test_whitespace_label_is_refused(self) -> None:
        self.agy_hook()
        with self.assertRaises(SystemExit):
            self.x.main(["name", self.agy_sid, "two words"])

    def test_sender_signs_with_its_peer_label(self) -> None:
        """agy 侧的「发送名称」：没有 XMSG_FROM、host 也没有名字索引时，发送方
        自动用 `xmsg name` 设置的名字署名 —— 接收方按名字回信即可解析。"""
        self.agy_hook()
        self.x.main(["name", self.agy_sid, "agy-main"])
        with mock.patch.dict(
            os.environ,
            {"XMSG_FROM_TOOL": "agy", "XMSG_FROM_SESSION": self.agy_sid},
        ):
            label, tool, session = self.x.default_sender_label()
        self.assertEqual(label, "agy-main")
        self.assertEqual(tool, "agy")
        self.assertEqual(session, self.agy_sid)

    def test_explicit_xmsg_from_still_wins_over_peer_label(self) -> None:
        self.agy_hook()
        self.x.main(["name", self.agy_sid, "agy-main"])
        with mock.patch.dict(
            os.environ,
            {
                "XMSG_FROM": "per-message-override",
                "XMSG_FROM_TOOL": "agy",
                "XMSG_FROM_SESSION": self.agy_sid,
            },
        ):
            label, _, _ = self.x.default_sender_label()
        self.assertEqual(label, "per-message-override")


class TestTtyNotification(Base):
    """_notify_tty: opt-in single-line banner - CRLF, sanitized, non-blocking.

    The banner is OFF by default: under a host TUI our /dev/tty bytes land at
    the host's cursor (its input box) and garble on repaint - a real agy user
    reported exactly that, which no byte-level test can see. When enabled
    (XMSG_TTY_BANNER=1) it is written *after* the rows are claimed, so it must
    never hang (a flow-controlled terminal would let the wrapper timeout kill
    the hook and lose the injection) and never echo peer-supplied escape
    sequences onto the user's terminal. Base redirects TTY_PATH into tmp.
    """

    def setUp(self) -> None:
        super().setUp()
        # The shape tests exercise the enabled banner; the opt-in gate itself
        # has its own test below.
        patcher = mock.patch.dict(os.environ, {"XMSG_TTY_BANNER": "1"})
        patcher.start()
        self.addCleanup(patcher.stop)

    def row(self, label: str = "peer-a", body: str = "hello") -> dict:
        return {"from_label": label, "from_tool": "claude", "from_session": "s1", "body": body}

    def banner(self) -> str:
        # read_bytes, NOT read_text: text mode's universal-newline translation
        # would silently rewrite the very \r\n this class is asserting on.
        p = Path(self.x.TTY_PATH)
        return p.read_bytes().decode("utf-8", errors="replace") if p.exists() else ""

    def test_banner_is_one_line_wrapped_in_crlf(self) -> None:
        self.x._notify_tty([self.row()])
        content = self.banner()
        self.assertTrue(content.startswith("\r\n"))
        self.assertTrue(content.endswith("\r\n"))
        self.assertEqual(content.count("\n"), 2, "banner must stay a single line")
        self.assertIn("1 条跨会话消息", content)
        self.assertIn("peer-a", content)

    def test_every_newline_is_preceded_by_cr(self) -> None:
        """Raw-mode terminals: a bare \\n is the staircase effect."""
        self.x._notify_tty([self.row(body="multi\nline body")])
        content = self.banner()
        for i, ch in enumerate(content):
            if ch == "\n":
                self.assertEqual(content[i - 1], "\r", f"bare \\n at offset {i}")

    def test_banner_is_opt_in_and_silent_by_default(self) -> None:
        """The regression the user saw: under a TUI the banner lands on the
        host's input box. Without XMSG_TTY_BANNER=1 nothing may be written."""
        env_without = {k: v for k, v in os.environ.items() if k != "XMSG_TTY_BANNER"}
        with mock.patch.dict(os.environ, env_without, clear=True):
            self.x._notify_tty([self.row()])
        self.assertEqual(self.banner(), "")

    def test_no_rows_no_banner(self) -> None:
        self.x._notify_tty([])
        self.assertEqual(self.banner(), "")

    def test_peer_supplied_ansi_and_control_chars_are_stripped(self) -> None:
        """from_label/body are attacker-controllable: echoing raw escapes
        would let a sender paint the receiver's terminal."""
        self.x._notify_tty(
            [self.row(label="evil\x1b[31mRED\x1b[0m\x07", body="x\x1b[2Jy\x00z")]
        )
        content = self.banner()
        # Only the banner's own two color codes may contain ESC.
        self.assertEqual(content.count("\x1b"), 2)
        self.assertIn("evilRED", content)
        self.assertIn("xyz", content)

    def test_banner_is_clipped_to_terminal_width(self) -> None:
        with mock.patch(
            "shutil.get_terminal_size", return_value=os.terminal_size((60, 24))
        ):
            self.x._notify_tty([self.row(label="来源很长" * 10, body="内容很长" * 100)])
        plain = (
            self.banner().replace("\x1b[1;36m", "").replace("\x1b[0m", "").strip("\r\n")
        )
        self.assertLessEqual(self.x._display_width(plain), 60)
        self.assertTrue(plain.endswith("…"))

    def test_display_width_counts_cjk_as_two_and_controls_as_zero(self) -> None:
        self.assertEqual(self.x._display_width("ab中文"), 6)
        # ESC itself counts 0, but the sequence *body* is visible text - which
        # is exactly why echo paths must run _sanitize_for_tty, not just width.
        self.assertEqual(self.x._display_width("\x1b"), 0)
        self.assertEqual(self.x._display_width("\x1b[31m"), 4)
        self.assertEqual(self.x._display_width(""), 0)

    def test_many_senders_are_summarized_not_listed(self) -> None:
        rows = [self.row(label=f"peer-{i}") for i in range(5)]
        self.x._notify_tty(rows)
        content = self.banner()
        self.assertIn("5 条跨会话消息", content)
        self.assertIn("等 5 方", content)
        self.assertNotIn("peer-4", content)

    def test_unreadable_terminal_does_not_hang_or_raise(self) -> None:
        """O_NONBLOCK regression: opening a FIFO with no reader blocks forever
        in blocking mode; non-blocking gets ENXIO immediately and skips. The
        claimed rows must never depend on the terminal accepting a write."""
        fifo = self.tmp / "dead-tty"
        os.mkfifo(fifo)
        self.x.TTY_PATH = str(fifo)
        start = time.monotonic()
        self.x._notify_tty([self.row()])
        self.assertLess(time.monotonic() - start, 2.0)

    def test_missing_tty_is_a_quiet_noop(self) -> None:
        self.x.TTY_PATH = str(self.tmp / "nonexistent" / "tty")
        self.x._notify_tty([self.row()])  # must not raise

    def test_hook_delivery_paints_the_redirected_tty(self) -> None:
        """Integration: the claim path fires the banner (redirected into tmp),
        so delivery notifies without touching the real terminal in tests."""
        self.queue("NONCE-TTY-1")
        self.hook()
        self.assertIn("1 条跨会话消息", self.banner())


class TestSweep(Base):
    def test_sweep_expires_a_message_past_its_ttl(self) -> None:
        mid = self.queue("stale", ttl=-1)
        conn = self.x.connect()
        try:
            self.x.sweep(conn)
            row = conn.execute("SELECT expired_at, delivered_at FROM messages WHERE id = ?", (mid,)).fetchone()
        finally:
            conn.close()
        self.assertIsNotNone(row["expired_at"])
        self.assertIsNone(row["delivered_at"])

    def test_sweep_leaves_a_live_message_alone(self) -> None:
        mid = self.queue("fresh")
        conn = self.x.connect()
        try:
            self.x.sweep(conn)
            row = conn.execute("SELECT expired_at FROM messages WHERE id = ?", (mid,)).fetchone()
        finally:
            conn.close()
        self.assertIsNone(row["expired_at"])

    def age_finished_rows(self, seconds: int) -> None:
        """Backdate every finished row so it falls outside the retention window."""
        conn = self.x.connect()
        try:
            conn.execute(
                "UPDATE messages SET delivered_at = ? WHERE delivered_at IS NOT NULL",
                (self.x.now() - seconds,),
            )
            conn.execute(
                "UPDATE messages SET expired_at = ? WHERE expired_at IS NOT NULL",
                (self.x.now() - seconds,),
            )
        finally:
            conn.close()

    def count_messages(self) -> int:
        conn = self.x.connect()
        try:
            return int(conn.execute("SELECT COUNT(*) c FROM messages").fetchone()["c"])
        finally:
            conn.close()

    def test_delivered_row_is_deleted_once_past_retention(self) -> None:
        """A read message does not stay forever - retention is a window, not storage."""
        self.queue("done with this one")
        self.hook()
        self.age_finished_rows(self.x.RETAIN_DELIVERED_SECONDS + 86400)
        conn = self.x.connect()
        try:
            self.assertEqual(self.x.sweep(conn), 1)
        finally:
            conn.close()
        self.assertEqual(self.count_messages(), 0)

    def test_delivered_row_survives_inside_retention(self) -> None:
        """`xmsg outbox` has to be able to answer "did it land" for a while."""
        self.queue("recent")
        self.hook()
        conn = self.x.connect()
        try:
            self.x.sweep(conn)
        finally:
            conn.close()
        self.assertEqual(self.count_messages(), 1)

    def test_receive_only_machine_still_sweeps(self) -> None:
        """The gap this closes: sweeping used to happen on send only, so a
        machine that only ever received never swept and grew without bound."""
        for i in range(5):
            self.queue(f"msg-{i}")
        self.hook()
        self.age_finished_rows(self.x.RETAIN_DELIVERED_SECONDS + 86400)
        self.assertEqual(self.count_messages(), 5)
        # Nothing is sent from here on - only the hook path runs.
        conn = self.x.connect()
        try:
            conn.execute(
                "INSERT INTO meta (key, value) VALUES ('last_sweep_at', ?) "
                "ON CONFLICT(key) DO UPDATE SET value = excluded.value",
                (self.x.now() - self.x.HOOK_SWEEP_INTERVAL_SECONDS - 1,),
            )
        finally:
            conn.close()
        self.hook()
        self.assertEqual(self.count_messages(), 0, "receive-only path never swept")

    def test_hook_sweep_is_throttled(self) -> None:
        """PreToolUse fires dozens of times per turn; sweeping every time would
        spend the hook's budget on housekeeping instead of delivery."""
        conn = self.x.connect()
        try:
            self.assertTrue(self.x.sweep_if_due(conn), "first call should sweep")
            self.assertFalse(self.x.sweep_if_due(conn), "second call should be throttled")
        finally:
            conn.close()

    def test_hook_sweep_runs_again_once_the_interval_passes(self) -> None:
        conn = self.x.connect()
        try:
            self.assertTrue(self.x.sweep_if_due(conn))
            conn.execute(
                "UPDATE meta SET value = ? WHERE key = 'last_sweep_at'",
                (self.x.now() - self.x.HOOK_SWEEP_INTERVAL_SECONDS - 1,),
            )
            self.assertTrue(self.x.sweep_if_due(conn), "should sweep again after the interval")
        finally:
            conn.close()

    def test_sweeping_does_not_disturb_a_queued_message(self) -> None:
        """Housekeeping must never eat mail that has not been delivered yet."""
        self.queue("NONCE-UNIT-SWEEP-KEEP")
        conn = self.x.connect()
        try:
            self.x.sweep(conn)
            self.x.sweep_if_due(conn)
        finally:
            conn.close()
        self.assertIn("NONCE-UNIT-SWEEP-KEEP", self.hook())

    def test_deleting_rows_reclaims_disk_rather_than_leaving_a_hole(self) -> None:
        """DELETE returns pages to the freelist but never shrinks the file, so
        without VACUUM a traffic burst would inflate the database permanently."""
        self.queue_many(400, "x" * 4000, sessions=4)
        # claim() caps each call at MAX_MESSAGES_PER_INJECT, so drain rather than
        # assuming one hook call per session empties the queue.
        for i in range(4):
            payload = json.dumps(dict(PAYLOAD, session_id=f"sess-{i}"))
            while self.x.hook_body("claude", payload):
                pass
        peak = self.db.stat().st_size
        self.age_finished_rows(self.x.RETAIN_DELIVERED_SECONDS + 86400)
        conn = self.x.connect()
        try:
            self.x.sweep(conn)
            self.assertEqual(int(conn.execute("PRAGMA freelist_count").fetchone()[0]), 0)
        finally:
            conn.close()
        self.assertLess(self.db.stat().st_size, peak // 2, "file did not shrink after deletion")

    def test_vacuum_is_skipped_when_there_is_nothing_to_reclaim(self) -> None:
        """Keeps the rewrite off the common path: two pragmas and no work."""
        self.queue("small")
        conn = self.x.connect()
        try:
            self.assertFalse(self.x.reclaim(conn))
        finally:
            conn.close()


class TestEnvelope(Base):
    """The `<cross-session-message>` element handed to the host's own socket.

    Claude re-renders what it parsed and compares it to the input, so an
    attribute holding a character outside its class does not merely get ignored:
    the whole envelope fails to parse and the message degrades to unattributed.
    These tests pin the shape rather than trusting that.
    """

    def test_attributes_are_emitted_in_host_order(self) -> None:
        env = self.x.envelope("hi", label="peer-a", from_session="sess-a", from_mode="bypass")
        self.assertEqual(
            env,
            '<cross-session-message from="peer-a" from-session="sess-a" '
            'from-mode="bypass">\nhi\n</cross-session-message>',
        )

    def test_body_is_isolated_on_its_own_lines(self) -> None:
        # The host's regex anchors the body between newlines; a body glued to the
        # tags fails to match and the message arrives unattributed.
        env = self.x.envelope("line1\nline2", label="p", from_session="s", from_mode="bypass")
        self.assertIn(">\nline1\nline2\n</", env)

    def test_session_id_outside_the_class_is_dropped_not_emitted(self) -> None:
        # Better to lose the attribute than to void the envelope carrying it.
        env = self.x.envelope("hi", label="peer-a", from_session="not a valid id!", from_mode="bypass")
        self.assertNotIn("from-session", env)
        self.assertIn('from="peer-a"', env)

    def test_label_outside_the_class_falls_back_to_the_session_id(self) -> None:
        # A non-ASCII session title is legal on both hosts but outside this
        # attribute's class. Dropping `from` entirely would leave the receiver
        # with no address to answer, so it degrades to the id prefix instead.
        env = self.x.envelope("hi", label="péer a", from_session="sess-a", from_mode="bypass")
        self.assertIn('from="sess-a"', env)
        self.assertIn('from-session="sess-a"', env)

    def test_unusable_label_is_dropped_when_no_session_can_replace_it(self) -> None:
        env = self.x.envelope("hi", label="péer a", from_session="", from_mode="bypass")
        self.assertNotIn("from=", env)

    def test_unknown_mode_is_not_claimed(self) -> None:
        env = self.x.envelope("hi", label="p", from_session="s", from_mode="whatever")
        self.assertNotIn("from-mode", env)

    def test_no_attributes_still_renders(self) -> None:
        env = self.x.envelope("hi", label="", from_session="", from_mode="")
        self.assertEqual(env, "<cross-session-message>\nhi\n</cross-session-message>")


class TestClaimedMode(Base):
    """Mapping a host permission mode onto the class the receiver compares.

    An empty result is the meaningful case: it means "this sender attested
    nothing", which is what makes a bypass-mode receiver park the message for
    human review. Filling in a default here would forge an attestation.
    """

    def test_bypass_maps_to_bypass(self) -> None:
        self.assertEqual(self.x.claimed_mode("bypassPermissions"), "bypass")

    def test_prompting_modes_map_to_prompting(self) -> None:
        for mode in ("default", "plan", "ask", "acceptEdits"):
            with self.subTest(mode=mode):
                self.assertEqual(self.x.claimed_mode(mode), "prompting")

    def test_absent_or_unknown_claims_nothing(self) -> None:
        for mode in ("", "something-new", "BYPASSPERMISSIONS"):
            with self.subTest(mode=mode):
                self.assertEqual(self.x.claimed_mode(mode), "")

    def test_sender_with_no_peer_row_claims_nothing(self) -> None:
        conn = self.x.connect()
        try:
            self.assertEqual(self.x.sender_claimed_mode(conn, "nobody"), "")
            self.assertEqual(self.x.sender_claimed_mode(conn, ""), "")
        finally:
            conn.close()

    def test_sender_mode_is_read_from_its_own_peer_row(self) -> None:
        # The mode belongs to the sending session, not to the shell running the
        # command, so it is read back from what that session reported.
        self.hook(tool="claude")
        conn = self.x.connect()
        try:
            self.assertEqual(self.x.sender_claimed_mode(conn, self.sid), "bypass")
        finally:
            conn.close()


class DirectBase(Base):
    """Shared scaffolding: a live process that is not the test process itself.

    direct_send refuses to write to its own pid (a session must never be handed
    its own message back as a peer's), so every test that wants a *successful*
    or "live but unreachable" path needs a real third process to aim at.
    """

    def spawn_target(self) -> int:
        proc = subprocess.Popen(
            [sys.executable, "-c", "import time; time.sleep(30)"],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )
        self.addCleanup(proc.wait)
        self.addCleanup(proc.kill)
        # Wait for /proc to be readable; without this pid_start_time can race the
        # fork and read None, which would make the test refuse for the wrong reason.
        for _ in range(200):
            if self.x.pid_start_time(proc.pid) is not None:
                break
            time.sleep(0.01)
        self.assertIsNotNone(self.x.pid_start_time(proc.pid), "target process never appeared")
        return proc.pid

    def listener(self, path: Path) -> tuple[threading.Thread, list]:
        got: list = []
        srv = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        srv.bind(str(path))
        srv.listen(1)

        def serve() -> None:
            try:
                conn, _ = srv.accept()
                buf = b""
                while not buf.endswith(b"\n"):
                    chunk = conn.recv(65536)
                    if not chunk:
                        break
                    buf += chunk
                got.append(buf)
                conn.close()
            except OSError:
                pass
            finally:
                srv.close()

        th = threading.Thread(target=serve, daemon=True)
        th.start()
        return th, got


class TestDirectDeliveryTargeting(DirectBase):
    """Who direct delivery will and will not write to.

    Every refusal below leaves the row queued, so the hook path still delivers
    it. That is the whole safety argument for this feature: it can only ever be
    faster than the queue, never a way to lose a message.
    """

    def peer_row(self, **over):
        pid = self.spawn_target()
        base = {"pid": pid, "pid_start": self.x.pid_start_time(pid),
                "sock_path": "", "tool": "claude", "permission_mode": "bypassPermissions"}
        base.update(over)
        return base

    def test_no_pid_recorded_is_refused(self) -> None:
        ok, detail = self.x.direct_send(
            self.peer_row(pid=None), "hi", label="p", from_session="s", from_mode="bypass"
        )
        self.assertFalse(ok)
        self.assertIn("no pid", detail)

    def test_dead_pid_is_refused(self) -> None:
        # A pid nothing owns: /proc has no entry, so there is nobody to deliver to.
        dead = 4194303
        ok, detail = self.x.direct_send(
            self.peer_row(pid=dead, pid_start=1), "hi", label="p", from_session="s", from_mode="bypass"
        )
        self.assertFalse(ok)
        self.assertIn("gone", detail)

    def test_recycled_pid_is_refused(self) -> None:
        # Same pid number, different start time: the session that owned this pid
        # exited and something else took the slot. Delivering here would put the
        # message in a stranger's context.
        row = self.peer_row(pid_start=1)
        ok, detail = self.x.direct_send(row, "hi", label="p", from_session="s", from_mode="bypass")
        self.assertFalse(ok)
        self.assertIn("recycled", detail)

    def test_live_pid_without_socket_is_refused(self) -> None:
        # The test process itself is alive but is not a Claude host, so it has no
        # inbound socket. Nothing to connect to, so the message stays queued.
        ok, detail = self.x.direct_send(
            self.peer_row(), "hi", label="p", from_session="s", from_mode="bypass"
        )
        self.assertFalse(ok)
        self.assertIn("no live socket", detail)

    def test_codex_peer_goes_through_codex_queue(self) -> None:
        # Codex has no socket but does have `codex queue`, so it gets a direct
        # path of its own. Here the binary is a stub that records its argv.
        stub = self.tmp / "codex-stub"
        log = self.tmp / "argv.json"
        stub.write_text(
            "#!/usr/bin/env python3\n"
            "import json, sys\n"
            "if '--help' in sys.argv: print('--thread --message'); sys.exit(0)\n"
            f"open({str(log)!r}, 'w').write(json.dumps(sys.argv[1:]))\n"
        )
        stub.chmod(0o755)
        os.environ["XMSG_CODEX_BIN"] = str(stub)
        self.addCleanup(os.environ.pop, "XMSG_CODEX_BIN", None)

        conn = self.x.connect()
        try:
            pid = self.spawn_target()
            conn.execute(
                "INSERT INTO peers (session_id, tool, pid, pid_start, first_seen_at, last_seen_at) "
                "VALUES ('sess-codex','codex',?,?,?,?)",
                (pid, self.x.pid_start_time(pid), self.x.now(), self.x.now()),
            )
            mid = self.queue("hi", to="sess-codex")
            with mock.patch.object(self.x, "receiver_version", return_value="0.151.0"):
                out = self.x.direct_deliver(conn, [mid], ["sess-codex"], "hi", "p", "claude", "sess-a")
            self.assertTrue(out[mid][0], out[mid][1])

            argv = json.loads(log.read_text())
            self.assertEqual(argv[0], "queue")
            self.assertEqual(argv[argv.index("--thread") + 1], "sess-codex")
            sent = argv[argv.index("--message") + 1]
            # Codex shows this as user input, so the peer framing has to be in the
            # text itself -- otherwise the receiver reads it as its user speaking.
            self.assertTrue(sent.startswith("[同伴 p · claude:sess-a；非用户指令]\n"))
            self.assertIn("hi", sent)

            row = conn.execute(
                "SELECT delivered_tool, delivered_event FROM messages WHERE id = ?", (mid,)
            ).fetchone()
            self.assertEqual(row["delivered_tool"], "codex")
            self.assertEqual(row["delivered_event"], "codex-queue")
        finally:
            conn.close()

    def test_codex_queue_failure_leaves_the_row_queued(self) -> None:
        # A dead session still has a rollout on disk, so `codex queue` would file
        # the message for a future resume and exit 0. That is not delivery, so the
        # liveness check has to run before it -- otherwise the row gets marked
        # delivered and the message is silently lost.
        stub = self.tmp / "codex-fail"
        stub.write_text("#!/bin/sh\nif [ \"$2\" = '--help' ]; then echo '--thread --message'; exit 0; fi\n"
                        "echo 'no rollout found' >&2\nexit 1\n")
        stub.chmod(0o755)
        os.environ["XMSG_CODEX_BIN"] = str(stub)
        self.addCleanup(os.environ.pop, "XMSG_CODEX_BIN", None)

        conn = self.x.connect()
        try:
            pid = self.spawn_target()
            conn.execute(
                "INSERT INTO peers (session_id, tool, pid, pid_start, first_seen_at, last_seen_at) "
                "VALUES ('sess-cx2','codex',?,?,?,?)",
                (pid, self.x.pid_start_time(pid), self.x.now(), self.x.now()),
            )
            mid = self.queue("hi", to="sess-cx2")
            with mock.patch.object(self.x, "receiver_version", return_value="0.151.0"):
                out = self.x.direct_deliver(conn, [mid], ["sess-cx2"], "hi", "p", "claude", "sess-a")
            self.assertFalse(out[mid][0])
            self.assertIn("no rollout", out[mid][1])
            row = conn.execute("SELECT delivered_at FROM messages WHERE id = ?", (mid,)).fetchone()
            self.assertIsNone(row["delivered_at"])
        finally:
            conn.close()

    def test_dead_codex_session_is_refused_before_codex_runs(self) -> None:
        # The guard has to fire without invoking codex at all: a stub that would
        # succeed must never be reached for a session whose process is gone.
        stub = self.tmp / "codex-never"
        marker = self.tmp / "ran"
        stub.write_text(f"#!/bin/sh\ntouch {marker}\nexit 0\n")
        stub.chmod(0o755)
        os.environ["XMSG_CODEX_BIN"] = str(stub)
        self.addCleanup(os.environ.pop, "XMSG_CODEX_BIN", None)

        ok, detail = self.x.direct_send_codex(
            {"pid": 4194303, "pid_start": 1}, "hi", to_session="sess-gone"
        )
        self.assertFalse(ok)
        self.assertIn("gone", detail)
        self.assertFalse(marker.exists(), "codex must not be invoked for a dead session")

    def test_unknown_peer_is_skipped_without_a_verdict(self) -> None:
        conn = self.x.connect()
        try:
            mid = self.queue("hi", to="sess-nowhere")
            out = self.x.direct_deliver(conn, [mid], ["sess-nowhere"], "hi", "p", "claude", "sess-a")
            self.assertNotIn(mid, out)
        finally:
            conn.close()


class TestDirectDeliveryOverSocket(DirectBase):
    """Direct delivery against a stand-in listener on a real unix socket.

    A fake host cannot prove Claude accepts the bytes -- only the end-to-end run
    against a live session does that -- but it does pin the wire shape, and it
    pins that a successful write claims the row so the hook cannot deliver it a
    second time.
    """

    def test_wire_shape_is_one_json_line(self) -> None:
        pid = self.spawn_target()
        th, got = self.listener(self.sockdir / f"{pid}.sock")
        row = {"pid": pid, "pid_start": self.x.pid_start_time(pid), "sock_path": "",
               "tool": "claude", "permission_mode": "bypassPermissions"}
        ok, detail = self.x.direct_send(
            row, "通报：探针 ZZ-1", label="peer-a", from_session="sess-a", from_mode="bypass"
        )
        th.join(timeout=5)
        self.assertTrue(ok, detail)
        self.assertEqual(len(got), 1)
        raw = got[0]
        self.assertTrue(raw.endswith(b"\n"), "the host reads by line; the write must terminate one")
        msg = json.loads(raw.decode())
        self.assertEqual(msg["type"], "user")
        self.assertEqual(msg["message"]["role"], "user")
        self.assertIn('from-mode="bypass"', msg["message"]["content"])
        self.assertIn("通报：探针 ZZ-1", msg["message"]["content"])

    def test_success_claims_the_row_so_the_hook_cannot_resend(self) -> None:
        pid = self.spawn_target()
        th, _ = self.listener(self.sockdir / f"{pid}.sock")
        conn = self.x.connect()
        try:
            conn.execute(
                "INSERT INTO peers (session_id, tool, pid, pid_start, permission_mode, "
                "first_seen_at, last_seen_at) VALUES (?,?,?,?,?,?,?)",
                ("sess-live", "claude", pid, self.x.pid_start_time(pid), "bypassPermissions",
                 self.x.now(), self.x.now()),
            )
            mid = self.queue("hi there", to="sess-live")
            out = self.x.direct_deliver(
                conn, [mid], ["sess-live"], "hi there", "peer-a", "claude", "sess-a"
            )
            th.join(timeout=5)
            self.assertTrue(out[mid][0], out[mid][1])
            row = conn.execute(
                "SELECT delivered_at, delivered_event FROM messages WHERE id = ?", (mid,)
            ).fetchone()
            self.assertIsNotNone(row["delivered_at"])
            self.assertEqual(row["delivered_event"], "uds-direct")
        finally:
            conn.close()
        # And the hook, running afterwards for that session, finds nothing left.
        payload = dict(PAYLOAD, session_id="sess-live")
        self.assertEqual(self.hook(payload, tool="claude"), "")

    def test_refusing_to_deliver_to_the_sending_process(self) -> None:
        # A session must not be handed its own message: it would read its own
        # words back as a peer's. Guarded on pid because that is what a loop-back
        # actually looks like from here.
        row = {"pid": os.getpid(), "pid_start": self.x.pid_start_time(os.getpid()),
               "sock_path": "", "tool": "claude", "permission_mode": ""}
        self.x.SOCK_DIRS = [self.sockdir]
        (self.sockdir / f"{os.getpid()}.sock").touch()
        ok, detail = self.x.direct_send(row, "hi", label="p", from_session="s", from_mode="bypass")
        self.assertFalse(ok)
        self.assertIn("sending process", detail)


class TestPeerDirectColumns(Base):
    """What the receiver's own hook records so it can be reached directly."""

    def test_hook_records_permission_mode(self) -> None:
        self.hook(tool="claude")
        conn = self.x.connect()
        try:
            row = conn.execute(
                "SELECT permission_mode FROM peers WHERE session_id = ?", (self.sid,)
            ).fetchone()
            self.assertEqual(row["permission_mode"], "bypassPermissions")
        finally:
            conn.close()

    def test_missing_mode_does_not_erase_a_known_one(self) -> None:
        self.hook(tool="claude")
        self.hook(dict(PAYLOAD, permission_mode=""), tool="claude")
        conn = self.x.connect()
        try:
            row = conn.execute(
                "SELECT permission_mode FROM peers WHERE session_id = ?", (self.sid,)
            ).fetchone()
            self.assertEqual(row["permission_mode"], "bypassPermissions")
        finally:
            conn.close()

    def test_reachable_now_needs_a_live_host(self) -> None:
        # No pid recorded, a dead pid, and an unknown tool are all unreachable.
        self.assertFalse(self.x.reachable_now({"pid": None, "tool": "claude", "pid_start": None}))
        self.assertFalse(self.x.reachable_now({"pid": 4194303, "tool": "claude", "pid_start": 1}))
        self.assertFalse(
            self.x.reachable_now({"pid": os.getpid(), "tool": "cursor", "pid_start": None})
        )

    def test_reachable_now_for_codex_needs_only_a_live_process(self) -> None:
        # Codex is addressed by session id rather than by socket, so a live process
        # plus a codex binary is all that can be checked from here.
        os.environ["XMSG_CODEX_BIN"] = "/bin/sh"
        self.addCleanup(os.environ.pop, "XMSG_CODEX_BIN", None)
        self.assertTrue(
            self.x.reachable_now({"pid": os.getpid(), "tool": "codex", "pid_start": None})
        )
        self.assertFalse(self.x.reachable_now({"pid": 4194303, "tool": "codex", "pid_start": 1}))

    def test_migration_adds_columns_to_an_older_database(self) -> None:
        # A database created before direct delivery existed must gain the columns
        # rather than fail: CREATE TABLE IF NOT EXISTS never adds one.
        old = self.tmp / "old.sqlite3"
        conn = sqlite3.connect(old)
        conn.executescript(
            "CREATE TABLE peers (session_id TEXT PRIMARY KEY, tool TEXT NOT NULL, "
            "cwd TEXT NOT NULL DEFAULT '', model TEXT NOT NULL DEFAULT '', "
            "label TEXT NOT NULL DEFAULT '', first_seen_at INTEGER NOT NULL, "
            "last_seen_at INTEGER NOT NULL, injected_count INTEGER NOT NULL DEFAULT 0)"
        )
        conn.execute(
            "INSERT INTO peers (session_id, tool, first_seen_at, last_seen_at) VALUES ('s','claude',1,1)"
        )
        conn.commit()
        conn.close()

        x2 = load_impl(old)
        c2 = x2.connect()
        try:
            cols = {r["name"] for r in c2.execute("PRAGMA table_info(peers)")}
            self.assertTrue({"pid", "pid_start", "sock_path", "permission_mode"} <= cols)
            row = c2.execute("SELECT session_id, pid FROM peers").fetchone()
            self.assertEqual(row["session_id"], "s", "existing rows must survive the migration")
            self.assertIsNone(row["pid"])
        finally:
            c2.close()


class TestFailOpen(unittest.TestCase):
    """The hook must never break the session it runs inside.

    These drive xmsg-hook.sh as a subprocess, because that wrapper is what the
    host actually invokes and what bounds the failures Python cannot catch.
    """

    def setUp(self) -> None:
        self.tmp = Path(tempfile.mkdtemp(prefix="xmsg-failopen-"))

    def tearDown(self) -> None:
        shutil.rmtree(self.tmp, ignore_errors=True)

    def run_hook(self, *, env: dict[str, str], stdin: str, timeout: int = 30) -> subprocess.CompletedProcess:
        e = dict(os.environ)
        e.pop("XMSG_NO_FAILOPEN", None)
        e["XMSG_DB"] = str(self.tmp / "messages.sqlite3")
        # Pin the implementation to the copy that sits next to this test file.
        # Without this the wrapper falls back to $HOME/agent-msg/xmsg.py, so a
        # mutated checkout would silently exercise the pristine installed copy
        # and every fail-open test would pass no matter what was broken.
        e["XMSG_IMPL"] = str(IMPL)
        e.update(env)
        return subprocess.run(
            [str(HOOK_SH), "codex"],
            input=stdin,
            capture_output=True,
            text=True,
            env=e,
            timeout=timeout,
        )

    def test_healthy_hook_succeeds_quietly(self) -> None:
        r = self.run_hook(env={}, stdin=json.dumps(PAYLOAD))
        self.assertEqual(r.returncode, 0)
        self.assertEqual(r.stdout, "")

    def test_missing_implementation_does_not_fail_the_session(self) -> None:
        r = self.run_hook(env={"XMSG_IMPL": "/nonexistent/xmsg.py"}, stdin=json.dumps(PAYLOAD))
        self.assertEqual(r.returncode, 0)
        self.assertEqual(r.stdout, "")

    def test_corrupt_database_does_not_fail_the_session(self) -> None:
        bad = self.tmp / "corrupt.sqlite3"
        bad.write_text("this is not a database")
        r = self.run_hook(env={"XMSG_DB": str(bad)}, stdin=json.dumps(PAYLOAD))
        self.assertEqual(r.returncode, 0)
        self.assertEqual(r.stdout, "")

    def test_unwritable_database_directory_does_not_fail_the_session(self) -> None:
        ro = self.tmp / "ro"
        ro.mkdir()
        ro.chmod(0o500)
        try:
            r = self.run_hook(env={"XMSG_DB": str(ro / "sub" / "m.sqlite3")}, stdin=json.dumps(PAYLOAD))
        finally:
            ro.chmod(0o700)
        self.assertEqual(r.returncode, 0)
        self.assertEqual(r.stdout, "")

    def test_malformed_payload_does_not_fail_the_session(self) -> None:
        r = self.run_hook(env={}, stdin="not json {{{")
        self.assertEqual(r.returncode, 0)
        self.assertEqual(r.stdout, "")

    def test_empty_payload_does_not_fail_the_session(self) -> None:
        r = self.run_hook(env={}, stdin="")
        self.assertEqual(r.returncode, 0)
        self.assertEqual(r.stdout, "")

    def test_payload_without_session_id_is_ignored(self) -> None:
        r = self.run_hook(env={}, stdin=json.dumps({"hook_event_name": "PreToolUse", "cwd": "/tmp"}))
        self.assertEqual(r.returncode, 0)
        self.assertEqual(r.stdout, "")

    def test_a_hanging_implementation_is_cut_off_well_inside_the_host_budget(self) -> None:
        hang = self.tmp / "hang.py"
        hang.write_text("import time\ntime.sleep(60)\n")
        t0 = time.monotonic()
        r = self.run_hook(
            env={"XMSG_IMPL": str(hang), "XMSG_HOOK_TIMEOUT": "2"},
            stdin=json.dumps(PAYLOAD),
            timeout=20,
        )
        elapsed = time.monotonic() - t0
        self.assertEqual(r.returncode, 0)
        self.assertEqual(r.stdout, "")
        # The host allows 5s; the wrapper must come back comfortably under it.
        self.assertLess(elapsed, 4.5, f"hook took {elapsed:.1f}s, host budget is 5s")

    def test_exit_code_is_never_2_because_that_is_what_blocks_the_tool(self) -> None:
        """Exit 2 is the one code that actually kills the tool call.

        Measured against a real Claude Code session with a secret only a real
        Bash call could read: hook exit 0 -> tool runs, exit 1 -> tool still
        runs (stderr is logged), exit 2 -> tool is blocked. So the load-bearing
        promise is not "exit 0" in the abstract, it is "never 2" - and the worst
        failure mode, a missing implementation file, is exactly what python
        exits 2 for. This asserts the one number that matters, across every
        failure mode, rather than trusting the blanket `exit 0`.
        """
        bad = self.tmp / "corrupt.sqlite3"
        bad.write_text("this is not a database")
        hang = self.tmp / "hang.py"
        hang.write_text("import time\ntime.sleep(60)\n")
        cases = {
            "missing implementation": {"XMSG_IMPL": "/nonexistent/xmsg.py"},
            "corrupt database": {"XMSG_DB": str(bad)},
            "hanging implementation": {"XMSG_IMPL": str(hang), "XMSG_HOOK_TIMEOUT": "1"},
        }
        for label, env in cases.items():
            with self.subTest(failure=label):
                r = self.run_hook(env=env, stdin=json.dumps(PAYLOAD), timeout=20)
                self.assertNotEqual(r.returncode, 2, f"{label} would block the tool call")
                self.assertEqual(r.returncode, 0)


class TestCli(unittest.TestCase):
    """End-to-end through the wrapper in ~/bin, the way an operator drives it."""

    def setUp(self) -> None:
        self.tmp = Path(tempfile.mkdtemp(prefix="xmsg-cli-"))
        self.env = dict(os.environ)
        self.env.pop("XMSG_NO_FAILOPEN", None)
        for key in (
            "XMSG_FROM",
            "XMSG_FROM_TOOL",
            "XMSG_FROM_SESSION",
            "CLAUDE_CODE_SESSION_ID",
            "CLAUDECODE",
            "XMSG_REMOTE",
            "XMSG_REMOTE_UP",
        ):
            self.env.pop(key, None)
        self.env["XMSG_DB"] = str(self.tmp / "messages.sqlite3")

    def tearDown(self) -> None:
        shutil.rmtree(self.tmp, ignore_errors=True)

    def cli(self, *args: str, stdin: str = "") -> subprocess.CompletedProcess:
        return subprocess.run(
            [sys.executable, str(IMPL), *args], input=stdin, capture_output=True, text=True, env=self.env
        )

    def hook_once(self, session_id: str) -> subprocess.CompletedProcess:
        return subprocess.run(
            [sys.executable, str(IMPL), "hook", "--tool", "codex"],
            input=json.dumps(dict(PAYLOAD, session_id=session_id)),
            capture_output=True,
            text=True,
            env=self.env,
        )

    def agy_hook_once(
        self, event: str, conversation_id: str, workspace: str | None = None
    ) -> subprocess.CompletedProcess:
        """agy payloads carry no event name, so the CLI takes it as --event,
        exactly the way xmsg-hook.sh forwards its second argument."""
        payload = dict(AGY_PAYLOAD, conversationId=conversation_id)
        if workspace is not None:
            payload["workspacePaths"] = [workspace]
        return subprocess.run(
            [sys.executable, str(IMPL), "hook", "--tool", "agy", "--event", event],
            input=json.dumps(payload),
            capture_output=True,
            text=True,
            env=self.env,
        )

    def agy_live_env(self, conversation_id: str) -> None:
        """Point the CLI at a fake presence/proc pair reporting: this agy
        conversation is attached to a running process that holds its lock open,
        so sends must stay on the hook-only path instead of waking a second one."""
        presence = self.tmp / f"presence-{conversation_id}"
        presence.mkdir(parents=True, exist_ok=True)
        lock = presence / f"{conversation_id}.lock"
        lock.write_text("")
        proc = self.tmp / f"proc-{conversation_id}"
        fd = proc / "999999" / "fd"
        fd.mkdir(parents=True, exist_ok=True)
        os.symlink(str(lock), str(fd / "7"))
        self.env["XMSG_AGY_PRESENCE_DIR"] = str(presence)
        self.env["XMSG_PROC_ROOT"] = str(proc)

    def test_send_then_hook_delivers_and_outbox_confirms(self) -> None:
        self.hook_once("sess-cli-1")
        send = self.cli("send", "sess-cli-1", "NONCE-CLI-9001 hi")
        self.assertEqual(send.returncode, 0, send.stderr)
        self.assertIn("queued #1", send.stdout)

        hooked = self.hook_once("sess-cli-1")
        self.assertIn("NONCE-CLI-9001", hooked.stdout)

        outbox = self.cli("outbox", "--all")
        self.assertIn("delivered", outbox.stdout)

    def test_empty_message_is_refused(self) -> None:
        self.hook_once("sess-cli-2")
        r = self.cli("send", "sess-cli-2", "   ")
        self.assertNotEqual(r.returncode, 0)
        self.assertIn("empty", r.stderr)

    def test_oversized_message_is_refused(self) -> None:
        self.hook_once("sess-cli-3")
        r = self.cli("send", "sess-cli-3", "x" * 20001)
        self.assertNotEqual(r.returncode, 0)
        self.assertIn("limit", r.stderr)

    def test_cancel_withdraws_an_undelivered_message(self) -> None:
        self.hook_once("sess-cli-4")
        self.cli("send", "sess-cli-4", "will be cancelled")
        self.assertIn("cancelled #1", self.cli("cancel", "1").stdout)
        self.assertEqual(self.hook_once("sess-cli-4").stdout, "")

    def test_cancel_after_delivery_is_reported_as_not_cancellable(self) -> None:
        self.hook_once("sess-cli-5")
        self.cli("send", "sess-cli-5", "already gone")
        self.hook_once("sess-cli-5")
        self.assertIn("not cancellable", self.cli("cancel", "1").stdout)

    def test_list_shows_a_registered_session_with_its_queue_depth(self) -> None:
        self.hook_once("sess-cli-6")
        self.cli("send", "sess-cli-6", "pending")
        rows = json.loads(self.cli("list", "--json").stdout)
        row = next(r for r in rows if r["session_id"] == "sess-cli-6")
        self.assertEqual(row["queued"], 1)
        self.assertEqual(row["tool"], "codex")

    def test_send_to_agy_session_waits_for_its_hook(self) -> None:
        """A live agy has no direct channel: send must queue (not fail, not fake
        a delivery, not wake a second process over the attached one) and the
        next PreInvocation hook injects it."""
        self.agy_hook_once("PreInvocation", "sess-agy-1")
        self.agy_live_env("sess-agy-1")
        send = self.cli("send", "sess-agy-1", "NONCE-CLI-AGY-1 hi")
        self.assertEqual(send.returncode, 0, send.stderr)
        self.assertIn("queued #1", send.stdout)
        self.assertIn("hook-only", send.stdout)

        hooked = self.agy_hook_once("PreInvocation", "sess-agy-1")
        self.assertIn("NONCE-CLI-AGY-1", hooked.stdout)
        self.assertIn("injectSteps", hooked.stdout)

    def test_send_to_dead_agy_conversation_dispatches_a_wake(self) -> None:
        """Real subprocesses end to end: a registered agy conversation nobody
        holds the lock of will never run a hook again, so send resumes it
        headlessly. The body does not ride along -- the row stays queued for
        the woken process's own PreInvocation hook to claim."""
        presence = self.tmp / "presence-dead"
        presence.mkdir()
        (presence / "sess-agy-wake.lock").write_text("")  # stale, no holder
        proc = self.tmp / "proc-empty"
        proc.mkdir()
        wake_args = self.tmp / "wake-args.txt"
        fake_agy = self.tmp / "bin" / "agy"
        fake_agy.parent.mkdir()
        fake_agy.write_text(f'#!/bin/sh\nprintf "%s\\n" "$@" > {wake_args}\n')
        fake_agy.chmod(0o755)
        self.env.update(
            {
                "XMSG_AGY_PRESENCE_DIR": str(presence),
                "XMSG_PROC_ROOT": str(proc),
                "XMSG_AGY_BIN": str(fake_agy),
            }
        )
        self.agy_hook_once("PreInvocation", "sess-agy-wake", workspace=str(self.tmp))
        send = self.cli("send", "sess-agy-wake", "NONCE-CLI-AGY-WAKE")
        self.assertEqual(send.returncode, 0, send.stderr)
        self.assertIn("queued #1", send.stdout)
        self.assertIn("wake dispatched", send.stdout)

        for _ in range(50):  # the wake is detached; wait for it to record its args
            if wake_args.exists():
                break
            time.sleep(0.1)
        args = wake_args.read_text().splitlines()
        self.assertEqual(args[args.index("--conversation") + 1], "sess-agy-wake")
        self.assertIn("-p", args)
        self.assertRegex(args[args.index("--print-timeout") + 1], r"^\d+[smh]$")

        outbox = self.cli("outbox")
        self.assertIn("NONCE-CLI-AGY-WAKE", outbox.stdout)
        self.assertIn("queued", outbox.stdout)

        # ...and the woken process's hook injects the row, closing the loop.
        hooked = self.agy_hook_once(
            "PreInvocation", "sess-agy-wake", workspace=str(self.tmp)
        )
        self.assertIn("NONCE-CLI-AGY-WAKE", hooked.stdout)
        self.assertIn("injectSteps", hooked.stdout)

    def test_agy_stop_delivers_continue_decision(self) -> None:
        self.agy_hook_once("PreInvocation", "sess-agy-2")
        self.cli("send", "sess-agy-2", "NONCE-CLI-AGY-2")
        out = json.loads(self.agy_hook_once("Stop", "sess-agy-2").stdout)
        self.assertEqual(out["decision"], "continue")
        self.assertIn("NONCE-CLI-AGY-2", out["reason"])

    def test_hook_sh_forwards_the_event_argument(self) -> None:
        """The real wrapper: agy's hook config relies on argv[2] reaching
        --event; a dropped argument would silently fall back to the
        PreToolUse-shaped output agy cannot read."""
        self.agy_hook_once("PreInvocation", "sess-agy-3")
        self.cli("send", "sess-agy-3", "NONCE-CLI-AGY-3")
        proc = subprocess.run(
            [str(HOOK_SH), "agy", "Stop"],
            input=json.dumps(dict(AGY_PAYLOAD, conversationId="sess-agy-3")),
            capture_output=True,
            text=True,
            env=self.env,
        )
        self.assertEqual(proc.returncode, 0, proc.stderr)
        out = json.loads(proc.stdout)
        self.assertEqual(out["decision"], "continue")
        self.assertIn("NONCE-CLI-AGY-3", out["reason"])

    def test_list_shows_the_agy_session(self) -> None:
        self.agy_hook_once("PreInvocation", "sess-agy-4")
        rows = json.loads(self.cli("list", "--json").stdout)
        row = next(r for r in rows if r["session_id"] == "sess-agy-4")
        self.assertEqual(row["tool"], "agy")
        self.assertFalse(row["direct"])
        self.assertEqual(row["cwd"], AGY_PAYLOAD["workspacePaths"][0])

    def test_send_reads_the_body_from_stdin(self) -> None:
        self.hook_once("sess-cli-7")
        r = self.cli("send", "sess-cli-7", "-", stdin="NONCE-CLI-STDIN from a pipe")
        self.assertEqual(r.returncode, 0, r.stderr)
        self.assertIn("NONCE-CLI-STDIN", self.hook_once("sess-cli-7").stdout)

    def test_find_session_script_prints_complete_id_for_custom_name(self) -> None:
        projects = self.tmp / "claude-projects"
        session_id = "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa"
        title = projects / "project" / session_id / "custom-title.json"
        title.parent.mkdir(parents=True)
        title.write_text('{"customTitle":"named-target"}')
        env = dict(self.env, XMSG_CLAUDE_PROJECTS=str(projects))
        found = subprocess.run(
            [str(FIND_SH), "named-target"], capture_output=True, text=True, env=env
        )
        self.assertEqual(found.returncode, 0, found.stderr)
        self.assertEqual(found.stdout.strip(), session_id)

        detailed = subprocess.run(
            [str(FIND_SH), "named-target", "--json"], capture_output=True, text=True, env=env
        )
        self.assertEqual(detailed.returncode, 0, detailed.stderr)
        self.assertEqual(json.loads(detailed.stdout)[0]["name"], "named-target")

    def test_find_session_script_refuses_ambiguous_custom_name(self) -> None:
        projects = self.tmp / "claude-projects"
        for session_id in (
            "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa",
            "bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb",
        ):
            title = projects / "project" / session_id / "custom-title.json"
            title.parent.mkdir(parents=True)
            title.write_text('{"customTitle":"same-name"}')
        env = dict(self.env, XMSG_CLAUDE_PROJECTS=str(projects))
        found = subprocess.run(
            [str(FIND_SH), "same-name"], capture_output=True, text=True, env=env
        )
        self.assertNotEqual(found.returncode, 0)
        self.assertIn("matches 2 sessions", found.stderr)



class TestLiveClaudeIdle(DirectBase):
    """Idle Claude hosts that have never run the xmsg hook are still reachable."""

    def spawn_child(self, sid: str, sock: Path) -> int:
        proc = subprocess.Popen(
            [sys.executable, "-c", "import time; time.sleep(30)"],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            env={
                **os.environ,
                "CLAUDE_CODE_SESSION_ID": sid,
                "CLAUDE_CODE_MESSAGING_SOCKET": str(sock),
                "CLAUDE_PROJECT_DIR": "/tmp/live-test",
            },
        )
        self.addCleanup(proc.wait)
        self.addCleanup(proc.kill)
        for _ in range(200):
            if self.x.pid_start_time(proc.pid) is not None:
                break
            time.sleep(0.01)
        return proc.pid

    def test_live_scan_finds_a_host_with_no_peer_row(self) -> None:
        host = self.spawn_target()
        sock = self.sockdir / f"{host}.sock"
        sock.touch()
        sid = "cccccccc-cccc-4ccc-8ccc-cccccccccccc"
        self.spawn_child(sid, sock)
        found = {row["session_id"]: row for row in self.x.live_claude_sessions()}
        self.assertIn(sid, found)
        self.assertEqual(found[sid]["pid"], host)
        self.assertEqual(found[sid]["cwd"], "/tmp/live-test")

    def test_direct_deliver_uses_the_live_scan_when_peers_is_empty(self) -> None:
        host = self.spawn_target()
        sock = self.sockdir / f"{host}.sock"
        th, got = self.listener(sock)
        sid = "dddddddd-dddd-4ddd-8ddd-dddddddddddd"
        self.spawn_child(sid, sock)
        conn = self.x.connect()
        try:
            mid = self.queue("idle hello", to=sid)
            out = self.x.direct_deliver(
                conn, [mid], [sid], "idle hello", "peer-a", "claude", "sess-a"
            )
            th.join(timeout=5)
            self.assertIn(mid, out)
            self.assertTrue(out[mid][0], out[mid][1])
            row = conn.execute(
                "SELECT delivered_event FROM messages WHERE id = ?", (mid,)
            ).fetchone()
            self.assertEqual(row["delivered_event"], "uds-direct")
        finally:
            conn.close()
        self.assertTrue(got, "idle host must receive the json line")


class TestRemotePeerPrefix(unittest.TestCase):
    """`peer:` is a transport prefix, not a session id. SSH is operator-supplied."""

    def setUp(self) -> None:
        self.tmp = Path(tempfile.mkdtemp(prefix="xmsg-remote-"))
        self.env = dict(os.environ)
        self.env.pop("XMSG_NO_FAILOPEN", None)
        for key in (
            "XMSG_FROM",
            "XMSG_FROM_TOOL",
            "XMSG_FROM_SESSION",
            "CLAUDE_CODE_SESSION_ID",
            "CLAUDECODE",
            "XMSG_REMOTE",
            "XMSG_REMOTE_UP",
        ):
            self.env.pop(key, None)
        self.env["XMSG_DB"] = str(self.tmp / "messages.sqlite3")

    def tearDown(self) -> None:
        shutil.rmtree(self.tmp, ignore_errors=True)

    def test_split_peer_target(self) -> None:
        x = load_impl(self.tmp / "db.sqlite3")
        self.assertEqual(x.split_peer_target("peer:leaderpc"), "leaderpc")
        self.assertEqual(x.split_peer_target("peer:"), "all")
        self.assertEqual(x.split_peer_target("peer"), "all")
        self.assertEqual(x.split_peer_target("peer:all"), "all")
        self.assertIsNone(x.split_peer_target("leaderpc"))
        self.assertIsNone(x.split_peer_target("all"))

    def test_send_runs_one_quoted_command_through_XMSG_REMOTE(self) -> None:
        stub = self.tmp / "remote"
        log = self.tmp / "log.txt"
        stub.write_text(
            "#!/usr/bin/env python3\n"
            "import sys, pathlib\n"
            f"pathlib.Path({str(log)!r}).write_text("
            "repr(sys.argv[1:]) + chr(10) + sys.stdin.read())\n"
        )
        stub.chmod(0o755)
        env = dict(
            self.env,
            XMSG_REMOTE=str(stub),
            XMSG_REMOTE_UP="/bin/true",
            XMSG_FROM_TOOL="claude",
            XMSG_FROM_SESSION="aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa",
        )
        r = subprocess.run(
            [sys.executable, str(IMPL), "send", "peer:leaderpc", "hello from here"],
            capture_output=True,
            text=True,
            env=env,
        )
        self.assertEqual(r.returncode, 0, r.stderr + r.stdout)
        dumped = log.read_text()
        # One argv token: a shell command, not a list of unquoted words.
        self.assertIn("xmsg", dumped)
        self.assertIn("send", dumped)
        self.assertIn("leaderpc", dumped)
        self.assertIn("hello from here", dumped)
        self.assertIn("XMSG_FROM_SESSION=aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa", dumped)

    def test_missing_remote_is_a_clear_error(self) -> None:
        env = dict(self.env, PATH="/usr/bin:/bin", XMSG_REMOTE="")
        env.pop("XMSG_REMOTE", None)
        # PATH without ~/bin so shutil.which("peer") cannot salvage it.
        r = subprocess.run(
            [sys.executable, str(IMPL), "send", "peer:leaderpc", "nope"],
            capture_output=True,
            text=True,
            env=env,
        )
        self.assertNotEqual(r.returncode, 0)
        self.assertIn("XMSG_REMOTE", r.stderr)


if __name__ == "__main__":
    unittest.main(verbosity=2)
