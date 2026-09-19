"""History selection uses scratch stores and fake launchers; no provider calls."""

from __future__ import annotations

import contextlib
import base64
import hashlib
import io
import json
import os
import socket
import sqlite3
import struct
import subprocess
import sys
import tempfile
import threading
import time
import unittest
from pathlib import Path
from unittest.mock import Mock, patch

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))
import codex_history as history

SID_A = "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa"
SID_B = "bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb"
SID_C = "cccccccc-cccc-4ccc-8ccc-cccccccccccc"


class HistoryTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory(prefix="codex-history-test-")
        self.addCleanup(self.tmp.cleanup)
        self.home = Path(self.tmp.name)
        self.db = self.home / "state_5.sqlite"
        self.workspace = self.home / "项目 with spaces"
        self.workspace.mkdir()
        self.config = self.home / "config.toml"
        self.config.write_text('[model_providers.original]\nname="Original"\n[model_providers.other]\nname="Other"\n')
        with sqlite3.connect(self.db) as con:
            con.execute('CREATE TABLE threads (id TEXT, name TEXT, title TEXT, model_provider TEXT, model TEXT, '
                        'cwd TEXT, updated_at INTEGER, source TEXT, archived INTEGER, rollout_path TEXT)')
        self.row = self.add_row(SID_A)

    def add_row(self, sid, **changes):
        row = {"id": sid, "name": "测试任务", "title": "原始标题", "model_provider": "original", "model": "model-a",
               "cwd": str(self.workspace), "updated_at": 10, "source": "cli", "archived": 0, "rollout_path": "/unused"}
        row.update(changes)
        with sqlite3.connect(self.db) as con:
            con.execute('INSERT INTO threads VALUES (?,?,?,?,?,?,?,?,?,?)', [row[k] for k in history.FIELDS])
        return row

    def load(self, **kwargs):
        return history.load_history(self.home, "sqlite", "unused", **kwargs)[0]

    def args(self, *extra):
        return history.parser().parse_args(["resume", SID_A, "--home", str(self.home), "--backend", "sqlite", *extra])

    def main(self, *extra):
        out, err = io.StringIO(), io.StringIO()
        with contextlib.redirect_stdout(out), contextlib.redirect_stderr(err):
            rc = history.main(list(extra))
        return rc, out.getvalue(), err.getvalue()

    def test_all_providers_same_list_and_sorted(self):
        self.add_row(SID_B, model_provider="other", updated_at=20)
        self.assertEqual([r["id"] for r in self.load()], [SID_B, SID_A])

    def test_read_only_preserves_database_bytes_and_mtime(self):
        before, mtime = self.db.read_bytes(), self.db.stat().st_mtime_ns
        self.load()
        self.assertEqual(before, self.db.read_bytes())
        self.assertEqual(mtime, self.db.stat().st_mtime_ns)

    def test_excludes_archived_and_non_interactive_by_default(self):
        self.add_row(SID_B, archived=1)
        self.add_row(SID_C, source='{"subagent":{"thread_spawn":{}}}')
        self.assertEqual(len(self.load()), 1)
        self.assertEqual(len(self.load(include_archived=True)), 2)
        self.assertEqual(len(self.load(include_archived=True, include_non_interactive=True)), 3)

    def test_casefold_search_includes_custom_name_model_provider_cwd_and_id(self):
        for query in ("测试", "MODEL-A", "ORIGINAL", "SPACES", "AAAA"):
            self.assertEqual(len(history.filter_history(self.load(), None, query, None)), 1)
        self.assertEqual(history.filter_history(self.load(), "other", "", None), [])
        self.assertEqual(history.filter_history(self.load(), None, "", "/other"), [])

    def test_index_last_rename_fills_missing_name_but_not_database_name(self):
        self.add_row(SID_B, name=None)
        index = self.home / "session_index.jsonl"
        index.write_text(json.dumps({"id": SID_B, "thread_name": "Old"}) + '\n' +
                         json.dumps({"id": SID_B, "thread_name": "New"}) + '\n{"partial":\n' +
                         json.dumps({"id": SID_A, "thread_name": "Stale"}) + '\n')
        rows = {r["id"]: r for r in self.load()}
        self.assertEqual(rows[SID_B]["name"], "New")
        self.assertEqual(rows[SID_A]["name"], "测试任务")

    def test_newest_database_generation_not_lexicographic_or_merged(self):
        (self.home / "state_10.sqlite").write_bytes(self.db.read_bytes())
        self.add_row(SID_B)
        self.assertEqual(len(self.load()), 1)

    def test_incompatible_newer_database_fails_instead_of_reading_old(self):
        with sqlite3.connect(self.home / "state_10.sqlite") as con:
            con.execute('CREATE TABLE unrelated (id TEXT)')
        with self.assertRaisesRegex(history.HistoryError, "结构不兼容"):
            self.load()

    def test_missing_database_is_not_created(self):
        empty = self.home / "empty"
        empty.mkdir()
        with self.assertRaises(history.HistoryError):
            history.read_sqlite(empty)
        self.assertEqual(list(empty.iterdir()), [])

    def test_optional_schema_fields_can_be_missing(self):
        with sqlite3.connect(self.db) as con:
            con.execute('ALTER TABLE threads DROP COLUMN model')
            con.execute('ALTER TABLE threads DROP COLUMN name')
        self.assertIsNone(self.load()[0]["model"])
        self.assertEqual(self.load()[0]["name"], "原始标题")

    def test_exact_uuid_beats_another_threads_name(self):
        self.add_row(SID_B, name=SID_A)
        self.assertEqual(history.resolve_thread(self.load(), SID_A)["id"], SID_A)

    def test_missing_uuid_does_not_fall_back_to_a_custom_name(self):
        self.add_row(SID_B, name=SID_C)
        with self.assertRaisesRegex(history.HistoryError, "完整会话 ID"):
            history.resolve_thread(self.load(), SID_C)

    def test_custom_name_and_id_prefix(self):
        rows = self.load()
        self.assertEqual(history.resolve_thread(rows, "测试任务")["id"], SID_A)
        self.assertEqual(history.resolve_thread(rows, "aaaa")["id"], SID_A)
        with self.assertRaises(history.HistoryError):
            history.resolve_thread(rows, "aaa")

    def test_ambiguous_names_show_full_ids(self):
        self.add_row(SID_B)
        with self.assertRaises(history.HistoryError) as error:
            history.resolve_thread(self.load(), "测试任务")
        self.assertIn(SID_A, str(error.exception))
        self.assertIn(SID_B, str(error.exception))

    def test_resume_pins_provider_model_cwd_and_does_not_pass_prompt(self):
        argv = history.resume_command(self.row, self.args(), self.home)
        self.assertEqual(argv, ["codex", "resume", SID_A, "-c", 'model_provider="original"',
                                "--model", "model-a", "--cd", str(self.workspace)])

    def test_export_transcript_is_provider_neutral_and_excludes_ids(self):
        rollout = self.home / "sessions" / "rollout.jsonl"
        rollout.parent.mkdir()
        records = [
            {"type": "response_item", "payload": {"type": "message", "id": "msg_old",
             "role": "developer", "content": [{"type": "input_text", "text": "secret wiring"}]}},
            {"type": "response_item", "payload": {"type": "message", "id": "msg_user",
             "role": "user", "content": [{"type": "input_text", "text": "继续处理"}]}},
            {"type": "response_item", "payload": {"type": "function_call", "id": "at_old",
             "name": "exec", "arguments": "{\"cmd\":\"pwd\"}"}},
            {"type": "response_item", "payload": {"type": "function_call_output", "id": "fco_old",
             "output": "结果"}},
            {"type": "response_item", "payload": {"type": "reasoning", "id": "rs_old",
             "encrypted_content": "DO NOT COPY", "summary": [{"type": "summary_text", "text": "思路"}]}},
            {"type": "response_item", "payload": {"type": "message", "id": "msg_assistant",
             "role": "assistant", "content": [{"type": "output_text", "text": "已完成"}]}},
        ]
        rollout.write_text("\n".join(json.dumps(x, ensure_ascii=False) for x in records) + "\n")
        row = {**self.row, "rollout_path": str(rollout)}
        text = history.export_transcript(row, self.home)
        self.assertIn("继续处理", text)
        self.assertIn("已完成", text)
        self.assertIn("工具调用", text)
        self.assertNotIn("secret wiring", text)
        self.assertNotIn("DO NOT COPY", text)
        self.assertNotIn("at_old", text)

    def test_export_full_keeps_reasoning_summary_and_cli_writes_file(self):
        rollout = self.home / "rollout.jsonl"
        rollout.write_text(json.dumps({"type": "response_item", "payload": {
            "type": "message", "role": "user", "content": [{"type": "input_text", "text": "你好"}]}}) + "\n" +
            json.dumps({"type": "response_item", "payload": {"type": "reasoning", "summary": [
                {"type": "summary_text", "text": "思路摘要"}]}}) + "\n")
        with sqlite3.connect(self.db) as con:
            con.execute("UPDATE threads SET rollout_path=? WHERE id=?", (str(rollout), SID_A))
        output = self.home / "out.md"
        out, err = io.StringIO(), io.StringIO()
        with contextlib.redirect_stdout(out), contextlib.redirect_stderr(err):
            rc = history.main(["export", SID_A, "--home", str(self.home), "--backend", "sqlite",
                               "--full", "--output", str(output)])
        self.assertEqual(rc, 0, err)
        self.assertTrue(output.is_file())
        self.assertIn("思路摘要", output.read_text())

    def test_provider_filter_is_not_a_switch(self):
        argv = history.resume_command(self.row, self.args("--provider", "other"), self.home)
        self.assertIn('model_provider="original"', argv)

    def test_switch_requires_explicit_model(self):
        with self.assertRaisesRegex(history.HistoryError, "--model"):
            history.resume_command(self.row, self.args("--switch-provider", "other"), self.home)
        argv = history.resume_command(self.row, self.args("--switch-provider", "other", "--model", "model-b"), self.home)
        self.assertIn('model_provider="other"', argv)
        self.assertIn("model-b", argv)

    def test_unknown_provider_and_missing_model_are_not_guessed(self):
        with self.assertRaisesRegex(history.HistoryError, "provider"):
            history.resume_command({**self.row, "model_provider": "deleted"}, self.args(), self.home)
        with self.assertRaisesRegex(history.HistoryError, "--model"):
            history.resume_command({**self.row, "model": None}, self.args(), self.home)

    def test_missing_cwd_requires_explicit_override(self):
        row = {**self.row, "cwd": "/a/directory/that/is/not/here"}
        with self.assertRaisesRegex(history.HistoryError, "--cd"):
            history.resume_command(row, self.args(), self.home)
        argv = history.resume_command(row, self.args("--cd", str(self.workspace)), self.home)
        self.assertEqual(argv[-1], str(self.workspace))

    def test_archived_thread_never_automatically_unarchives(self):
        with self.assertRaisesRegex(history.HistoryError, "归档"):
            history.resume_command({**self.row, "archived": 1}, self.args(), self.home)

    def test_named_profile_providers_and_no_secret_in_argv(self):
        (self.home / "custom.config.toml").write_text('[model_providers.special]\nexperimental_bearer_token="SENSITIVE"\n')
        argv = history.resume_command({**self.row, "model_provider": "special"}, self.args("--profile", "custom"), self.home)
        self.assertIn("custom", argv)
        self.assertNotIn("SENSITIVE", repr(argv))
        with self.assertRaises(history.HistoryError):
            history.configured_providers(self.home, "../custom")

    def test_invalid_config_never_echoes_secrets_in_parser_error(self):
        self.config.write_text('experimental_bearer_token="SENSITIVE" this is invalid\n')
        with self.assertRaises(history.HistoryError) as error:
            history.configured_providers(self.home, None)
        self.assertNotIn("SENSITIVE", str(error.exception))

    def test_shell_metacharacters_are_only_an_argument(self):
        argv = history.resume_command(self.row, self.args("--model", 'model;$(touch /tmp/not-executed)'), self.home)
        self.assertIn('model;$(touch /tmp/not-executed)', argv)
        with self.assertRaises(history.HistoryError):
            history.resume_command(self.row, self.args("--model", "bad\nmodel"), self.home)

    def test_terminal_output_removes_escapes_newlines_and_bidi(self):
        text = history.row_text({**self.row, "name": "evil\x1b[31m\n\t\u202eX"})
        for control in ("\x1b", "\n", "\t", "\u202e"):
            self.assertNotIn(control, text)

    def test_auto_fallback_is_explicit_and_does_not_start_daemon(self):
        with patch.object(history, "read_api", side_effect=history.HistoryError("no socket")) as api, contextlib.redirect_stderr(io.StringIO()) as err:
            rows, source = history.load_history(self.home, "auto", "codex")
        self.assertEqual(source, "sqlite")
        self.assertEqual(len(rows), 1)
        self.assertIn("保底", err.getvalue())
        api.assert_called_once()

    def test_forced_api_failure_is_not_silently_hidden(self):
        with patch.object(history, "read_api", side_effect=history.HistoryError("no socket")), patch.object(history, "read_sqlite") as db:
            with self.assertRaises(history.HistoryError):
                history.load_history(self.home, "app-server", "codex")
            db.assert_not_called()

    def test_api_model_enriched_by_id_without_adding_sqlite_threads(self):
        self.add_row(SID_B)
        api_row = {**self.row, "model": None, "status": "notLoaded"}
        with patch.object(history, "read_api", return_value=[api_row]):
            rows, source = history.load_history(self.home, "app-server", "codex")
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["model"], "model-a")
        self.assertEqual(source, "app-server")

    def test_api_provider_change_never_inherits_model_from_other_provider(self):
        with patch.object(history, "read_api", return_value=[{**self.row, "model_provider": "other", "model": None}]):
            rows, _ = history.load_history(self.home, "app-server", "codex")
        self.assertIsNone(rows[0]["model"])

    def test_api_pagination_includes_both_archive_passes_and_all_providers(self):
        client = Mock()
        client.call.side_effect = [{},
                                   {"data": [{"id": SID_A, "modelProvider": "original"}], "nextCursor": "next"},
                                   {"data": [{"id": SID_B, "modelProvider": "other"}], "nextCursor": None},
                                   {"data": [{"id": SID_C}], "nextCursor": None}]
        with patch.object(history, "RpcClient", return_value=client):
            rows = history.read_api(self.home, "codex", True, True)
        self.assertEqual([r["id"] for r in rows], [SID_A, SID_B, SID_C])
        calls = client.call.call_args_list[1:]
        self.assertEqual([c.args[1]["archived"] for c in calls], [False, False, True])
        self.assertEqual([c.args[1]["cursor"] for c in calls], [None, "next", None])
        for call in calls:
            self.assertEqual(call.args[0], "thread/list")
            self.assertEqual(call.args[1]["modelProviders"], [])
            self.assertTrue(call.args[1]["useStateDbOnly"])
        client.close.assert_called_once()

    def test_api_repeated_cursor_fails_and_closes_proxy(self):
        client = Mock()
        client.call.side_effect = [{}, {"data": [], "nextCursor": "same"}, {"data": [], "nextCursor": "same"}]
        with patch.object(history, "RpcClient", return_value=client), self.assertRaisesRegex(history.HistoryError, "游标"):
            history.read_api(self.home, "codex", False, False)
        client.close.assert_called_once()

    def test_current_session_guard(self):
        with patch.dict(os.environ, {"CODEX_THREAD_ID": SID_A}):
            self.assertIsNotNone(history.live_evidence(self.row, self.home / "no-peers.sqlite"))

    def test_pid_reuse_is_not_reported_as_live(self):
        path = self.home / "peers.sqlite"
        with sqlite3.connect(path) as db:
            db.execute('CREATE TABLE peers (session_id TEXT, tool TEXT, pid INTEGER, pid_start INTEGER)')
            db.execute('INSERT INTO peers VALUES (?, ?, ?, ?)', (SID_A, "codex", 9999, 123))
        with patch.object(history.xmsg, "pid_start_time", return_value=456):
            self.assertIsNone(history.live_evidence(self.row, path))
        with patch.object(history.xmsg, "pid_start_time", return_value=123):
            self.assertIsNotNone(history.live_evidence(self.row, path))

    def test_number_picker_and_cancel(self):
        with patch("sys.stdin.isatty", return_value=True), patch("sys.stdout.isatty", return_value=True), contextlib.redirect_stdout(io.StringIO()):
            # redirect_stdout replaces the mocked stream; mock after replacement.
            with patch("sys.stdout.isatty", return_value=True), patch("builtins.input", return_value="1"):
                self.assertEqual(history.choose_thread([self.row], False)["id"], SID_A)
            with patch("sys.stdout.isatty", return_value=True), patch("builtins.input", return_value=""):
                self.assertIsNone(history.choose_thread([self.row], False))

    def test_non_tty_never_auto_selects_single_row(self):
        with patch("sys.stdin.isatty", return_value=False), self.assertRaises(history.HistoryError):
            history.choose_thread([self.row])

    def test_fzf_selection_uses_exact_id_and_strips_ambient_shell_bindings(self):
        proc = subprocess.CompletedProcess([], 0, SID_A + "\tlabel\n")
        with patch("sys.stdin.isatty", return_value=True), patch("sys.stdout.isatty", return_value=True), \
                patch.object(history.shutil, "which", return_value="/bin/fzf"), \
                patch.dict(os.environ, {"FZF_DEFAULT_OPTS": "--bind=enter:execute(bad-command)"}), \
                patch.object(history.subprocess, "run", return_value=proc) as run:
            self.assertEqual(history.choose_thread([self.row])["id"], SID_A)
        self.assertNotIn("FZF_DEFAULT_OPTS", run.call_args.kwargs["env"])
        self.assertNotIn("shell", run.call_args.kwargs)

    def test_fzf_cancellation_never_selects_a_row(self):
        with patch("sys.stdin.isatty", return_value=True), patch("sys.stdout.isatty", return_value=True), \
                patch.object(history.shutil, "which", return_value="/bin/fzf"), \
                patch.object(history.subprocess, "run", return_value=subprocess.CompletedProcess([], 130, "")):
            self.assertIsNone(history.choose_thread([self.row]))

    def test_dry_run_never_launches_codex_and_does_not_modify_stores(self):
        before = self.db.read_bytes(), self.config.read_bytes()
        with patch.object(history.os, "execvpe") as execute:
            rc, out, err = self.main("resume", SID_A, "--home", str(self.home), "--backend", "sqlite", "--dry-run")
        self.assertEqual(rc, 0, err)
        self.assertIn('model_provider="original"', json.loads(out)["argv"])
        execute.assert_not_called()
        self.assertEqual(before, (self.db.read_bytes(), self.config.read_bytes()))

    def test_switch_confirmation_rejection_does_not_execute(self):
        args = ["resume", SID_A, "--home", str(self.home), "--backend", "sqlite", "--switch-provider", "other", "--model", "model-b"]
        with patch("sys.stdin.isatty", return_value=True), patch("sys.stdout.isatty", return_value=True), \
                patch("builtins.input", return_value="no"), patch.object(history, "live_evidence", return_value=None), \
                patch.object(history.os, "execvpe") as execute:
            # Do not redirect stdout: isatty controls whether launch is reachable.
            rc = history.main(args)
        self.assertEqual(rc, 0)
        execute.assert_not_called()

    def test_confirmed_resume_executes_argv_with_selected_home(self):
        args = ["resume", SID_A, "--home", str(self.home), "--backend", "sqlite"]
        with patch("sys.stdin.isatty", return_value=True), patch("sys.stdout.isatty", return_value=True), \
                patch("builtins.input", return_value="y"), patch.object(history, "live_evidence", return_value=None), \
                patch.object(history.os, "execvpe") as execute:
            self.assertEqual(history.main(args), 0)
        self.assertEqual(execute.call_args.args[1][2], SID_A)
        self.assertEqual(execute.call_args.args[2]["CODEX_HOME"], str(self.home))

    def test_live_thread_never_launches(self):
        with patch("sys.stdin.isatty", return_value=True), patch("sys.stdout.isatty", return_value=True), \
                patch.object(history, "live_evidence", return_value="already running"), \
                patch.object(history.os, "execvpe") as execute:
            self.assertEqual(history.main(["resume", SID_A, "--home", str(self.home), "--backend", "sqlite"]), 2)
        execute.assert_not_called()

    def test_symlink_entry_works_from_another_directory(self):
        link = self.home / "codex-history"
        link.symlink_to(ROOT / "bin" / "codex-history")
        proc = subprocess.run([str(link), "list", "--home", str(self.home), "--backend", "sqlite", "--json"],
                              cwd=self.workspace, capture_output=True, text=True)
        self.assertEqual(proc.returncode, 0, proc.stderr)
        self.assertEqual(json.loads(proc.stdout)["threads"][0]["id"], SID_A)


class WebSocketTest(unittest.TestCase):
    def setUp(self):
        client, server = socket.socketpair()
        self.addCleanup(client.close)
        self.addCleanup(server.close)
        self.server = server
        self.client = history.RpcClient.__new__(history.RpcClient)
        self.client.socket = client
        self.client.deadline = time.monotonic() + 2
        self.client.counter = 0

    @staticmethod
    def frame(opcode, data, final=True):
        header = bytes([(0x80 if final else 0) | opcode])
        if len(data) < 126:
            return header + bytes([len(data)]) + data
        if len(data) < 65536:
            return header + b'\x7e' + struct.pack('>H', len(data)) + data
        return header + b'\x7f' + struct.pack('>Q', len(data)) + data

    @staticmethod
    def exact(sock, count):
        data = b''
        while len(data) < count:
            part = sock.recv(count - len(data))
            if not part:
                raise RuntimeError('unexpected EOF')
            data += part
        return data

    def decode_client_frame(self):
        header = self.exact(self.server, 2)
        self.assertTrue(header[1] & 0x80)
        length = header[1] & 0x7f
        if length == 126:
            length = struct.unpack('>H', self.exact(self.server, 2))[0]
        elif length == 127:
            length = struct.unpack('>Q', self.exact(self.server, 8))[0]
        mask = self.exact(self.server, 4)
        data = self.exact(self.server, length)
        return header[0] & 15, bytes(b ^ mask[i % 4] for i, b in enumerate(data))

    def test_client_masks_all_three_payload_lengths(self):
        for count in (10, 200, 70000):
            with self.subTest(count=count):
                data = {'message': 'x' * count}
                self.client.send(data)
                opcode, payload = self.decode_client_frame()
                self.assertEqual(opcode, 1)
                self.assertEqual(json.loads(payload), data)

    def test_large_server_text_frame(self):
        data = {'result': 'x' * 70000}
        self.server.sendall(self.frame(1, json.dumps(data).encode()))
        self.assertEqual(self.client.receive(), data)

    def test_fragmented_text_with_ping_and_pong(self):
        data = json.dumps({'中文': '内容'}, ensure_ascii=False).encode()
        self.server.sendall(self.frame(1, data[:5], False) + self.frame(9, b'ping') +
                            self.frame(10, b'pong') + self.frame(0, data[5:]))
        self.assertEqual(self.client.receive(), {'中文': '内容'})
        self.assertEqual(self.decode_client_frame(), (10, b'ping'))

    def test_reject_masked_server_frame(self):
        self.server.sendall(b'\x81\x80')
        with self.assertRaisesRegex(history.HistoryError, 'mask'):
            self.client.receive()

    def test_reject_invalid_control_and_continuation(self):
        for data in (self.frame(9, b'', False), self.frame(0, b'{}')):
            self.server.sendall(data)
            with self.assertRaises(history.HistoryError):
                self.client.receive()

    def test_oversize_rejected_before_reading_body(self):
        self.server.sendall(b'\x81\x7f' + struct.pack('>Q', 16 * 1024 * 1024 + 1))
        with self.assertRaisesRegex(history.HistoryError, '过大'):
            self.client.receive()

    def test_total_deadline_enforced(self):
        self.client.deadline = time.monotonic() - 1
        with self.assertRaisesRegex(history.HistoryError, '超时'):
            self.client.receive()
        with self.assertRaisesRegex(history.HistoryError, '超时'):
            self.client.send({'id': 1})

    def test_server_request_same_id_cannot_be_mistaken_for_response(self):
        self.server.sendall(self.frame(1, json.dumps({'id': 1, 'method': 'approve'}).encode()) +
                            self.frame(1, json.dumps({'id': 1, 'result': {'data': []}}).encode()))
        self.assertEqual(self.client.call('thread/list', {}), {'data': []})
        self.assertEqual(json.loads(self.decode_client_frame()[1])['method'], 'thread/list')
        self.assertEqual(json.loads(self.decode_client_frame()[1])['error']['code'], -32601)

    def test_real_unix_upgrade_and_paginated_api(self):
        with tempfile.TemporaryDirectory(prefix='history-ws-') as tmp:
            home = Path(tmp)
            folder = home / 'app-server-control'
            folder.mkdir()
            listener = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
            self.addCleanup(listener.close)
            listener.bind(str(folder / 'app-server-control.sock'))
            listener.listen(1)
            listener.settimeout(3)
            errors, requests = [], []

            def serve():
                try:
                    conn, _ = listener.accept()
                    with conn:
                        conn.settimeout(3)
                        request = b''
                        while b'\r\n\r\n' not in request:
                            request += self.exact(conn, 1)
                        key = history.RpcClient._header(request, b'sec-websocket-key')
                        accept = base64.b64encode(hashlib.sha1(key + b'258EAFA5-E914-47DA-95CA-C5AB0DC85B11').digest())
                        conn.sendall(b'HTTP/1.1 101 Switching Protocols\r\nUpgrade: websocket\r\n'
                                     b'Connection: Upgrade\r\nSec-WebSocket-Accept: ' + accept + b'\r\n\r\n')
                        self.server = conn
                        for _ in range(4):
                            message = json.loads(self.decode_client_frame()[1])
                            requests.append(message)
                            if message['method'] == 'initialized':
                                continue
                            if message['method'] == 'initialize':
                                result = {}
                            elif message['params']['cursor'] is None:
                                result = {'data': [{'id': SID_A, 'modelProvider': 'original'}], 'nextCursor': 'page2'}
                            else:
                                result = {'data': [{'id': SID_B, 'modelProvider': 'other'}], 'nextCursor': None}
                            conn.sendall(self.frame(1, json.dumps({'id': message['id'], 'result': result}).encode()))
                except BaseException as exc:
                    errors.append(exc)

            server = threading.Thread(target=serve, daemon=True)
            server.start()
            try:
                rows = history.read_api(home, 'unused', False, False)
            finally:
                server.join(4)
            self.assertFalse(server.is_alive())
            self.assertEqual(errors, [])
            self.assertEqual([r['id'] for r in rows], [SID_A, SID_B])
            self.assertEqual([r['method'] for r in requests], ['initialize', 'initialized', 'thread/list', 'thread/list'])


if __name__ == "__main__":
    unittest.main()
