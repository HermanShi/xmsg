"""Version negotiation and at-most-once delivery; no real hosts are contacted."""
from __future__ import annotations

import contextlib
import io
import os
import subprocess
import unittest
from pathlib import Path
from unittest import mock

import codex_delivery as delivery
from test_xmsg import Base


class FakeRpc:
    def __init__(self, status="idle", version="0.156.1", loaded=True, accept=True, write_error=None,
                 source="source", client_version="0.156.1"):
        self.counter = 0
        self.calls = []
        self.replies = []
        self.status, self.version, self.loaded, self.accept = status, version, loaded, accept
        self.write_error = write_error
        self.source, self.client_version = source, client_version
        self.closed = False

    def close(self):
        self.closed = True

    def send(self, message):
        self.calls.append(message)
        method = message.get("method")
        if "id" not in message:
            return
        if method.startswith("turn/") and self.write_error == "timeout":
            raise TimeoutError("write may have reached the server")
        result = {
            "initialize": {"userAgent": "codex-tui/" + self.version},
            "thread/loaded/list": {"data": ["target", self.source] if self.loaded else []},
            "thread/read": {"thread": {"status": {"type": self.status}, "canAcceptDirectInput": self.accept,
                                        "cliVersion": self.client_version}},
            "thread/turns/list": {"data": [{"id": "active-turn", "status": "inProgress"}]},
            "turn/start": {"turn": {"id": "new-turn"}},
            "turn/steer": {"turnId": "active-turn"},
        }[method]
        reply = {"id": message["id"], "result": result}
        if method.startswith("turn/") and self.write_error == "reject":
            reply = {"id": message["id"], "error": {"code": -32602, "message": "rejected"}}
        self.replies.append(reply)

    def receive(self):
        return self.replies.pop(0)


class VisibleRpcTests(unittest.TestCase):
    def send(self, fake, source="", text="peer message", native_text=None):
        with mock.patch("codex_history.RpcClient", return_value=fake):
            return delivery.send_visible(Path("/isolated"), "target", text, 2, source_thread=source,
                                         tui_version=fake.client_version, native_text=native_text)

    def test_idle_uses_start_with_no_model_or_permission_overrides(self):
        fake = FakeRpc()
        self.assertTrue(self.send(fake)[0])
        call = fake.calls[-1]
        self.assertEqual(call["method"], "turn/start")
        self.assertEqual(set(call["params"]), {"threadId", "input"})
        self.assertTrue(fake.closed)

    def test_busy_uses_steer_and_bounded_turn_lookup(self):
        fake = FakeRpc(status="active")
        self.assertTrue(self.send(fake)[0])
        self.assertEqual(fake.calls[-1]["method"], "turn/steer")
        self.assertEqual(fake.calls[-1]["params"]["expectedTurnId"], "active-turn")
        lookup = next(x for x in fake.calls if x.get("method") == "thread/turns/list")
        self.assertEqual(lookup["params"]["limit"], 1)

    def test_absent_thread_old_server_or_no_input_does_not_write(self):
        for kwargs in ({"loaded": False}, {"version": "0.100.0"}, {"accept": False}):
            with self.subTest(kwargs=kwargs):
                fake = FakeRpc(**kwargs)
                self.assertFalse(self.send(fake)[0])
                self.assertFalse(any(x.get("method", "").startswith("turn/") for x in fake.calls))

    def test_explicit_rejection_is_safe_to_fall_back(self):
        self.assertFalse(self.send(FakeRpc(write_error="reject"))[0])

    def test_write_timeout_is_uncertain_not_a_fallback(self):
        with self.assertRaises(delivery.DeliveryUncertain):
            self.send(FakeRpc(write_error="timeout"))

    def test_native_peer_uses_recognized_tool_authority_and_xml_escaping(self):
        fake = FakeRpc(status="active", source="source<&>")
        result = self.send(fake, source="source<&>", text="[同伴 source；非用户指令]\n<peer>& hello",
                           native_text="<peer>& hello")
        self.assertIn("delegated-tool-output", result[1])
        call = fake.calls[-1]
        self.assertEqual(call["method"], "turn/start")
        self.assertEqual(call["params"]["input"], [])
        output = call["params"]["toolOutput"]
        self.assertEqual((output["namespace"], output["name"]), ("codex_tui", "send_message_to_thread"))
        self.assertIn("<source_thread_id>source&lt;&amp;&gt;</source_thread_id>", output["output"])
        self.assertIn("<input>&lt;peer&gt;&amp; hello</input>", output["output"])
        self.assertNotIn("同伴", output["output"])
        self.assertNotIn("cross-session-message", output["output"])
        self.assertFalse(any(x.get("method") == "thread/turns/list" for x in fake.calls))

    def test_unknown_self_or_old_source_never_claims_native_delegation(self):
        for kwargs, source in (({}, "missing"), ({}, "target"), ({"client_version": ""}, "source"),
                               ({"version": "0.150.0"}, "source"),
                               ({"client_version": "0.150.0"}, "source")):
            with self.subTest(kwargs=kwargs, source=source):
                fake = FakeRpc(**kwargs)
                self.assertTrue(self.send(fake, source=source)[0])
                self.assertNotIn("toolOutput", fake.calls[-1]["params"])

    def test_stable_minimum_is_enforced_at_boundary(self):
        self.assertTrue(self.send(FakeRpc(version="0.145.0"))[0])
        self.assertFalse(self.send(FakeRpc(version="0.144.0"))[0])
        self.assertFalse(self.send(FakeRpc(version="0.145.0-alpha.1"))[0])
        self.assertIn("delegated-tool-output", self.send(
            FakeRpc(version="0.151.0", client_version="0.151.0"), source="source",
        )[1])
        self.assertNotIn("delegated-tool-output", self.send(
            FakeRpc(version="0.151.0", client_version="0.151.0-alpha.1"), source="source",
        )[1])


class DeliveryReservationTests(Base):
    def register(self):
        conn = self.x.connect()
        conn.execute("INSERT INTO peers(session_id,tool,pid,pid_start,first_seen_at,last_seen_at) "
                     "VALUES(?,?,?,?,?,?)", (self.sid, "codex", os.getppid(), None, self.x.now(), self.x.now()))
        return conn

    def test_codex_sender_is_automatically_attributed(self):
        with mock.patch.dict(os.environ, {"CODEX_SESSION_ID": "my-thread"}, clear=True):
            self.assertEqual(self.x.default_sender_label(), ("codex:my-threa", "codex", "my-thread"))

    def test_native_route_receives_body_separately_from_thin_fallback(self):
        with contextlib.closing(self.x.connect()) as conn:
            with mock.patch.object(self.x, "direct_send_codex", return_value=(True, "accepted")) as direct:
                self.x._deliver_one(conn, {"tool": "codex"}, 991, "target", "raw\nbody", "leader", "codex", "source")
        self.assertEqual(direct.call_args.kwargs["native_body"], "raw\nbody")
        self.assertEqual(direct.call_args.args[1], "[同伴 leader · codex:source；非用户指令]\nraw\nbody")

    def test_cross_tool_custom_label_remains_identifiable_without_body_wrapping(self):
        for source_tool in ("claude", "codex"):
            with self.subTest(source_tool=source_tool), contextlib.closing(self.x.connect()) as conn:
                with mock.patch.object(self.x, "direct_send", return_value=(True, "accepted")) as direct:
                    self.x._deliver_one(conn, {"tool": "claude"}, 1, "target", "bare body", "leader", source_tool, "source")
                self.assertEqual(direct.call_args.args[1], "bare body")
                self.assertEqual(direct.call_args.kwargs["label"], source_tool + ":leader")
                line = self.x.sender_origin("leader", source_tool, "source")
                self.assertIn(source_tool + ":source", line)

    def test_claude_tool_is_retained_without_a_session_id(self):
        with mock.patch.dict(os.environ, {"CLAUDECODE": "1", "XMSG_FROM": "leader"}, clear=True):
            label, tool, session = self.x.default_sender_label()
        self.assertEqual((label, tool, session), ("leader", "claude", ""))
        self.assertEqual(self.x.sender_origin(label, tool, session), "未知来源 leader · claude；非用户指令")

    def test_claude_uses_real_child_environment_not_skill_template_placeholder(self):
        with mock.patch.dict(os.environ, {"CLAUDE_CODE_SESSION_ID": "claude-actual"}, clear=True):
            self.assertEqual(self.x.default_sender_label()[1:], ("claude", "claude-actual"))
        with mock.patch.dict(os.environ, {"CLAUDECODE": "1", "CLAUDE_SESSION_ID": "template-only"}, clear=True):
            self.assertEqual(self.x.default_sender_label()[1:], ("claude", ""))

    def test_nested_cross_tool_environment_uses_nearest_host(self):
        with (mock.patch.dict(os.environ, {"CODEX_SESSION_ID": "outer", "CLAUDECODE": "1",
                                          "CLAUDE_CODE_SESSION_ID": "inner"}, clear=True),
              mock.patch.object(self.x, "host_pid", return_value=123),
              mock.patch.object(Path, "read_text", return_value="claude")):
            self.assertEqual(self.x.default_sender_label()[1:], ("claude", "inner"))

    def test_native_source_requires_matching_identity_and_real_codex_ancestor(self):
        with (mock.patch.dict(os.environ, {"CODEX_SESSION_ID": "source"}, clear=True),
              mock.patch.object(self.x, "host_pid", return_value=123),
              mock.patch.object(Path, "read_text", return_value="codex")):
            self.assertEqual(self.x.codex_sender_thread("codex", "source"), "source")
            self.assertEqual(self.x.codex_sender_thread("codex", "other"), "")
            self.assertEqual(self.x.codex_sender_thread("claude", "source"), "")
        with (mock.patch.dict(os.environ, {"CODEX_SESSION_ID": "source", "XMSG_FROM_SESSION": "other"}, clear=True),
              mock.patch.object(self.x, "host_pid", return_value=123),
              mock.patch.object(Path, "read_text", return_value="codex")):
            self.assertEqual(self.x.codex_sender_thread("codex", "other"), "")
        with (mock.patch.dict(os.environ, {"XMSG_FROM_SESSION": "source"}, clear=True),
              mock.patch.object(self.x, "host_pid", return_value=123),
              mock.patch.object(Path, "read_text", return_value="codex")):
            self.assertEqual(self.x.codex_sender_thread("codex", "source"), "")
        with (mock.patch.dict(os.environ, {"XMSG_FROM_SESSION": "source", "XMSG_FROM_TOOL": "codex"}, clear=True),
              mock.patch.object(self.x, "host_pid", return_value=0),
              mock.patch.object(Path, "read_text", side_effect=FileNotFoundError)):
            self.assertEqual(self.x.codex_sender_thread("codex", "source"), "")

    def fake_process(self, pid, args, *, home="/isolated", executable=None):
        proc = self.tmp / str(pid)
        proc.mkdir()
        (proc / "comm").write_text("codex")
        (proc / "cmdline").write_bytes(("\0".join(["codex", *args]) + "\0").encode())
        (proc / "environ").write_bytes(("CODEX_HOME=" + home + "\0").encode())
        if executable:
            (proc / "exe").symlink_to(executable)
        else:
            (proc / "exe").touch()
        return proc

    def test_tui_version_filters_noninteractive_and_other_profiles_and_caches(self):
        tui = self.fake_process(1, ["resume", "thread"])
        duplicate = self.fake_process(2, [], executable=tui / "exe")
        daemon = self.fake_process(3, ["app-server"])
        queue = self.fake_process(4, ["queue", "--thread", "thread"])
        execution = self.fake_process(5, ["exec", "task"])
        other = self.fake_process(6, [], home="/other")
        with (mock.patch.object(Path, "glob", return_value=[tui, duplicate, daemon, queue, execution, other]),
              mock.patch.object(self.x, "executable_version", return_value="0.156.1") as version):
            self.assertEqual(self.x.running_codex_tui_version(Path("/isolated")), "0.156.1")
            self.assertEqual(version.call_count, 1)

    def test_tui_minimum_is_conservative_with_old_or_unknown_clients(self):
        a = self.fake_process(1, [])
        b = self.fake_process(2, [])
        with (mock.patch.object(Path, "glob", return_value=[a, b]),
              mock.patch.object(self.x, "executable_version", side_effect=["0.156.1", "0.150.0"])):
            self.assertEqual(self.x.running_codex_tui_version(Path("/isolated")), "0.150.0")
        with (mock.patch.object(Path, "glob", return_value=[a]),
              mock.patch.object(self.x, "executable_version", return_value="")):
            self.assertEqual(self.x.running_codex_tui_version(Path("/isolated")), "")

    def test_shared_parent_pid_is_not_self_delivery(self):
        with mock.patch.object(self.x, "send_visible", return_value=(True, "codex-turn-steer accepted")):
            self.assertTrue(self.x.direct_send_codex(
                {"pid": os.getppid(), "pid_start": None}, "hello", to_session="target", from_session="sender",
            )[0])
            self.assertFalse(self.x.direct_send_codex(
                {"pid": os.getppid(), "pid_start": None}, "hello", to_session="sender", from_session="sender",
            )[0])
            with mock.patch.dict(os.environ, {"CODEX_SESSION_ID": "actual"}, clear=True):
                self.assertFalse(self.x.direct_send_codex(
                    {"pid": os.getppid(), "pid_start": None}, "hello", to_session="actual", from_session="spoofed",
                )[0])

    def test_shared_daemon_does_not_queue_for_unloaded_thread(self):
        with (mock.patch.object(self.x, "send_visible", return_value=(False, "not loaded")),
              mock.patch.object(self.x, "is_codex_daemon", return_value=True),
              mock.patch.object(self.x, "invoke_codex_queue") as queue):
            self.assertFalse(self.x.direct_send_codex(
                {"pid": os.getppid(), "pid_start": None}, "hello", to_session="target",
            )[0])
            queue.assert_not_called()

    def test_hook_cannot_claim_while_direct_write_is_in_progress(self):
        conn = self.register()
        self.addCleanup(conn.close)
        mid = self.queue("visible")
        def write(*args, **kwargs):
            with contextlib.closing(self.x.connect()) as hook_conn:
                self.assertEqual(self.x.claim(hook_conn, self.sid, "codex", "PreToolUse"), [])
            return True, "codex-turn-start accepted"
        with mock.patch.object(self.x, "direct_send_codex", side_effect=write):
            self.assertTrue(self.x.direct_deliver(conn, [mid], [self.sid], "visible", "peer", "codex", "other")[mid][0])

    def test_hook_winner_prevents_a_second_direct_write(self):
        conn = self.register()
        self.addCleanup(conn.close)
        mid = self.queue("hello")
        self.x.claim(conn, self.sid, "codex", "PreToolUse")
        with mock.patch.object(self.x, "direct_send_codex") as write:
            self.x.direct_deliver(conn, [mid], [self.sid], "hello", "peer", "codex", "other")
            write.assert_not_called()

    def test_uncertain_delivery_is_held_and_reported_not_retried(self):
        conn = self.register()
        self.addCleanup(conn.close)
        mid = self.queue("hello")
        with mock.patch.object(self.x, "direct_send_codex", side_effect=delivery.DeliveryUncertain("lost ACK")):
            result = self.x.direct_deliver(conn, [mid], [self.sid], "hello", "peer", "codex", "other")
        self.assertIn("UNKNOWN", result[mid][1])
        self.assertEqual(self.x.claim(conn, self.sid, "codex", "PreToolUse"), [])
        stdout = io.StringIO()
        with contextlib.redirect_stdout(stdout):
            self.x.main(["outbox"])
        self.assertIn("UNKNOWN delivery outcome", stdout.getvalue())

    def test_queue_timeout_is_not_retried_via_hook(self):
        with mock.patch.object(self.x.subprocess, "run", side_effect=subprocess.TimeoutExpired("codex", 1)):
            with self.assertRaises(delivery.DeliveryUncertain):
                self.x.invoke_codex_queue("codex", "target", "hello")

    def test_cli_returns_nonzero_when_delivery_ack_is_unknown(self):
        conn = self.register()
        self.addCleanup(conn.close)
        with (mock.patch.object(self.x, "direct_send_codex", side_effect=delivery.DeliveryUncertain("lost ACK")),
              contextlib.redirect_stdout(io.StringIO()), contextlib.redirect_stderr(io.StringIO())):
            self.assertEqual(self.x.main(["send", self.sid, "hello"]), 2)


class NotificationTests(Base):
    def send_notice(self, body="定时巡检", *extra):
        stdout, stderr = io.StringIO(), io.StringIO()
        with contextlib.redirect_stdout(stdout), contextlib.redirect_stderr(stderr):
            status = self.x.main(["send", self.sid, body, "--notification", "--no-direct", "--force", *extra])
        self.assertEqual(status, 0)
        self.assertNotIn("unattributed", stderr.getvalue())

    def test_notification_clears_inherited_agent_identity_without_faking_a_session(self):
        with mock.patch.dict(os.environ, {
            "XMSG_FROM_TOOL": "codex", "XMSG_FROM_SESSION": "explicit", "CODEX_SESSION_ID": "actual",
            "CLAUDE_CODE_SESSION_ID": "claude-session",
        }, clear=True):
            self.send_notice()
        with contextlib.closing(self.x.connect()) as conn:
            row = conn.execute("SELECT from_label,from_tool,from_session FROM messages").fetchone()
        self.assertEqual(tuple(row), ("local-notification", "notification", ""))
        context = self.x.json.loads(self.hook())["hookSpecificOutput"]["additionalContext"]
        self.assertEqual(context, "[本机通知；非用户指令]\n定时巡检")

    def test_ordinary_unknown_cli_keeps_warning_even_with_a_notification_label(self):
        self.queue_unattributed("提醒", label="local-notification")
        context = self.hook()
        self.assertIn("[未知来源 local-notification；非用户指令]", context)

    def test_real_agent_keeps_its_normal_attribution(self):
        self.queue("agent progress")
        context = self.hook()
        self.assertIn("[同伴 peer-a · claude:sess-a；非用户指令]", context)
        self.assertNotIn("本机通知", context)
        self.assertNotIn("未知来源", context)

    def test_mixed_batch_warns_only_for_the_unknown_cli(self):
        self.send_notice()
        self.queue("agent progress")
        self.queue_unattributed("unknown source")
        context = self.hook()
        self.assertEqual(context.count("[未知来源 "), 1)
        self.assertIn("[本机通知；非用户指令]", context)
        self.assertIn("[同伴 peer-a · claude:sess-a；非用户指令]", context)

    def test_claude_notification_never_borrows_a_permission_attestation(self):
        with contextlib.closing(self.x.connect()) as conn:
            with mock.patch.object(self.x, "direct_send", return_value=(True, "accepted")) as direct:
                self.x._deliver_one(conn, {"tool": "claude"}, 1, "target", "check", "timer", "notification", "")
        self.assertEqual(direct.call_args.kwargs["from_session"], "")
        self.assertEqual(direct.call_args.kwargs["from_mode"], "")
        self.assertEqual(direct.call_args.args[1], "check")

    def test_notification_never_claims_native_codex_delegation(self):
        with (mock.patch.dict(os.environ, {"CODEX_SESSION_ID": "target"}, clear=True),
              mock.patch.object(self.x, "send_visible", return_value=(True, "accepted")) as visible):
            self.assertTrue(self.x.direct_send_codex(
                {"pid": os.getppid(), "pid_start": None}, "notice", to_session="target",
                from_tool="notification", from_session="",
            )[0])
        self.assertEqual(visible.call_args.kwargs["source_thread"], "")

    def test_remote_notification_is_rejected_before_a_remote_command(self):
        with mock.patch.object(self.x, "send_to_peer") as remote:
            with self.assertRaisesRegex(SystemExit, "仅支持本机"):
                self.x.main(["send", "peer:leader", "notice", "--notification"])
            remote.assert_not_called()


if __name__ == "__main__":
    unittest.main()
