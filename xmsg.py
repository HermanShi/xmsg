#!/usr/bin/env python3
"""xmsg - visible cross-session delivery for Claude Code / Codex CLI.

Send persists then reserves a message before direct I/O. Codex uses its existing
app-server's visible API after version/capability discovery, or the older queue
CLI. Claude uses its permission-aware peer socket. Definite failures fall back
to PreToolUse/Stop hooks; unknown write outcomes remain held to avoid duplicates.

Why a separate database: ~/.agent-memory/index.sqlite3 is mirrored on a
one-minute systemd timer and rebuilt from Markdown; message rows have neither
property and would fight that machinery. This file is pure runtime state.

Delivery semantics: at-most-once, claimed atomically. See DESIGN in README.md.
"""

from __future__ import annotations

import argparse
import json
import os
import re
import shlex
import shutil
import socket
import sqlite3
import subprocess
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from codex_delivery import DeliveryUncertain, MIN_CODEX_QUEUE, loaded_thread_ids, send_visible, version_tuple

DB_PATH = Path(
    os.environ.get(
        "XMSG_DB",
        Path.home() / ".local" / "share" / "agent-msg" / "messages.sqlite3",
    )
)

# These are read-only discovery inputs.  They are deliberately configurable so
# tests and operators using a separate profile can inspect the matching queues
# without changing the xmsg database.
CODEX_HOME = Path(os.environ.get("CODEX_HOME", Path.home() / ".codex"))
CLAUDE_HOME = Path(os.environ.get("CLAUDE_HOME", Path.home() / ".claude"))
CODEX_QUEUE_DB = Path(
    os.environ.get("XMSG_CODEX_QUEUE_DB", str(CODEX_HOME / "queue_1.sqlite"))
)
CODEX_SESSION_INDEX = Path(
    os.environ.get("XMSG_CODEX_SESSION_INDEX", str(CODEX_HOME / "session_index.jsonl"))
)
CLAUDE_PROJECTS = Path(
    os.environ.get("XMSG_CLAUDE_PROJECTS", str(CLAUDE_HOME / "projects"))
)

# A priority only affects messages waiting for the xmsg hook.  The official
# `codex queue` command has no priority flag; its messages remain ordered by
# Codex itself and are reported as "next turn" rather than "current turn".
DEFAULT_MESSAGE_PRIORITY = 0
URGENT_MESSAGE_PRIORITY = 100

# A session that has not run a tool call in this long is no longer offered as a
# delivery target by `xmsg list`. PreToolUse fires many times per turn, so a
# live session refreshes this constantly; the only way to go quiet is to stop.
PEER_STALE_SECONDS = int(os.environ.get("XMSG_PEER_STALE_SECONDS", "1800"))

# An undelivered message older than this is abandoned rather than injected.
# Without it a message addressed to a session that already exited would sit in
# the queue forever and land on whatever future session happens to reuse the
# id, arbitrarily far out of context.
DEFAULT_TTL_SECONDS = int(os.environ.get("XMSG_TTL_SECONDS", "3600"))

# Cap on what one hook invocation will inject, so a flood of queued messages
# can never blow up the receiver's context or trip the host's own limits.
MAX_MESSAGES_PER_INJECT = int(os.environ.get("XMSG_MAX_PER_INJECT", "10"))
MAX_BODY_CHARS = int(os.environ.get("XMSG_MAX_BODY_CHARS", "8000"))

# Delivered rows are kept for a while so `xmsg outbox` can answer "did it land",
# then deleted. Sweeping runs on send, and - throttled - on the hook path too,
# because a machine that only ever receives would otherwise never sweep at all.
RETAIN_DELIVERED_SECONDS = int(os.environ.get("XMSG_RETAIN_SECONDS", str(14 * 86400)))

# A peer that has not been seen this long is dropped from the registry.
RETAIN_PEER_SECONDS = int(os.environ.get("XMSG_RETAIN_PEER_SECONDS", str(30 * 86400)))

# How rarely the hook path is allowed to sweep. The hook fires dozens of times
# per turn and its budget belongs to delivery, so sweeping there has to be the
# exception: one session records the attempt, everyone else skips cheaply.
HOOK_SWEEP_INTERVAL_SECONDS = int(os.environ.get("XMSG_HOOK_SWEEP_INTERVAL", "3600"))

# Reclaim disk once the freelist is worth reclaiming. DELETE alone leaves the
# pages in the file, so a burst that inflates the database keeps that size for
# good without this.
VACUUM_FREE_PAGE_THRESHOLD = int(os.environ.get("XMSG_VACUUM_FREE_PAGES", "256"))

SCHEMA = """
CREATE TABLE IF NOT EXISTS messages (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    created_at INTEGER NOT NULL,
    expires_at INTEGER NOT NULL,
    from_label TEXT NOT NULL,
    from_tool TEXT NOT NULL DEFAULT '',
    from_session TEXT NOT NULL DEFAULT '',
    to_session TEXT NOT NULL,
    body TEXT NOT NULL,
    priority INTEGER NOT NULL DEFAULT 0,
    -- NULL while queued. Set exactly once, inside the claiming transaction.
    delivered_at INTEGER,
    delivered_tool TEXT,
    delivered_event TEXT,
    -- Set instead of delivered_at when the TTL ran out first.
    expired_at INTEGER
);

-- The hook's hot path: one indexed lookup per PreToolUse.
CREATE INDEX IF NOT EXISTS messages_pending_idx
    ON messages(to_session, delivered_at, expired_at);

CREATE TABLE IF NOT EXISTS peers (
    session_id TEXT PRIMARY KEY,
    tool TEXT NOT NULL,
    cwd TEXT NOT NULL DEFAULT '',
    model TEXT NOT NULL DEFAULT '',
    label TEXT NOT NULL DEFAULT '',
    -- Direct-delivery coordinates, filled in by the receiver's own hook: the
    -- host process behind this session, when it started (so a recycled pid is
    -- not mistaken for the same session), the socket it listens on, and the
    -- permission mode it reported. See "UDS direct delivery" below.
    pid INTEGER,
    pid_start INTEGER,
    sock_path TEXT NOT NULL DEFAULT '',
    permission_mode TEXT NOT NULL DEFAULT '',
    first_seen_at INTEGER NOT NULL,
    last_seen_at INTEGER NOT NULL,
    injected_count INTEGER NOT NULL DEFAULT 0
);

CREATE INDEX IF NOT EXISTS peers_last_seen_idx ON peers(last_seen_at);

-- One row per housekeeping key. Exists so the hook path can rate-limit its own
-- sweeping without a lock file or a timestamp on disk.
CREATE TABLE IF NOT EXISTS meta (
    key TEXT PRIMARY KEY,
    value INTEGER NOT NULL
);
"""


# --------------------------------------------------------------------------
# storage
# --------------------------------------------------------------------------
# Columns added to `peers` after the first release. CREATE TABLE IF NOT EXISTS
# covers a missing table but never a missing column, so these are applied by
# hand; ALTER TABLE ADD COLUMN is cheap and idempotent once guarded.
PEER_ADDED_COLUMNS = {
    "pid": "INTEGER",
    "pid_start": "INTEGER",
    "sock_path": "TEXT NOT NULL DEFAULT ''",
    "permission_mode": "TEXT NOT NULL DEFAULT ''",
}

MESSAGE_ADDED_COLUMNS = {
    "priority": "INTEGER NOT NULL DEFAULT 0",
}


def migrate(conn: sqlite3.Connection) -> None:
    """Bring an existing database up to the current `peers` shape."""
    have = {row["name"] for row in conn.execute("PRAGMA table_info(peers)")}
    if have:
        for column, decl in PEER_ADDED_COLUMNS.items():
            if column not in have:
                conn.execute(f"ALTER TABLE peers ADD COLUMN {column} {decl}")
    message_have = {row["name"] for row in conn.execute("PRAGMA table_info(messages)")}
    if message_have:
        for column, decl in MESSAGE_ADDED_COLUMNS.items():
            if column not in message_have:
                conn.execute(f"ALTER TABLE messages ADD COLUMN {column} {decl}")


def connect(*, create: bool = True) -> sqlite3.Connection:
    if create:
        DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    # A short timeout is deliberate: this runs inside a hook with a <=5s budget,
    # and waiting on a lock is worse than skipping a delivery.
    conn = sqlite3.connect(DB_PATH, timeout=2.0, isolation_level=None)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA busy_timeout=2000")
    if create:
        conn.executescript(SCHEMA)
        migrate(conn)
    return conn


def now() -> int:
    return int(time.time())


def iso(ts: int) -> str:
    return datetime.fromtimestamp(ts, timezone.utc).astimezone().isoformat(timespec="seconds")


def ago(ts: int) -> str:
    d = max(0, now() - ts)
    if d < 60:
        return f"{d}s"
    if d < 3600:
        return f"{d // 60}m"
    if d < 86400:
        return f"{d // 3600}h"
    return f"{d // 86400}d"


def _timestamp(value: Any, fallback: int = 0) -> int:
    """Parse the ISO timestamps used by Codex's index without making discovery fragile."""
    if isinstance(value, (int, float)):
        return int(value)
    if isinstance(value, str) and value:
        try:
            return int(datetime.fromisoformat(value.replace("Z", "+00:00")).timestamp())
        except ValueError:
            return fallback
    return fallback


def _jsonl_records(path: Path) -> list[dict[str, Any]]:
    if not path.is_file():
        return []
    records: list[dict[str, Any]] = []
    try:
        with path.open(encoding="utf-8") as handle:
            for line in handle:
                try:
                    value = json.loads(line)
                except (json.JSONDecodeError, OSError):
                    continue
                if isinstance(value, dict):
                    records.append(value)
    except OSError:
        return []
    return records


def discover_sessions(conn: sqlite3.Connection) -> list[dict[str, Any]]:
    """Merge xmsg peers with Codex and Claude's local session name indexes.

    The peer row remains authoritative for liveness and direct-delivery
    coordinates.  The other two sources are read-only indexes, so a session can
    still be found by name before it has installed the xmsg hook.
    """
    by_id: dict[str, dict[str, Any]] = {}
    for row in conn.execute("SELECT * FROM peers ORDER BY last_seen_at DESC"):
        item = dict(row)
        item.update({"name": item.get("label") or "", "source": "xmsg", "updated_at": item.get("last_seen_at", 0)})
        by_id[str(item["session_id"])] = item

    # Codex writes one index row per rename.  Keep the last occurrence for a
    # thread, which is the same meaning as the resume picker.
    for item in _jsonl_records(CODEX_SESSION_INDEX):
        session_id = str(item.get("id") or "")
        if not session_id:
            continue
        record = by_id.setdefault(
            session_id,
            {"session_id": session_id, "tool": "codex", "source": "codex", "queued": 0},
        )
        record.update(
            {
                "tool": "codex",
                "name": str(item.get("thread_name") or record.get("name") or ""),
                "updated_at": _timestamp(item.get("updated_at"), int(record.get("updated_at") or 0)),
                "source": record.get("source", "codex"),
            }
        )

    # Claude's title files live below one or more project roots.  A malformed or
    # partially written title must not make list/send fail.
    try:
        title_files = CLAUDE_PROJECTS.glob("*/????????-????-????-????-????????????/custom-title.json")
    except OSError:
        title_files = []
    for path in title_files:
        session_id = path.parent.name
        try:
            value = json.loads(path.read_text(encoding="utf-8"))
            name = str(value.get("customTitle") or "").strip() if isinstance(value, dict) else ""
        except (OSError, json.JSONDecodeError):
            continue
        if not name:
            continue
        record = by_id.setdefault(
            session_id,
            {"session_id": session_id, "tool": "claude", "source": "claude", "queued": 0},
        )
        record["name"] = name
        record.setdefault("tool", "claude")
        record["source"] = record.get("source", "claude")
        try:
            record["updated_at"] = max(int(record.get("updated_at") or 0), int(path.stat().st_mtime))
        except OSError:
            pass

    for item in live_claude_sessions():
        sid = str(item.get("session_id") or "")
        if not sid:
            continue
        record = by_id.setdefault(sid, dict(item))
        for key in ("pid", "pid_start", "sock_path", "tool", "cwd"):
            value = item.get(key)
            if value not in (None, ""):
                record[key] = value
        # Keep an existing xmsg peer row as the source of permission_mode;
        # still mark that a live host is listening.
        if record.get("source") != "xmsg":
            record["source"] = "live-claude"
        record["last_seen_at"] = max(
            int(record.get("last_seen_at") or 0), int(item.get("last_seen_at") or 0)
        )
        record["updated_at"] = max(
            int(record.get("updated_at") or 0), int(item.get("updated_at") or 0)
        )

    return list(by_id.values())


def codex_queue_rows() -> list[dict[str, Any]]:
    """Read Codex's durable next-turn queue without taking a write lock."""
    if not CODEX_QUEUE_DB.is_file():
        return []
    try:
        db = sqlite3.connect(f"file:{CODEX_QUEUE_DB}?mode=ro", uri=True, timeout=0.2)
        db.row_factory = sqlite3.Row
        try:
            rows = db.execute(
                "SELECT id, thread_id, payload_json, queue_order, created_at_ms, updated_at_ms "
                "FROM queued_items ORDER BY queue_order, created_at_ms"
            ).fetchall()
        finally:
            db.close()
    except (OSError, sqlite3.Error):
        return []

    records: list[dict[str, Any]] = []
    for row in rows:
        item = dict(row)
        payload = str(item.get("payload_json") or "")
        try:
            parsed = json.loads(payload)
            text = parsed.get("UserInput", {}).get("content", [{}])[0].get("text", "")
        except (json.JSONDecodeError, AttributeError, IndexError, TypeError):
            text = ""
        item["summary"] = str(text or payload).replace("\n", " ").strip()[:160]
        item["created_at"] = int(int(item.get("created_at_ms") or 0) / 1000)
        item["updated_at"] = int(int(item.get("updated_at_ms") or 0) / 1000)
        item["source"] = "codex"
        item["state"] = "next-turn"
        records.append(item)
    return records


def _session_by_id(conn: sqlite3.Connection) -> dict[str, dict[str, Any]]:
    return {str(row["session_id"]): row for row in discover_sessions(conn)}


def _queue_name_map(conn: sqlite3.Connection) -> dict[str, str]:
    return {sid: session_name(row) for sid, row in _session_by_id(conn).items()}


def session_name(record: dict[str, Any]) -> str:
    return str(record.get("name") or "")


def _format_candidates(spec: str, rows: list[dict[str, Any]]) -> str:
    lines = []
    for row in rows:
        seen = int(row.get("last_seen_at") or row.get("updated_at") or 0)
        freshness = f", seen {ago(seen)} ago" if seen else ""
        lines.append(
            f"  {session_name(row) or '-'}  [{row.get('tool', '?')}/{row.get('source', '?')}] "
            f"{row['session_id']}{freshness}"
        )
    return "\n".join(lines)


def _record_is_live(row: dict[str, Any]) -> bool:
    """Whether a discovered record is a current target rather than history."""
    seen = int(row.get("last_seen_at") or 0)
    return bool(seen and seen >= now() - PEER_STALE_SECONDS)



def _parse_env_bytes(data: bytes) -> dict[str, str]:
    out: dict[str, str] = {}
    for item in data.split(b"\0"):
        if not item or b"=" not in item:
            continue
        key, _, value = item.partition(b"=")
        try:
            out[key.decode()] = value.decode("utf-8", "replace")
        except UnicodeDecodeError:
            continue
    return out


def live_claude_sessions() -> list[dict[str, Any]]:
    """Claude hosts listening right now, even if they have never run an xmsg hook.

    The host process often has no CLAUDE_CODE_SESSION_ID in its own environ.
    Children do, and they point CLAUDE_CODE_MESSAGING_SOCKET at `{pid}.sock`.
    """
    socks: dict[int, dict[str, Any]] = {}
    for directory in SOCK_DIRS:
        try:
            found = directory.glob("*.sock")
        except OSError:
            continue
        for sock in found:
            try:
                pid = int(sock.stem)
            except ValueError:
                continue
            start = pid_start_time(pid)
            if start is None:
                continue
            socks[pid] = {
                "pid": pid,
                "pid_start": start,
                "sock_path": str(sock),
                "tool": "claude",
                "source": "live-claude",
            }
    if not socks:
        return []
    by_sid: dict[str, dict[str, Any]] = {}
    try:
        procs = list(Path("/proc").iterdir())
    except OSError:
        return []
    for proc in procs:
        if not proc.name.isdigit():
            continue
        try:
            data = (proc / "environ").read_bytes()
        except OSError:
            continue
        if b"CLAUDE_CODE_SESSION_ID=" not in data:
            continue
        env = _parse_env_bytes(data)
        sid = env.get("CLAUDE_CODE_SESSION_ID") or ""
        sock = env.get("CLAUDE_CODE_MESSAGING_SOCKET") or ""
        if not sid or not sock:
            continue
        try:
            host_pid = int(Path(sock).stem)
        except ValueError:
            continue
        info = socks.get(host_pid)
        if info is None:
            continue
        rec = dict(info)
        rec["session_id"] = sid
        rec["cwd"] = env.get("CLAUDE_PROJECT_DIR") or env.get("PWD") or ""
        rec["last_seen_at"] = now()
        rec["updated_at"] = rec["last_seen_at"]
        rec["label"] = rec.get("label") or ""
        rec["queued"] = 0
        by_sid[sid] = rec
    return list(by_sid.values())


# --------------------------------------------------------------------------
# addressing
# --------------------------------------------------------------------------
def resolve_target(conn: sqlite3.Connection, spec: str, *, allow_unknown: bool) -> list[str]:
    """Turn a user-typed target into concrete session ids.

    Resolution is intentionally ordered: exact full id, exact custom name, then
    an id prefix (>=4 chars).  Names are not assumed unique; an ambiguous name
    is reported instead of silently sending to the wrong session.
    """
    spec = spec.strip()
    if spec.lower().startswith("peer:"):
        raise SystemExit("xmsg: internal error: peer: prefix leaked into resolve_target")
    if spec == "all":
        # Include idle Claude hosts that have never registered an xmsg peer
        # row — they still have a live socket, and "all" should mean all
        # reachable sessions, not only ones that have already called a tool.
        rows = [r for r in discover_sessions(conn) if _record_is_live(r)]
        if not rows:
            raise SystemExit("xmsg: no live sessions to broadcast to (try `xmsg list --all`)")
        return [str(r["session_id"]) for r in rows]

    discovered = discover_sessions(conn)
    exact = [row for row in discovered if row["session_id"] == spec]
    if exact:
        return [spec]

    named = [row for row in discovered if session_name(row).casefold() == spec.casefold()]
    live_named = [row for row in named if _record_is_live(row)]
    # Local title/index files retain old sessions indefinitely.  Prefer current
    # xmsg peers when present, so `xmsg send leader` does not become ambiguous
    # merely because a machine has historical sessions with that title.
    if live_named:
        named = live_named
    if len(named) == 1:
        return [str(named[0]["session_id"])]
    if len(named) > 1:
        raise SystemExit(
            f"xmsg: session name {spec!r} matches {len(named)} sessions:\n"
            f"{_format_candidates(spec, named)}\n"
            "Use the complete session id to choose one."
        )

    if len(spec) >= 4:
        rows = [row for row in discovered if str(row["session_id"]).startswith(spec)]
        rows.sort(key=lambda row: int(row.get("last_seen_at") or row.get("updated_at") or 0), reverse=True)
        if len(rows) == 1:
            return [str(rows[0]["session_id"])]
        if len(rows) > 1:
            raise SystemExit(f"xmsg: prefix {spec!r} matches {len(rows)} sessions:\n{_format_candidates(spec, rows)}")

    if allow_unknown:
        # Deliberate escape hatch: a session that has not run a tool call yet
        # has never registered as a peer, but is still a valid address.
        return [spec]
    raise SystemExit(
        f"xmsg: no session matches {spec!r}. Run `xmsg list` to see live sessions, "
        f"or pass --force to queue for an unregistered id."
    )


# --------------------------------------------------------------------------
# UDS direct delivery (Claude Code only)
# --------------------------------------------------------------------------
# Claude Code's host process listens on a per-pid unix socket and accepts inbound
# peer messages there. That path does not go through hooks at all: the host reads
# the line itself and starts a turn to handle it. So unlike hook delivery, it
# reaches a session that is sitting completely idle -- which is the one window
# hooks structurally cannot cover, since an idle session runs no hooks.
#
# Verified 2026-08-31 on claude-opus-5[1m]: a session parked at its prompt with no
# turn in flight received a message sent this way and answered it unprompted.
#
# This is the host's own channel, not a documented API. Everything below treats it
# as best-effort: if the socket is gone, the protocol shifts, or the write fails,
# the message stays queued and the hook path delivers it later. Direct delivery is
# an accelerator on top of the queue, never a replacement for it.
#
# Two properties are load-bearing:
#   * The kernel tells the receiver our pid (SO_PEERCRED), so we cannot spoof who
#     we are, and should not try to.
#   * `from-mode` inside the envelope is a *claim about the sender*. The receiver
#     holds a message for human review when a bypass-permissions session gets one
#     from a sender that did not attest its own mode. We fill it in only from what
#     the sending session actually reported about itself; a sender that reported
#     nothing sends no claim and its message is held, which is the correct outcome.
SOCK_DIRS = [
    Path(f"/run/user/{os.getuid()}/cc-socks"),
    Path("/tmp/cc-socks"),
]

# Claude's own parser for the envelope is strict: it re-renders what it parsed and
# compares, so an attribute carrying an out-of-range character silently voids the
# whole envelope rather than just that field. These mirror its character classes.
_SESSION_RE = re.compile(r"^[A-Za-z0-9_-]{1,80}$")
_LABEL_RE = re.compile(r'^[A-Za-z0-9%:_/.\\-]+$')

# The receiver closes a connection that sends no complete line, so the write has
# to be a single newline-terminated JSON object and nothing else.
DIRECT_CONNECT_TIMEOUT = float(os.environ.get("XMSG_DIRECT_TIMEOUT", "1.5"))


def sock_path_for(pid: int) -> str:
    """Where a Claude host with this pid would be listening, if it is."""
    for d in SOCK_DIRS:
        candidate = d / f"{pid}.sock"
        if candidate.exists():
            return str(candidate)
    return ""


def pid_start_time(pid: int) -> int | None:
    """Process start time in clock ticks, or None if the pid is gone.

    Paired with the pid this identifies a *process*, not just a slot in the pid
    table. Without it a recycled pid on a busy machine could take delivery of a
    message addressed to the session that used to hold it.
    """
    try:
        stat = Path(f"/proc/{pid}/stat").read_text()
    except OSError:
        return None
    # comm can contain spaces and parentheses, so field 22 is counted from the
    # last ')' rather than by splitting the whole line.
    tail = stat[stat.rfind(")") + 1 :].split()
    try:
        return int(tail[19])
    except (IndexError, ValueError):
        return None


def envelope(body: str, *, label: str, from_session: str, from_mode: str) -> str:
    """Build the `<cross-session-message>` element the host expects.

    Attributes are emitted in the host's own order and dropped individually when
    they would not survive its validation, because a rejected envelope is not a
    rejected attribute -- it degrades the whole message back to unattributed.

    A name the host's character class would reject (any non-ASCII title, which
    both hosts allow) falls back to the session's id prefix rather than being
    dropped: `from` is the address the receiver types to answer, and no address
    at all is worse than a less readable one. The full name stays in the outbox.
    """
    attrs = []
    if not (label and _LABEL_RE.match(label)) and from_session and _SESSION_RE.match(from_session):
        label = from_session[:8]
    if label and _LABEL_RE.match(label):
        attrs.append(f'from="{label}"')
    if from_session and _SESSION_RE.match(from_session):
        attrs.append(f'from-session="{from_session}"')
    if from_mode in ("bypass", "prompting"):
        attrs.append(f'from-mode="{from_mode}"')
    joined = (" " + " ".join(attrs)) if attrs else ""
    return f"<cross-session-message{joined}>\n{body}\n</cross-session-message>"


def claimed_mode(permission_mode: str) -> str:
    """Map a host-reported permission mode onto the two classes Claude compares.

    Returns "" when the sender reported nothing recognisable. That is not a
    fallback to a lenient default: an absent claim is what makes the receiver
    park the message for review, and inventing one here would be forging an
    attestation on the sender's behalf.
    """
    if permission_mode in ("bypassPermissions", "acceptEdits"):
        return "bypass" if permission_mode == "bypassPermissions" else "prompting"
    if permission_mode in ("default", "plan", "ask"):
        return "prompting"
    return ""


def direct_send(
    row: sqlite3.Row | dict[str, Any],
    body: str,
    *,
    label: str,
    from_session: str,
    from_mode: str,
) -> tuple[bool, str]:
    """Hand one message straight to a live Claude host. (delivered, detail).

    Definite connection failures return False. An uncertain write raises
    DeliveryUncertain so callers hold the row instead of duplicating delivery.
    """
    pid = row["pid"] if row["pid"] is not None else 0
    if not pid:
        return False, "no pid recorded for that session"
    if pid == os.getpid() or pid == os.getppid():
        return False, "refusing to deliver to the sending process"

    live_start = pid_start_time(int(pid))
    if live_start is None:
        return False, f"pid {pid} is gone"
    recorded = row["pid_start"]
    if recorded is not None and int(recorded) != live_start:
        # Same number, different process: the session that owned it has exited.
        return False, f"pid {pid} was recycled by another process"

    path = sock_path_for(int(pid)) or str(row["sock_path"] or "")
    if not path or not Path(path).exists():
        return False, f"no live socket for pid {pid}"

    version = receiver_version(int(pid), "claude")

    payload = {
        "type": "user",
        "from": label,
        "message": {
            "role": "user",
            "content": envelope(
                body, label=label, from_session=from_session, from_mode=from_mode
            ),
        },
    }
    sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    writing = False
    try:
        sock.settimeout(DIRECT_CONNECT_TIMEOUT)
        sock.connect(path)
        writing = True
        sock.sendall((json.dumps(payload, ensure_ascii=False) + "\n").encode())
        # The receiver reads the line asynchronously and answers nothing, so
        # there is nothing to wait for; shutdown() makes sure the bytes are on
        # their way before the socket goes away with this process.
        sock.shutdown(socket.SHUT_WR)
        return True, f"claude-uds accepted (receiver {version or 'version unknown'}; {path})"
    except OSError as exc:
        if writing:
            raise DeliveryUncertain(f"Claude UDS write outcome unknown ({type(exc).__name__})") from None
        return False, f"{type(exc).__name__}: {exc}"
    finally:
        sock.close()


# --------------------------------------------------------------------------
# Codex direct delivery
# --------------------------------------------------------------------------
# Older Codex versions expose `codex queue
# --thread <id> --message <text>` is a supported command that pushes a message
# into a running session. Verified 2026-08-31 on 0.151.0: an idle session picked
# the message up on its own and answered it.
#
# Two differences from the Claude path shape the code below.
#
# 1. It needs a rollout on disk, so a session that has never completed a turn is
#    rejected ("no rollout found"). Those sessions stay on the hook path.
# 2. The message arrives rendered as *user input* -- there is no peer envelope and
#    no ingress gate on that side. So the framing that tells the receiver this is
#    a peer talking has to be inside the text we pass, which is why this path
#    reuses render() rather than sending the bare body.
#
# The liveness check is the same as the Claude path and is not optional here: with
# a rollout present but the session gone, `codex queue` succeeds and files the
# message for whenever someone resumes that thread. That is not delivery, so
# marking the row delivered on the strength of its exit code would quietly lose
# the message. pid plus start time is what separates the two.
CODEX_QUEUE_TIMEOUT = float(os.environ.get("XMSG_CODEX_TIMEOUT", "20"))


def is_codex_daemon(pid: int) -> bool:
    """One daemon PID can own many threads, including completed ones."""
    try:
        args = Path(f"/proc/{pid}/cmdline").read_bytes().split(b"\0")
        return b"app-server" in args[1:3]
    except OSError:
        return False


def executable_version(executable: str) -> str:
    try:
        proc = subprocess.run([executable, "--version"], capture_output=True, text=True, timeout=3)
        value = version_tuple(proc.stdout) if proc.returncode == 0 else None
        return ".".join(map(str, value)) if value else ""
    except (OSError, subprocess.TimeoutExpired):
        return ""


def receiver_version(pid: int, tool: str) -> str:
    """Inspect the running executable, not the potentially newer PATH binary."""
    try:
        comm = Path(f"/proc/{pid}/comm").read_text().strip()
        if comm not in (tool, tool + ".exe"):
            return ""
        return executable_version(f"/proc/{pid}/exe")
    except OSError:
        return ""


def codex_sender_thread(from_tool: str, from_session: str) -> str:
    """Only an actual Codex ancestor may claim native Codex delegation."""
    actual = os.environ.get("CODEX_SESSION_ID") or os.environ.get("CODEX_THREAD_ID") or ""
    if from_tool != "codex" or not from_session or from_session != actual:
        return ""
    pid = host_pid()
    try:
        return from_session if Path(f"/proc/{pid}/comm").read_text().strip() in ("codex", "codex.exe") else ""
    except OSError:
        return ""


def running_codex_tui_version(home: Path) -> str:
    """Conservative minimum among live TUI processes using this CODEX_HOME.

    There is no supported thread-to-TUI-PID map. Native rendering is proven
    only when every matching client supports it. Per-send inode caching avoids
    repeatedly executing --version for several clients of the same release.
    """
    deadline = time.monotonic() + 3
    versions: dict[tuple[int, int], str] = {}
    found: list[tuple[int, int, int]] = []
    excluded = {"app-server", "exec", "queue", "mcp", "mcp-server", "doctor", "login", "logout",
                "features", "help", "--help", "--version", "-h", "-V", "cloud", "completion", "debug"}
    for proc in Path("/proc").glob("[0-9]*"):
        if time.monotonic() >= deadline:
            return ""
        try:
            if proc.stat().st_uid != os.getuid() or (proc / "comm").read_text().strip() not in ("codex", "codex.exe"):
                continue
        except OSError:
            continue
        try:
            args = (proc / "cmdline").read_bytes().decode(errors="replace").split("\0")
            if any(arg in excluded for arg in args[1:]):
                continue
            env = _parse_env_bytes((proc / "environ").read_bytes())
            candidate_home = Path(env.get("CODEX_HOME") or str(Path.home() / ".codex")).expanduser()
            if not candidate_home.is_absolute():
                candidate_home = Path(os.readlink(proc / "cwd")) / candidate_home
            if candidate_home.resolve() != home.resolve():
                continue
            stat = (proc / "exe").stat()
            inode = (stat.st_dev, stat.st_ino)
            if inode not in versions:
                versions[inode] = executable_version(str(proc / "exe"))
            parsed = version_tuple(versions[inode])
            if parsed is None:
                return ""
            found.append(parsed)
        except OSError:
            return ""
    return ".".join(map(str, min(found))) if found else ""


def codex_bin() -> str:
    """The codex executable to drive, or "" if there is none to find."""
    return os.environ.get("XMSG_CODEX_BIN", "") or (shutil.which("codex") or "")


def direct_send_codex(
    row: sqlite3.Row | dict[str, Any],
    rendered: str,
    *,
    to_session: str,
    from_session: str = "",
    from_tool: str = "",
    native_body: str | None = None,
) -> tuple[bool, str]:
    """Send raw body natively, or one-line source plus body on older routes."""
    pid = row["pid"] if row["pid"] is not None else 0
    if not pid:
        return False, "no pid recorded for that session"
    sender_threads = {from_session}
    if from_tool != "notification":
        sender_threads.update((os.environ.get("CODEX_SESSION_ID", ""), os.environ.get("CODEX_THREAD_ID", "")))
    if to_session and to_session in sender_threads:
        return False, "refusing direct delivery to the sending thread"

    live_start = pid_start_time(int(pid))
    if live_start is None:
        return False, f"pid {pid} is gone"
    recorded = row["pid_start"]
    if recorded is not None and int(recorded) != live_start:
        return False, f"pid {pid} was recycled by another process"

    # Connecting is a read-only capability probe until the exact loaded thread
    # and an accepted input route have been established. Shared PID != self.
    source = codex_sender_thread(from_tool, from_session)
    ok, detail = send_visible(
        CODEX_HOME, to_session, rendered, CODEX_QUEUE_TIMEOUT,
        source_thread=source, tui_version=running_codex_tui_version(CODEX_HOME) if source else "",
        native_text=native_body,
    )
    if ok:
        return ok, detail
    if is_codex_daemon(int(pid)):
        # A living daemon alone says nothing about this thread. Never enqueue
        # on its strength when membership/status discovery failed.
        return False, detail
    exe = codex_bin()
    if not exe:
        return False, "codex not found on PATH"

    peer_version = version_tuple(receiver_version(int(pid), "codex"))
    if peer_version is None or peer_version < MIN_CODEX_QUEUE:
        return False, "receiver version does not establish codex queue support"
    try:
        probe = subprocess.run([exe, "queue", "--help"], capture_output=True, text=True, timeout=3)
    except (OSError, subprocess.TimeoutExpired):
        return False, "codex queue capability probe failed"
    if probe.returncode or "--thread" not in probe.stdout or "--message" not in probe.stdout:
        return False, "installed codex CLI does not advertise queue --thread/--message"
    return invoke_codex_queue(exe, to_session, rendered)


def invoke_codex_queue(exe: str, to_session: str, rendered: str) -> tuple[bool, str]:
    """Invoke the supported Codex queue command after target resolution."""
    try:
        proc = subprocess.run(
            [exe, "queue", "--thread", to_session, "--message", rendered],
            capture_output=True,
            text=True,
            timeout=CODEX_QUEUE_TIMEOUT,
        )
    except subprocess.TimeoutExpired:
        raise DeliveryUncertain(f"codex queue timed out after {CODEX_QUEUE_TIMEOUT}s") from None
    except OSError as exc:
        return False, f"{type(exc).__name__}: {exc}"

    if proc.returncode != 0:
        detail = (proc.stderr or proc.stdout or "").strip().splitlines()
        diagnostic = detail[0] if detail else str(proc.returncode)
        if any(marker in diagnostic.lower() for marker in (
            "no rollout", "thread not found", "unrecognized subcommand", "unexpected argument",
        )):
            return False, f"codex queue failed: {diagnostic}"
        raise DeliveryUncertain(f"codex queue failed without a definite rejection (exit {proc.returncode})")
    return True, "codex queue accepted (next turn)"


# --------------------------------------------------------------------------
# sender side
# --------------------------------------------------------------------------
def sweep(conn: sqlite3.Connection, *, vacuum: bool = True) -> int:
    """Expire stale queued rows, delete long-finished ones, reclaim the space.

    A message is not kept forever: once delivered (or expired) and older than
    RETAIN_DELIVERED_SECONDS it is deleted outright. The retention window exists
    only so `xmsg outbox` can still answer "did it land" for a while.

    Returns the number of rows deleted.
    """
    t = now()
    conn.execute(
        "UPDATE messages SET expired_at = ? WHERE delivered_at IS NULL AND expired_at IS NULL AND expires_at < ?",
        (t, t),
    )
    cur = conn.execute(
        "DELETE FROM messages WHERE (delivered_at IS NOT NULL OR expired_at IS NOT NULL) "
        "AND COALESCE(delivered_at, expired_at) < ?",
        (t - RETAIN_DELIVERED_SECONDS,),
    )
    deleted = cur.rowcount if cur.rowcount and cur.rowcount > 0 else 0
    conn.execute("DELETE FROM peers WHERE last_seen_at < ?", (t - RETAIN_PEER_SECONDS,))
    conn.execute(
        "INSERT INTO meta (key, value) VALUES ('last_sweep_at', ?) "
        "ON CONFLICT(key) DO UPDATE SET value = excluded.value",
        (t,),
    )
    if vacuum:
        reclaim(conn)
    return deleted


def reclaim(conn: sqlite3.Connection) -> bool:
    """VACUUM once the freelist is big enough to be worth the rewrite.

    DELETE returns pages to SQLite's freelist but never shrinks the file, so a
    burst of traffic would leave the database permanently inflated at its high
    water mark. Checking freelist_count first keeps this off the common path:
    with nothing to reclaim it is two cheap pragmas and no rewrite. VACUUM needs
    the whole database, so it cannot run inside a transaction.
    """
    free = int(conn.execute("PRAGMA freelist_count").fetchone()[0])
    if free < VACUUM_FREE_PAGE_THRESHOLD:
        return False
    conn.execute("VACUUM")
    return True


def sweep_if_due(conn: sqlite3.Connection) -> bool:
    """The hook path's sweep: at most once per HOOK_SWEEP_INTERVAL_SECONDS.

    Without this, a machine that only ever *receives* would never sweep, since
    sweeping used to happen on send only - delivered rows would sit there for
    good. The claim is written before the work so that concurrent hooks (there
    are many) do not all sweep at once; losing the race means skipping, which
    is the right outcome for a path whose budget belongs to delivery.
    """
    t = now()
    conn.execute("BEGIN IMMEDIATE")
    try:
        row = conn.execute("SELECT value FROM meta WHERE key = 'last_sweep_at'").fetchone()
        last = int(row["value"]) if row is not None else 0
        due = (t - last) >= HOOK_SWEEP_INTERVAL_SECONDS
        if due:
            conn.execute(
                "INSERT INTO meta (key, value) VALUES ('last_sweep_at', ?) "
                "ON CONFLICT(key) DO UPDATE SET value = excluded.value",
                (t,),
            )
        conn.execute("COMMIT")
    except BaseException:
        conn.execute("ROLLBACK")
        raise
    if not due:
        return False
    # Outside the transaction: VACUUM cannot run inside one.
    sweep(conn)
    return True


def local_session_name(tool: str, session: str) -> str:
    """The sender's own name, as its host's local index records it.

    This is the same string `xmsg send <name>` resolves, which is the whole
    reason it belongs in the envelope's `from`: the receiver replies by copying
    that attribute back into a target. Read at send time rather than cached,
    because a session can be renamed at any point in its life.
    """
    if not session or not _SESSION_RE.match(session):
        # Also keeps a glob metacharacter out of the title lookup below.
        return ""
    if tool == "claude":
        try:
            paths = sorted(CLAUDE_PROJECTS.glob(f"*/{session}/custom-title.json"))
        except OSError:
            return ""
        for path in paths:
            try:
                value = json.loads(path.read_text(encoding="utf-8"))
            except (OSError, json.JSONDecodeError):
                continue
            name = str(value.get("customTitle") or "").strip() if isinstance(value, dict) else ""
            if name:
                return name
        return ""
    if tool == "codex":
        # One index row per rename; the last occurrence is the current name.
        name = ""
        for item in _jsonl_records(CODEX_SESSION_INDEX):
            if str(item.get("id") or "") == session:
                name = str(item.get("thread_name") or "").strip() or name
        return name
    return ""


def default_sender_label() -> tuple[str, str, str]:
    """(label, tool, session) for whoever is running `xmsg send`.

    An agent sending on its own behalf can identify itself via XMSG_FROM /
    XMSG_FROM_TOOL / XMSG_FROM_SESSION; a human at a shell gets user@host.

    With no XMSG_FROM the label is the sender's own session name, falling back
    to its id prefix -- both of which `resolve_target` accepts, so the receiver
    can answer whatever it was told. A `tool:id` composite resolves to nothing
    and is never produced here.

    ``--from`` and ``XMSG_FROM`` set only the *label*, never tool/session. The
    label is an address the receiver can type back, not an attestation;
    ``from_tool``/``from_session`` are what the receiver's framing calls
    attributed, so a sender cannot claim to be a session it is not by passing a
    prettier string.
    """
    tool = os.environ.get("XMSG_FROM_TOOL", "")
    codex_session = os.environ.get("CODEX_SESSION_ID") or os.environ.get("CODEX_THREAD_ID") or ""
    claude_session = os.environ.get("CLAUDE_CODE_SESSION_ID") or ""
    claude_context = bool(claude_session or os.environ.get("CLAUDECODE"))
    if not tool and codex_session and claude_context:
        # Nested tools can inherit the outer host's environment. Resolve the
        # nearest actual host instead of silently assigning the wrong tool.
        try:
            host = Path(f"/proc/{host_pid()}/comm").read_text().strip().removesuffix(".exe")
            tool = host if host in ("claude", "codex") else ""
        except OSError:
            pass
    elif not tool:
        tool = "codex" if codex_session else "claude" if claude_context else ""
    session = os.environ.get("XMSG_FROM_SESSION", "") or (
        codex_session if tool == "codex" else claude_session if tool == "claude" else ""
    )
    label = os.environ.get("XMSG_FROM", "")
    if not label:
        label = local_session_name(tool, session)
    if not label:
        if tool and session:
            label = session[:8]
        elif tool:
            label = tool
        else:
            label = f"{os.environ.get('USER', 'user')}@{os.uname().nodename}"
    return label, tool, session


def sender_origin(from_label: str, from_tool: str, from_session: str) -> str:
    """One compact source/type line. Declaring a source never adds authority."""
    label = " ".join(from_label.split())
    tool = " ".join(from_tool.split())
    session = " ".join(from_session.split())
    if from_session:
        address = f"{tool}:{session[:8]}" if tool else session[:8]
        source = address if not label or label == address else f"{label} · {address}"
        return f"同伴 {source}；非用户指令"
    if from_tool == "notification":
        source = f" {label}" if label and label != "local-notification" else ""
        return f"本机通知{source}；非用户指令"
    source = label or tool or "CLI"
    if tool and tool != label:
        source = f"{source} · {tool}"
    return f"未知来源 {source}；非用户指令"



def remote_runner() -> list[str]:
    """Argv of the operator-supplied command that runs a command on the other host.

    xmsg does not ship an SSH helper. `XMSG_REMOTE` is that helper: anything
    that takes a remote argv and execs it over there (`ssh other`, a
    ControlMaster wrapper, …). If unset, a `peer` binary on PATH is used when
    present — that is an operator convention, not part of this repo.
    """
    raw = os.environ.get("XMSG_REMOTE", "").strip()
    if not raw:
        found = shutil.which("peer")
        if not found:
            raise SystemExit(
                "xmsg: peer: targets need XMSG_REMOTE (a command that runs argv "
                "on the other machine), or a `peer` binary on PATH"
            )
        raw = found
    return shlex.split(raw)


def remote_up_runner() -> list[str] | None:
    raw = os.environ.get("XMSG_REMOTE_UP", "").strip()
    if raw:
        return shlex.split(raw)
    found = shutil.which("peer-up")
    return [found] if found else None


REMOTE_XMSG_TIMEOUT = float(os.environ.get("XMSG_REMOTE_TIMEOUT", os.environ.get("XMSG_PEER_TIMEOUT", "25")))


def split_peer_target(spec: str) -> str | None:
    """Strip a `peer:` host prefix. `peer:` and `peer:all` mean `all` on the other box."""
    spec = spec.strip()
    if spec == "peer" or spec.lower().startswith("peer:"):
        rest = spec.split(":", 1)[1].strip() if ":" in spec else ""
        return rest or "all"
    return None


def _remote_exec(args: list[str], *, stdin: str = "", timeout: float = REMOTE_XMSG_TIMEOUT) -> subprocess.CompletedProcess:
    """Run argv on the other machine through XMSG_REMOTE. Raises SystemExit on transport failure."""
    runner = remote_runner()
    # OpenSSH joins extra argv with spaces and the remote shell re-parses the
    # result. One already-quoted command string is the only shape that survives
    # both that join and a wrapper which execs ssh with "$@".
    command = " ".join(shlex.quote(a) for a in args)
    try:
        return subprocess.run(
            [*runner, command],
            input=stdin,
            capture_output=True,
            text=True,
            timeout=timeout,
        )
    except FileNotFoundError as exc:
        raise SystemExit(f"xmsg: cannot run XMSG_REMOTE {runner!r}: {exc}") from exc
    except subprocess.TimeoutExpired as exc:
        raise SystemExit(f"xmsg: remote xmsg timed out after {timeout:.0f}s") from exc


def ensure_remote_channel() -> None:
    """Best-effort: run XMSG_REMOTE_UP / `peer-up` so the runner has a live channel."""
    up = remote_up_runner()
    if not up:
        return
    try:
        subprocess.run(up, capture_output=True, text=True, timeout=45)
    except (OSError, subprocess.TimeoutExpired):
        return


def send_to_peer(spec: str, body: str, args: argparse.Namespace) -> int:
    """Run `xmsg send` on the other machine so delivery uses its UDS / sqlite."""
    ensure_remote_channel()
    label, tool, session = default_sender_label()
    if args.from_label:
        label = args.from_label
    remote = ["env", f"XMSG_FROM={label}"]
    if tool:
        remote.append(f"XMSG_FROM_TOOL={tool}")
    if session:
        remote.append(f"XMSG_FROM_SESSION={session}")
    remote.extend(["xmsg", "send", spec, "-"])
    if args.force:
        remote.append("--force")
    if args.no_direct:
        remote.append("--no-direct")
    if args.urgent:
        remote.append("--urgent")
    elif getattr(args, "priority", None) not in (None, DEFAULT_MESSAGE_PRIORITY):
        remote.extend(["--priority", str(args.priority)])
    if args.ttl is not None:
        remote.extend(["--ttl", str(args.ttl)])
    proc = _remote_exec(remote, stdin=body + "\n")
    if proc.stdout:
        sys.stdout.write(proc.stdout if proc.stdout.endswith("\n") else proc.stdout + "\n")
    if proc.stderr:
        sys.stderr.write(proc.stderr)
    if proc.returncode != 0:
        raise SystemExit("xmsg: remote send failed (is XMSG_REMOTE reachable?)")
    return 0


def list_peer(args: argparse.Namespace) -> int:
    ensure_remote_channel()
    remote = ["xmsg", "list"]
    if args.all:
        remote.append("--all")
    if args.json:
        remote.append("--json")
    proc = _remote_exec(remote)
    if proc.returncode != 0:
        if proc.stderr:
            sys.stderr.write(proc.stderr)
        raise SystemExit("xmsg: remote list failed (is XMSG_REMOTE reachable?)")
    sys.stdout.write(proc.stdout)
    return 0


def cmd_send(args: argparse.Namespace) -> int:
    body = args.text
    if body == "-" or (not body and not sys.stdin.isatty()):
        body = sys.stdin.read()
    body = (body or "").strip()
    if not body:
        raise SystemExit("xmsg: refusing to send an empty message")
    if len(body) > MAX_BODY_CHARS:
        raise SystemExit(f"xmsg: message is {len(body)} chars, limit is {MAX_BODY_CHARS}")

    remote_spec = split_peer_target(args.to)
    if remote_spec is not None:
        if getattr(args, "notification", False):
            raise SystemExit("xmsg: --notification 仅支持本机目标，不能用于 peer: 远端通知")
        return send_to_peer(remote_spec, body, args)

    conn = connect()
    try:
        sweep(conn)
        targets = resolve_target(conn, args.to, allow_unknown=args.force)
        label, tool, session = default_sender_label()
        if getattr(args, "notification", False):
            # This declares a message kind, not an identity or an authority.
            # Do not inherit a surrounding agent's session or permission claim.
            label, tool, session = os.environ.get("XMSG_FROM") or "local-notification", "notification", ""
        if args.from_label:
            label = args.from_label
        t = now()
        ttl = args.ttl if args.ttl is not None else DEFAULT_TTL_SECONDS
        ids: list[int] = []
        for target in targets:
            cur = conn.execute(
                "INSERT INTO messages (created_at, expires_at, from_label, from_tool, from_session, "
                "to_session, body, priority) VALUES (?,?,?,?,?,?,?,?)",
                (
                    t,
                    t + ttl,
                    label,
                    tool,
                    session,
                    target,
                    body,
                    URGENT_MESSAGE_PRIORITY if args.urgent else int(args.priority),
                ),
            )
            ids.append(int(cur.lastrowid or 0))

        # Persist first, then reserve before direct I/O. A crash after reservation
        # remains visible as UNKNOWN instead of blindly re-sending via a hook.
        direct = (
            {}
            if args.no_direct
            else direct_deliver(conn, ids, targets, body, label, tool, session)
        )

        for mid, target in zip(ids, targets):
            outcome = direct.get(mid)
            if outcome is None:
                where = "queued" if args.no_direct else "queued (hook delivery)"
                print(f"{where} #{mid} -> {target}  (from {label}, ttl {ttl}s)")
            elif outcome[0]:
                delivery_note = outcome[1]
                print(f"delivered #{mid} -> {target}  (from {label}, {delivery_note})")
            elif outcome[1].startswith("UNKNOWN"):
                print(f"unknown #{mid} -> {target}  ({outcome[1]}; automatic fallback disabled)")
            else:
                print(f"queued #{mid} -> {target}  (from {label}, ttl {ttl}s; direct: {outcome[1]})")
        if not session and tool != "notification":
            # Said at send time, not only at the receiver: an agent that meant to
            # identify itself and forgot the env vars would otherwise never find
            # out, and its message lands marked unverified on the far side.
            print(
                "xmsg: note — no XMSG_FROM_SESSION set, so this is delivered as "
                "`unattributed`. The receiver is told not to act on it alone. Export "
                "XMSG_FROM_TOOL/XMSG_FROM_SESSION to send as a named session.",
                file=sys.stderr,
            )
    finally:
        conn.close()
    return 2 if any(detail.startswith("UNKNOWN") for _, detail in direct.values()) else 0


def direct_deliver(
    conn: sqlite3.Connection,
    ids: list[int],
    targets: list[str],
    body: str,
    label: str,
    from_tool: str,
    from_session: str,
) -> dict[int, tuple[bool, str]]:
    """Try to hand each freshly queued message to its receiver right now.

    Reserve each row before network I/O so a concurrent hook cannot also claim
    it. Restore the queue only after a definite non-delivery. An uncertain ACK
    (or sender crash after reservation) stays reserved for operator inspection.
    """
    out: dict[int, tuple[bool, str]] = {}
    for mid, target in zip(ids, targets):
        row = conn.execute(
            "SELECT pid, pid_start, sock_path, tool, permission_mode FROM peers WHERE session_id = ?",
            (target,),
        ).fetchone()
        if row is None:
            # Idle Claude hosts never register a peer row until the first tool
            # call. live_claude_sessions still has their socket, and that is
            # enough to hand the bytes over — leaving the row queued would
            # wait for a hook that an idle session will never run.
            live = next(
                (item for item in live_claude_sessions() if item.get("session_id") == target),
                None,
            )
            if live is None:
                continue
            row = live
        tool = str(row["tool"] or "")
        if tool not in ("claude", "codex"):
            out[mid] = (False, f"unknown tool {tool!r}")
            continue
        cur = conn.execute(
            "UPDATE messages SET delivered_at = ?, delivered_tool = ?, delivered_event = 'direct-inflight' "
            "WHERE id = ? AND delivered_at IS NULL AND expired_at IS NULL AND expires_at >= ?",
            (now(), tool, mid, now()),
        )
        if not cur.rowcount:
            state = conn.execute("SELECT delivered_at, delivered_event FROM messages WHERE id = ?", (mid,)).fetchone()
            if state and state["delivered_event"] in ("direct-inflight", "direct-uncertain"):
                out[mid] = (False, "UNKNOWN: previous direct attempt is still held")
            elif state and state["delivered_at"] is not None:
                out[mid] = (True, "already claimed; no second delivery attempted")
            else:
                out[mid] = (False, "message expired or is no longer queued")
            continue
        try:
            ok, detail = _deliver_one(conn, row, mid, target, body, label, from_tool, from_session)
        except DeliveryUncertain as exc:
            conn.execute("UPDATE messages SET delivered_event = 'direct-uncertain' WHERE id = ?", (mid,))
            out[mid] = (False, f"UNKNOWN: {exc}")
            continue
        if ok:
            event = (detail.split()[0] if detail.startswith(("codex-turn-", "codex-delegated-"))
                     else "codex-queue" if tool == "codex" else "uds-direct")
            conn.execute("UPDATE messages SET delivered_event = ? WHERE id = ?", (event, mid))
        else:
            conn.execute(
                "UPDATE messages SET delivered_at = NULL, delivered_tool = NULL, delivered_event = NULL WHERE id = ?",
                (mid,),
            )
        out[mid] = (ok, detail)
    return out


def _deliver_one(
    conn: sqlite3.Connection, row: Any, mid: int, target: str, body: str,
    label: str, from_tool: str, from_session: str,
) -> tuple[bool, str]:
    if row["tool"] == "codex":
        # Visible user input must still identify its actual peer source.
        rendered = render([{
            "id": mid, "created_at": now(), "from_label": label,
            "from_tool": from_tool, "from_session": from_session, "body": body,
        }], direct=True)
        return direct_send_codex(
            row, rendered, to_session=target, from_session=from_session, from_tool=from_tool, native_body=body,
        )
    mode = sender_claimed_mode(conn, from_session)
    # Claude renders its own source header from this native envelope label, and a
    # receiver answers by copying that string back into a target. So it carries
    # the sender's name alone: a `tool:label` composite resolves to no session,
    # which is what made replies to xmsg unanswerable. Which tool sent it stays
    # available from `from-session` and the outbox, neither of which is an
    # address the receiver has to retype.
    return direct_send(row, body, label=label, from_session=from_session, from_mode=mode)


def sender_claimed_mode(conn: sqlite3.Connection, from_session: str) -> str:
    """The sending session's own permission class, as it last reported it.

    Read from the sender's peer row rather than from this process: `xmsg send`
    runs as a child of the sending session, and the mode is a property of that
    session, not of the shell. A sender with no row -- a human at a terminal, a
    cron job -- claims nothing, and the receiver will park the message for review
    instead of acting on it. That is the intended outcome, not a gap to close.
    """
    if not from_session:
        return ""
    row = conn.execute(
        "SELECT permission_mode FROM peers WHERE session_id = ?", (from_session,)
    ).fetchone()
    if row is None:
        return ""
    return claimed_mode(str(row["permission_mode"] or ""))


def cmd_list(args: argparse.Namespace) -> int:
    if getattr(args, "peer", False):
        return list_peer(args)
    conn = connect()
    try:
        cutoff = 0 if args.all else now() - PEER_STALE_SECONDS
        pending = {
            str(row["to_session"]): int(row["queued"])
            for row in conn.execute(
                "SELECT to_session, COUNT(*) AS queued FROM messages "
                "WHERE delivered_at IS NULL AND expired_at IS NULL GROUP BY to_session"
            )
        }
        codex_pending: dict[str, int] = {}
        for row in codex_queue_rows():
            sid = str(row["thread_id"])
            codex_pending[sid] = codex_pending.get(sid, 0) + 1
        rows: list[dict[str, Any]] = []
        discovered = discover_sessions(conn)
        loaded = (loaded_thread_ids(CODEX_HOME) if any(
            r.get("tool") == "codex" and r.get("pid") and is_codex_daemon(int(r["pid"]))
            for r in discovered
        ) else set())
        for row in discovered:
            sid = str(row["session_id"])
            row["queued"] = pending.get(sid, 0)
            row["codex_queued"] = codex_pending.get(sid, 0)
            row["direct"] = reachable_now(row, loaded=loaded) if row.get("tool") in ("claude", "codex") else False
            recent = int(row.get("last_seen_at") or row.get("updated_at") or 0) >= cutoff
            if args.all or recent or row["queued"] or row["codex_queued"]:
                rows.append(row)
        rows.sort(key=lambda row: int(row.get("last_seen_at") or row.get("updated_at") or 0), reverse=True)
        if args.json:
            print(json.dumps(rows, ensure_ascii=False, indent=2))
            return 0
        if not rows:
            print("no sessions found (xmsg peers and local Codex/Claude indexes are empty or stale)")
            return 0
        print(f"{'NAME':<18} {'SESSION':<38} {'TOOL':<7} {'SEEN':>5} {'Q':>3} {'CODEX':>5} {'DIRECT':<6}  CWD")
        for r in rows:
            print(
                f"{session_name(r)[:18]:<18} {r['session_id']:<38} {r.get('tool', '-'): <7} "
                f"{ago(int(r.get('last_seen_at') or r.get('updated_at') or now())):>5} "
                f"{r['queued']:>3} {r['codex_queued']:>5} {'yes' if r['direct'] else '-':<6}  {r.get('cwd', '')}"
            )
    finally:
        conn.close()
    return 0


def cmd_queue(args: argparse.Namespace) -> int:
    """Show both xmsg's hook queue and Codex's official next-turn queue."""
    conn = connect()
    try:
        names = _queue_name_map(conn)
        requested = (args.thread or args.name or "").strip()
        allowed: set[str] | None = None
        if requested:
            allowed = set(resolve_target(conn, requested, allow_unknown=False))
        records: list[dict[str, Any]] = []
        where = "delivered_at IS NULL AND expired_at IS NULL"
        for row in conn.execute(f"SELECT * FROM messages WHERE {where} ORDER BY priority DESC, id"):
            if allowed is not None and row["to_session"] not in allowed:
                continue
            item = dict(row)
            item.update(
                {
                    "kind": "xmsg",
                    "source": "xmsg",
                    "state": "hook-next-tool",
                    "name": names.get(str(row["to_session"]), ""),
                    "summary": str(row["body"] or "").replace("\n", " ").strip()[:160],
                }
            )
            records.append(item)
        for item in codex_queue_rows():
            if allowed is not None and item["thread_id"] not in allowed:
                continue
            item["kind"] = "codex"
            item["name"] = names.get(str(item["thread_id"]), "")
            records.append(item)
        records.sort(key=lambda row: (int(row.get("queue_order", row.get("priority", 0))), int(row.get("created_at", row.get("created_at_ms", 0)))))
        records = records[: max(0, int(args.limit))]
        if args.json:
            print(json.dumps(records, ensure_ascii=False, indent=2))
            return 0
        if not records:
            print("queue is empty")
            return 0
        print(f"{'SOURCE':<7} {'STATE':<16} {'NAME':<18} {'THREAD/TO':<38} SUMMARY")
        for row in records:
            target = str(row.get("thread_id") or row.get("to_session") or "")
            print(
                f"{str(row.get('source', '')):<7} {str(row.get('state', '')):<16} "
                f"{str(row.get('name', ''))[:18]:<18} {target:<38} {str(row.get('summary', ''))}"
            )
    finally:
        conn.close()
    return 0


def cmd_find(args: argparse.Namespace) -> int:
    """Resolve one session id/name for shell scripts."""
    conn = connect()
    try:
        discovered = discover_sessions(conn)
        if args.query:
            # resolve_target enforces the documented order and ambiguity guard;
            # use the resulting ids to return the richer discovery records.
            ids = resolve_target(conn, args.query, allow_unknown=False)
            rows = [row for row in discovered if row["session_id"] in ids]
        else:
            rows = discovered
            rows.sort(
                key=lambda row: int(row.get("last_seen_at") or row.get("updated_at") or 0),
                reverse=True,
            )
        if args.json:
            print(json.dumps(rows, ensure_ascii=False, indent=2))
            return 0
        if not rows:
            raise SystemExit(f"xmsg: no session matches {args.query!r}") if args.query else SystemExit(
                "xmsg: no discovered sessions"
            )
        if args.ids_only:
            for row in rows:
                print(row["session_id"])
            return 0
        print(f"{'NAME':<24} {'SESSION':<38} {'TOOL':<7} SOURCE")
        for row in rows:
            print(
                f"{session_name(row)[:24]:<24} {row['session_id']:<38} "
                f"{row.get('tool', '-'):<7} {row.get('source', '-')}"
            )
    finally:
        conn.close()
    return 0


def reachable_now(row: sqlite3.Row | dict[str, Any], *, loaded: set[str] | None = None) -> bool:
    """Whether direct delivery would find a live host for this peer right now.

    Deliberately re-derives the socket from the live pid instead of trusting the
    recorded path: a stale row and an exited process look identical otherwise,
    and a socket file outlives its owner.
    """
    try:
        pid = row["pid"]
        tool = row["tool"]
    except (KeyError, IndexError):
        return False
    if tool not in ("claude", "codex") or not pid:
        return False
    live = pid_start_time(int(pid))
    if live is None:
        return False
    recorded = row["pid_start"] if "pid_start" in row.keys() else None
    if recorded is not None and int(recorded) != live:
        return False
    if tool == "codex":
        if is_codex_daemon(int(pid)):
            targets = loaded if loaded is not None else loaded_thread_ids(CODEX_HOME)
            return "session_id" in row.keys() and row["session_id"] in targets
        # No socket to look for: `codex queue` addresses the session by id. A live
        # process is as much as can be checked from here -- whether a rollout
        # exists is only knowable by asking codex, which is the delivery itself.
        return bool(codex_bin())
    return bool(sock_path_for(int(pid)))


def cmd_outbox(args: argparse.Namespace) -> int:
    conn = connect()
    try:
        where = []
        params: list[Any] = []
        if not args.all:
            where.append("(delivered_at IS NULL AND expired_at IS NULL "
                         "OR delivered_event IN ('direct-inflight','direct-uncertain'))")
        if args.to:
            where.append("to_session LIKE ?")
            params.append(args.to + "%")
        clause = ("WHERE " + " AND ".join(where)) if where else ""
        rows = conn.execute(
            f"SELECT * FROM messages {clause} ORDER BY id DESC LIMIT ?", (*params, args.limit)
        ).fetchall()
        if args.json:
            print(json.dumps([dict(r) for r in rows], ensure_ascii=False, indent=2))
            return 0
        if not rows:
            print("nothing queued")
            return 0
        for r in rows:
            if r["delivered_event"] in ("direct-inflight", "direct-uncertain"):
                state = "UNKNOWN delivery outcome; held to prevent duplicate delivery"
            elif r["delivered_at"]:
                state = f"delivered {ago(r['delivered_at'])} ago via {r['delivered_tool']}/{r['delivered_event']}"
            elif r["expired_at"]:
                state = f"EXPIRED undelivered after {r['expires_at'] - r['created_at']}s"
            else:
                left = r["expires_at"] - now()
                state = f"queued, {left}s of ttl left" if left > 0 else "queued, ttl exhausted"
            first = r["body"].splitlines()[0] if r["body"] else ""
            attribution = ("  (本机自动提醒)" if r["from_tool"] == "notification" and not r["from_session"]
                           else "" if r["from_session"] else "  (unattributed)")
            print(f"#{r['id']}  {r['from_label']}{attribution} -> {r['to_session'][:8]}  [{state}]")
            print(f"      {first[:100]}")
    finally:
        conn.close()
    return 0


def cmd_cancel(args: argparse.Namespace) -> int:
    conn = connect()
    try:
        cur = conn.execute(
            "UPDATE messages SET expired_at = ? WHERE id = ? AND delivered_at IS NULL AND expired_at IS NULL",
            (now(), args.id),
        )
        if cur.rowcount:
            print(f"cancelled #{args.id}")
        else:
            print(f"#{args.id} is not cancellable (already delivered, expired, or absent)")
    finally:
        conn.close()
    return 0


# --------------------------------------------------------------------------
# receiver side (hook)
# --------------------------------------------------------------------------
def claim(conn: sqlite3.Connection, session_id: str, tool: str, event: str) -> list[sqlite3.Row]:
    """Atomically take ownership of this session's queued messages.

    PreToolUse fires many times per turn, and nothing stops two of them from
    overlapping, so the read and the mark have to be one step. BEGIN IMMEDIATE
    takes the write lock up front and `UPDATE ... RETURNING` marks and returns
    in a single statement: a second caller either waits and finds nothing, or
    fails the lock and injects nothing. Either way each row goes out once.
    """
    t = now()
    conn.execute("BEGIN IMMEDIATE")
    try:
        rows = conn.execute(
            "UPDATE messages SET delivered_at = ?, delivered_tool = ?, delivered_event = ? "
            "WHERE id IN ("
            "  SELECT id FROM messages"
            "   WHERE to_session = ? AND delivered_at IS NULL AND expired_at IS NULL AND expires_at >= ?"
            "   ORDER BY priority DESC, id LIMIT ?"
            ") RETURNING id, created_at, from_label, from_tool, from_session, body, priority",
            (t, tool, event, session_id, t, MAX_MESSAGES_PER_INJECT),
        ).fetchall()
        conn.execute("COMMIT")
    except BaseException:
        conn.execute("ROLLBACK")
        raise
    # SQLite does not promise that UPDATE ... RETURNING preserves the order of
    # its subquery.  Re-sort after the atomic claim so a high-priority message
    # is also rendered first to the receiver.
    return sorted(rows, key=lambda row: (-int(row["priority"] or 0), int(row["id"])))


def host_pid() -> int:
    """The agent host process this hook is running under.

    Walked rather than taken from getppid(): the hook runs as
    `sh -c` under the host, so the immediate parent is a shell. Stops at the
    first ancestor whose name matches a known host binary.
    """
    hosts = {"claude", "claude.exe", "codex", "codex.exe"}
    pid = os.getppid()
    for _ in range(12):
        if pid <= 1:
            return 0
        try:
            comm = Path(f"/proc/{pid}/comm").read_text().strip()
            stat = Path(f"/proc/{pid}/stat").read_text()
        except OSError:
            return 0
        if comm in hosts:
            return pid
        tail = stat[stat.rfind(")") + 1 :].split()
        try:
            pid = int(tail[1])
        except (IndexError, ValueError):
            return 0
    return 0


def _register_peer(conn: sqlite3.Connection, payload: dict[str, Any], tool: str, delivered: int) -> None:
    """Record this session as a delivery target, with its direct-delivery coordinates.

    Beyond keeping the session addressable, this is where the socket path, the
    owning pid (plus its start time, so a recycled pid cannot inherit someone
    else's mail) and the session's own permission mode get captured. All three
    come from the receiver describing itself; a sender never supplies them.
    """
    session_id = str(payload.get("session_id") or "")
    if not session_id:
        return
    t = now()
    pid = host_pid()
    start = pid_start_time(pid) if pid else None
    sock = sock_path_for(pid) if pid else ""
    mode = str(payload.get("permission_mode") or "")
    conn.execute(
        "INSERT INTO peers (session_id, tool, cwd, model, label, pid, pid_start, sock_path, "
        "permission_mode, first_seen_at, last_seen_at, injected_count) "
        "VALUES (?,?,?,?,?,?,?,?,?,?,?,?) "
        "ON CONFLICT(session_id) DO UPDATE SET "
        "  tool = excluded.tool,"
        "  cwd = CASE WHEN excluded.cwd <> '' THEN excluded.cwd ELSE peers.cwd END,"
        "  model = CASE WHEN excluded.model <> '' THEN excluded.model ELSE peers.model END,"
        "  pid = CASE WHEN excluded.pid IS NOT NULL THEN excluded.pid ELSE peers.pid END,"
        "  pid_start = CASE WHEN excluded.pid_start IS NOT NULL THEN excluded.pid_start ELSE peers.pid_start END,"
        "  sock_path = CASE WHEN excluded.sock_path <> '' THEN excluded.sock_path ELSE peers.sock_path END,"
        "  permission_mode = CASE WHEN excluded.permission_mode <> '' "
        "                         THEN excluded.permission_mode ELSE peers.permission_mode END,"
        "  last_seen_at = excluded.last_seen_at,"
        "  injected_count = peers.injected_count + ?",
        (
            session_id,
            tool,
            str(payload.get("cwd") or ""),
            str(payload.get("model") or ""),
            "",
            pid or None,
            start,
            sock,
            mode,
            t,
            t,
            delivered,
            delivered,
        ),
    )


def render(rows: list[sqlite3.Row] | list[dict[str, Any]], *, direct: bool = False) -> str:
    """Same thin fallback for hooks and user-input APIs; diagnostics stay in DB."""
    return "\n\n".join(
        f'[{sender_origin(r["from_label"], r["from_tool"], r["from_session"])}]\n{r["body"]}'
        for r in rows
    )


def hook_body(tool: str, raw: str) -> str:
    """The whole receiver path, minus the fail-open wrapper. Returns stdout.

    Two output shapes, because the two events that carry a delivery are read
    differently by the host:

      * PreToolUse -> hookSpecificOutput.additionalContext, spliced in before
        the tool call the turn was about to make.
      * Stop -> {"decision": "block", "reason": ...}. At Stop the turn has no
        further tool call to attach to, so additionalContext has nowhere to
        land; `block` is the only output that reaches the model, and it does so
        by restarting the turn with `reason` as the new context. That makes the
        moment a session goes idle a delivery window too - see WINDOWS below.
    """
    payload = json.loads(raw) if raw.strip() else {}
    if not isinstance(payload, dict):
        return ""
    event = str(payload.get("hook_event_name") or "PreToolUse")
    session_id = str(payload.get("session_id") or "")
    if not session_id:
        return ""

    # The turn we would restart is itself one the Stop hook already restarted.
    # Delivering again here is how you get an unbreakable block loop, so this
    # guard is load-bearing, not defensive. Claim nothing, register nothing.
    if event == "Stop" and payload.get("stop_hook_active"):
        return ""

    conn = connect()
    try:
        rows = claim(conn, session_id, tool, event)
        _register_peer(conn, payload, tool, len(rows))
        # Housekeeping, throttled to roughly hourly and never at the expense of
        # the delivery above: a machine that only receives would otherwise never
        # sweep, and its delivered rows would accumulate without bound. Failing
        # here must not cost the messages already claimed.
        try:
            sweep_if_due(conn)
        except BaseException:
            pass
    finally:
        conn.close()

    if not rows:
        return ""
    if event == "Stop":
        return json.dumps(
            {"decision": "block", "reason": render(rows)},
            ensure_ascii=False,
        )
    return json.dumps(
        {
            "hookSpecificOutput": {
                "hookEventName": event,
                "additionalContext": render(rows),
            }
        },
        ensure_ascii=False,
    )


def cmd_hook(args: argparse.Namespace) -> int:
    """PreToolUse entry point. Never fails loudly, never blocks the turn.

    XMSG_NO_FAILOPEN=1 strips the safety net. That exists so the fail-open
    guarantee can be falsified rather than asserted: with it set, a broken
    database or malformed payload really does propagate.
    """
    strict = os.environ.get("XMSG_NO_FAILOPEN") == "1"
    try:
        raw = sys.stdin.read()
    except BaseException:
        if strict:
            raise
        return 0
    try:
        out = hook_body(args.tool, raw)
    except BaseException:
        if strict:
            raise
        return 0
    if out:
        sys.stdout.write(out)
    return 0


# --------------------------------------------------------------------------
def cmd_doctor(args: argparse.Namespace) -> int:
    print(f"db          {DB_PATH}  ({'exists' if DB_PATH.exists() else 'MISSING - created on first use'})")
    print(f"ttl         {DEFAULT_TTL_SECONDS}s")
    print(f"peer stale  {PEER_STALE_SECONDS}s")
    print(f"retain      {RETAIN_DELIVERED_SECONDS}s delivered / {RETAIN_PEER_SECONDS}s peers")
    print(f"python3     {shutil.which('python3')}")
    socks = [str(d) for d in SOCK_DIRS if d.is_dir()]
    print(f"sock dirs   {', '.join(socks) if socks else 'none found (direct delivery unavailable)'}")
    conn = connect()
    try:
        size_before = DB_PATH.stat().st_size if DB_PATH.exists() else 0
        # doctor is a good moment to do the housekeeping unconditionally: it is
        # explicitly invoked, never on a hot path, and it reports what it did.
        deleted = sweep(conn)
        peers = conn.execute("SELECT COUNT(*) c FROM peers").fetchone()["c"]
        live = conn.execute(
            "SELECT COUNT(*) c FROM peers WHERE last_seen_at >= ?", (now() - PEER_STALE_SECONDS,)
        ).fetchone()["c"]
        queued = conn.execute(
            "SELECT COUNT(*) c FROM messages WHERE delivered_at IS NULL AND expired_at IS NULL"
        ).fetchone()["c"]
        kept = conn.execute(
            "SELECT COUNT(*) c FROM messages WHERE delivered_at IS NOT NULL OR expired_at IS NOT NULL"
        ).fetchone()["c"]
        free = int(conn.execute("PRAGMA freelist_count").fetchone()[0])
        row = conn.execute("SELECT value FROM meta WHERE key = 'last_sweep_at'").fetchone()
        reachable = sum(
            1
            for r in conn.execute(
                "SELECT pid, pid_start, tool FROM peers WHERE last_seen_at >= ?",
                (now() - PEER_STALE_SECONDS,),
            )
            if reachable_now(r)
        )
        print(f"peers       {live} live / {peers} known ({reachable} reachable by direct delivery)")
        print(f"queued      {queued}")
        print(f"retained    {kept} finished row(s) awaiting deletion")
        size = DB_PATH.stat().st_size if DB_PATH.exists() else 0
        note = f"  (was {size_before}, reclaimed)" if size < size_before else ""
        print(f"db size     {size} bytes, {free} free page(s){note}")
        if deleted:
            print(f"swept       deleted {deleted} row(s) past retention")
        if row is not None:
            print(f"last sweep  {ago(int(row['value']))} ago")
    finally:
        conn.close()
    return 0


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(
        prog="xmsg",
        description="Push a message into another agent session's next tool call.",
    )
    sub = p.add_subparsers(dest="cmd", required=True)

    s = sub.add_parser("send", help="queue a message for a session")
    s.add_argument(
        "to",
        help="session id, unique prefix, 'all', or 'peer:<same>' for the other machine",
    )
    s.add_argument("text", nargs="?", default="", help="message body ('-' or omit to read stdin)")
    s.add_argument("--from", dest="from_label", default="",
                   help="override the sender label (the address the receiver replies to; "
                        "defaults to this session's own name)")
    s.add_argument(
        "--notification", action="store_true",
        help="declare a local automated reminder, without inheriting agent identity or adding authority (local targets only)",
    )
    s.add_argument("--ttl", type=int, default=None, help=f"seconds before giving up (default {DEFAULT_TTL_SECONDS})")
    priority = s.add_mutually_exclusive_group()
    priority.add_argument(
        "--urgent",
        action="store_true",
        help="put ahead of ordinary xmsg hook messages (does not change Codex's official queue)",
    )
    priority.add_argument(
        "--priority",
        type=int,
        default=DEFAULT_MESSAGE_PRIORITY,
        help="xmsg hook priority (higher first; default 0)",
    )
    s.add_argument("--force", action="store_true", help="allow an unregistered session id")
    s.add_argument(
        "--no-direct",
        action="store_true",
        help="skip direct delivery; queue for the receiver's next hook instead",
    )
    s.set_defaults(fn=cmd_send)

    s = sub.add_parser("queue", help="show xmsg and Codex next-turn queues together")
    s.add_argument("--thread", default="", help="filter by complete session id, prefix, or custom name")
    s.add_argument("--name", default="", help="alias for --thread, useful for scripts")
    s.add_argument("--limit", type=int, default=50)
    s.add_argument("--json", action="store_true")
    s.set_defaults(fn=cmd_queue)

    s = sub.add_parser("find", help="find sessions by custom name or id")
    s.add_argument("query", nargs="?", default="", help="exact session id, custom name, or id prefix")
    s.add_argument("--ids-only", action="store_true", help="print only complete session ids")
    s.add_argument("--json", action="store_true")
    s.set_defaults(fn=cmd_find)

    s = sub.add_parser("list", help="list sessions available as delivery targets")
    s.add_argument("--all", action="store_true", help="include sessions that have gone quiet")
    s.add_argument("--json", action="store_true")
    s.add_argument("--peer", action="store_true", help="list sessions on the other machine")
    s.set_defaults(fn=cmd_list)

    s = sub.add_parser("outbox", help="show queued / recently delivered messages")
    s.add_argument("--all", action="store_true", help="include delivered and expired")
    s.add_argument("--to", default="", help="filter by target session prefix")
    s.add_argument("--limit", type=int, default=20)
    s.add_argument("--json", action="store_true")
    s.set_defaults(fn=cmd_outbox)

    s = sub.add_parser("cancel", help="withdraw a message that has not been delivered yet")
    s.add_argument("id", type=int)
    s.set_defaults(fn=cmd_cancel)

    s = sub.add_parser("hook", help="PreToolUse entry point (reads payload on stdin)")
    s.add_argument("--tool", required=True, help="claude | codex")
    s.set_defaults(fn=cmd_hook)

    s = sub.add_parser("doctor", help="show configuration and queue health")
    s.set_defaults(fn=cmd_doctor)

    args = p.parse_args(argv)
    return int(args.fn(args))


if __name__ == "__main__":
    sys.exit(main())
