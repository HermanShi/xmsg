"""Visible delivery to an existing Codex daemon, without resuming threads.

Only Python's standard library is required. Transport is shared lazily with
codex_history because that module itself imports xmsg for discovery.
"""
from __future__ import annotations

import re
from html import escape
from pathlib import Path
from typing import Any

# First stable tags containing every required protocol field. See README.
MIN_DAEMON_DELIVERY = (0, 145, 0)
MIN_CODEX_QUEUE = (0, 149, 0)
MIN_DELEGATED_TOOL_OUTPUT = (0, 151, 0)


def version_tuple(value: str) -> tuple[int, int, int] | None:
    match = re.search(r"(?<!\d)(\d+)\.(\d+)\.(\d+)(?!\d)", value)
    if match and value[match.end():].startswith("-"):
        return None  # A prerelease is not evidence of stable API availability.
    return tuple(map(int, match.groups())) if match else None


class DeliveryUncertain(Exception):
    """The write may have succeeded; automatic fallback could duplicate it."""


class RpcRejected(Exception):
    """A matching JSON-RPC error explicitly rejected this request."""


def rpc_call(client: Any, method: str, params: dict[str, Any]) -> dict[str, Any]:
    client.counter += 1
    request_id = client.counter
    client.send({"id": request_id, "method": method, "params": params})
    while True:
        message = client.receive()
        if "method" in message:
            if "id" in message:
                # A messaging client never handles permissions on another
                # client's behalf. Do not approve or change its configuration.
                client.send({"id": message["id"], "error": {
                    "code": -32601, "message": "Messaging client does not handle approvals",
                }})
            continue
        if message.get("id") != request_id:
            continue
        if "error" in message:
            raise RpcRejected(f"app-server {method} rejected")
        if not isinstance(message.get("result"), dict):
            raise ValueError("invalid app-server result")
        return message["result"]


def loaded_thread_ids(home: Path, timeout: float = 3) -> set[str]:
    """Read one daemon snapshot for list output; unavailable means unproven."""
    from codex_history import RpcClient
    client = None
    try:
        client = RpcClient("", home, timeout=timeout)
        rpc_call(client, "initialize", {"clientInfo": {"name": "xmsg", "version": "0.2.0"}})
        client.send({"method": "initialized", "params": {}})
        found: set[str] = set()
        cursor = None
        seen: set[str] = set()
        while True:
            page = rpc_call(client, "thread/loaded/list", {"limit": 100, "cursor": cursor})
            found.update(page["data"])
            cursor = page.get("nextCursor")
            if not cursor or cursor in seen:
                return found
            seen.add(cursor)
    except Exception:
        return set()
    finally:
        if client is not None:
            client.close()


def send_visible(
    home: Path, target: str, text: str, timeout: float, *, source_thread: str = "", tui_version: str = "",
    native_text: str | None = None,
) -> tuple[bool, str]:
    """Return a definite verdict or raise DeliveryUncertain after a write.

    Read-only calls establish the running server version, target membership,
    direct-input capability and active turn. No daemon/thread is started or
    resumed. Failed discovery is safe to fall back from.
    """
    from codex_history import RpcClient

    client = None
    try:
        client = RpcClient("", home, timeout=timeout)
        info = rpc_call(client, "initialize", {
            "clientInfo": {"name": "xmsg", "version": "0.2.0"},
            "capabilities": {"experimentalApi": True},
        })
        client.send({"method": "initialized", "params": {}})
        version = version_tuple(str(info.get("userAgent", "")))
        if version is None or version < MIN_DAEMON_DELIVERY:
            return False, "app-server version is outside the verified delivery baseline"
        cursor = None
        seen: set[str] = set()
        loaded: set[str] = set()
        while True:
            page = rpc_call(client, "thread/loaded/list", {"limit": 100, "cursor": cursor})
            loaded.update(page.get("data", []))
            cursor = page.get("nextCursor")
            if not cursor or cursor in seen:
                break
            seen.add(cursor)
        if target not in loaded:
            return False, "thread is not loaded in this app-server"
        thread = rpc_call(client, "thread/read", {"threadId": target, "includeTurns": False})["thread"]
        if thread.get("canAcceptDirectInput") is not True:
            return False, "thread does not accept direct input"
        status = thread.get("status", {}).get("type")
        # thread.cliVersion is its creation version, not the attached TUI.
        client_version = version_tuple(tui_version)
        native_peer = bool(
            source_thread and source_thread != target and source_thread in loaded
            and version >= MIN_DELEGATED_TOOL_OUTPUT
            and client_version is not None and client_version >= MIN_DELEGATED_TOOL_OUTPUT
        )
        params: dict[str, Any] = {
            "threadId": target,
            "input": [{"type": "text", "text": text, "text_elements": []}],
        }
        if status not in ("active", "idle"):
            return False, "thread status is not ready for visible delivery"
        if native_peer:
            # This is Codex's recognized delegation protocol, not an xmsg MCP
            # tool invocation. Use the real loaded source and escape both fields.
            params["input"] = []
            params["toolOutput"] = {
                "name": "send_message_to_thread", "namespace": "codex_tui",
                "output": ("<codex_delegation>\n"
                           f"  <source_thread_id>{escape(source_thread, quote=False)}</source_thread_id>\n"
                           f"  <input>{escape(native_text if native_text is not None else text, quote=False)}</input>\n"
                           "</codex_delegation>"),
            }
            method = "turn/start"
        elif status == "active":
            # Read only the newest turn, never a whole conversation transcript.
            page = rpc_call(client, "thread/turns/list", {
                "threadId": target, "limit": 1, "sortDirection": "desc",
            })
            turns = page.get("data", [])
            if not turns or turns[0].get("status") != "inProgress" or not turns[0].get("id"):
                return False, "active turn changed during discovery"
            params["expectedTurnId"] = turns[0]["id"]
            method = "turn/steer"
        elif status == "idle":
            method = "turn/start"
        try:
            rpc_call(client, method, params)
        except RpcRejected:
            return False, f"app-server {method} rejected; safe to fall back"
        except Exception as exc:
            # The frame may already have reached the daemon. Never guess that
            # a missing ACK means no delivery, including partial send failures.
            raise DeliveryUncertain(f"codex {method} acknowledgement unknown ({type(exc).__name__})") from None
        event = "codex-delegated-tool-output" if native_peer else "codex-" + method.replace("/", "-")
        return True, f"{event} accepted (visible message)"
    except DeliveryUncertain:
        raise
    except Exception as exc:
        return False, f"app-server discovery unavailable ({type(exc).__name__})"
    finally:
        if client is not None:
            client.close()
