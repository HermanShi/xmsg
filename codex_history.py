#!/usr/bin/env python3
"""Cross-provider Codex history picker and text handoff exporter."""

from __future__ import annotations

import argparse
import base64
import contextlib
import hashlib
import json
import os
import shlex
import shutil
import socket
import sqlite3
import struct
import subprocess
import sys
import time
import tomllib
import unicodedata
import uuid
from datetime import datetime
from pathlib import Path
from typing import Any

import xmsg


class HistoryError(Exception):
    pass


INTERACTIVE_SOURCES = ("cli", "vscode")
ALL_SOURCES = (*INTERACTIVE_SOURCES, "exec", "appServer", "subAgent", "subAgentReview",
               "subAgentCompact", "subAgentThreadSpawn", "subAgentOther", "unknown")
FIELDS = ("id", "name", "title", "model_provider", "model", "cwd", "updated_at",
          "source", "archived", "rollout_path")
REQUIRED_FIELDS = {"id", "model_provider", "cwd", "updated_at", "archived", "source"}


def clean(value: Any) -> str:
    """One display line, with no terminal escapes, bidi controls, or tab fields."""
    return "".join(" " if unicodedata.category(c).startswith("C") else c for c in str(value or ""))


def _content_text(content: Any) -> str:
    """Extract human-visible text without carrying Responses API IDs forward."""
    if isinstance(content, str):
        return content
    if not isinstance(content, list):
        return ""
    parts: list[str] = []
    for item in content:
        if not isinstance(item, dict):
            continue
        text = item.get("text")
        if isinstance(text, str):
            parts.append(text)
        elif isinstance(item.get("input_text"), str):
            parts.append(item["input_text"])
    return "\n".join(parts)


def _clip(text: str, limit: int | None) -> str:
    if limit is None or len(text) <= limit:
        return text
    return text[:limit] + f"\n\n[… 已截断，原长度 {len(text)} 字符 …]"


def _json_text(value: Any) -> str:
    if isinstance(value, str):
        return value
    try:
        return json.dumps(value, ensure_ascii=False, indent=2)
    except (TypeError, ValueError):
        return str(value)


def export_transcript(row: dict[str, Any], home: Path, *, full: bool = False) -> str:
    """Render a rollout as provider-neutral Markdown for a new session.

    Only visible conversation/tool data is exported. IDs, internal metadata and
    encrypted reasoning are deliberately not copied because another provider
    cannot replay them as Responses API items.
    """
    rollout = row.get("rollout_path")
    if not rollout:
        raise HistoryError("历史缺少 rollout 文件，无法导出上下文。")
    path = Path(str(rollout)).expanduser()
    try:
        path = path.resolve()
        path.relative_to(home.resolve())
    except ValueError:
        raise HistoryError("rollout 文件不在 CODEX_HOME 下，拒绝导出。") from None
    if not path.is_file():
        raise HistoryError("找不到该会话的 rollout 文件，无法导出上下文。")

    clip_limit = None if full else 8000
    blocks: list[str] = []
    skipped = 0
    with path.open(encoding="utf-8") as stream:
        for line_no, line in enumerate(stream, 1):
            try:
                record = json.loads(line)
            except json.JSONDecodeError:
                skipped += 1
                continue
            item = record.get("payload") if isinstance(record, dict) else None
            if not isinstance(item, dict) or record.get("type") != "response_item":
                continue
            item_type = item.get("type")
            if item_type == "message":
                role = item.get("role")
                text = _content_text(item.get("content"))
                # Developer/system prompts contain local policy and provider
                # wiring; they are not portable conversation context.
                if role not in ("user", "assistant") or not text.strip():
                    continue
                blocks.append(f"## {'用户' if role == 'user' else '助手'}\n\n{text.strip()}\n")
            elif item_type in ("function_call", "custom_tool_call"):
                name = item.get("name") or "tool"
                arguments = item.get("arguments", item.get("input", ""))
                blocks.append(f"### 工具调用：`{clean(name)}`\n\n```text\n{_clip(_json_text(arguments), clip_limit)}\n```\n")
            elif item_type in ("function_call_output", "custom_tool_call_output"):
                output = _json_text(item.get("output", ""))
                blocks.append(f"### 工具结果\n\n```text\n{_clip(output, clip_limit)}\n```\n")
            elif full and item_type == "reasoning":
                summaries = item.get("summary") or item.get("summary_text") or []
                text = _content_text(summaries) if isinstance(summaries, list) else str(summaries)
                if text.strip():
                    blocks.append(f"### 助手思路摘要（非原始 reasoning）\n\n{text.strip()}\n")
            else:
                skipped += 1

    if not blocks:
        raise HistoryError("rollout 中没有可导出的用户/助手文本。")
    title = clean(row.get("name") or row.get("title") or "Codex 会话")
    header = (f"# Codex 上下文导出：{title}\n\n"
              f"- 会话：`{row['id']}`\n"
              f"- 原 provider/model：`{clean(row.get('model_provider'))}/{clean(row.get('model'))}`\n"
              f"- 工作目录：`{clean(row.get('cwd'))}`\n"
              f"\n> 这是 provider-neutral 的文本上下文，不是原生 resume。请基于以下记录继续工作；"
              f"不要尝试重放旧工具调用或使用旧消息 ID。\n\n")
    if not full:
        header += "> 默认模式省略了内部 metadata、加密 reasoning，并截断了过长工具输出。\n\n"
    if skipped:
        header += f"> 另有 {skipped} 条不可移植的内部记录未导出。\n\n"
    return header + "\n".join(blocks).rstrip() + "\n"


def read_sqlite(home: Path) -> list[dict[str, Any]]:
    databases = [p for p in home.glob("state_*.sqlite") if p.stem[6:].isdigit()]
    if not databases:
        raise HistoryError("找不到 Codex state_*.sqlite；请确认 --home / CODEX_HOME。")
    # Never merge old database generations or silently fall back to stale ones.
    path = max(databases, key=lambda p: int(p.stem[6:]))
    try:
        with contextlib.closing(sqlite3.connect(path.as_uri() + "?mode=ro", uri=True, timeout=1)) as db:
            db.row_factory = sqlite3.Row
            db.execute("PRAGMA query_only=ON")
            columns = {r[1] for r in db.execute("PRAGMA table_info(threads)")}
            if not REQUIRED_FIELDS <= columns:
                raise HistoryError(f"{path.name} 的 threads 结构不兼容；未读取旧库或修改数据库。")
            projection = ", ".join(f'"{f}"' if f in columns else f'NULL AS "{f}"' for f in FIELDS)
            return [dict(row) for row in db.execute(f"SELECT {projection} FROM threads")]
    except sqlite3.Error as exc:
        raise HistoryError(f"只读历史库失败（{type(exc).__name__}）；未修改数据库。") from None


class RpcClient:
    """JSON-RPC over the daemon's local Unix WebSocket (not raw JSONL).

    This deliberately uses only the Python standard library. The control socket
    requires HTTP Upgrade plus WebSocket frames. Do not assume the byte proxy
    converts the standalone app-server's stdio JSONL protocol into WebSocket.
    """

    def __init__(self, executable: str, home: Path, timeout: float = 10):
        self.deadline = time.monotonic() + timeout
        self.counter = 0
        self.socket = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        self.socket.settimeout(max(0.1, timeout))
        socket_path = home / "app-server-control" / "app-server-control.sock"
        try:
            self.socket.connect(str(socket_path))
            key = base64.b64encode(os.urandom(16)).decode("ascii")
            self.socket.sendall(("GET / HTTP/1.1\r\nHost: localhost\r\n"
                                 "Upgrade: websocket\r\nConnection: Upgrade\r\n"
                                 "Sec-WebSocket-Version: 13\r\nSec-WebSocket-Key: " + key + "\r\n\r\n").encode())
            response = self._read_http_headers()
            if not response.startswith(b"HTTP/1.1 101 "):
                raise HistoryError("app-server control socket 未接受 WebSocket 连接。")
            accept = self._header(response, b"sec-websocket-accept")
            expected = base64.b64encode(hashlib.sha1((key + "258EAFA5-E914-47DA-95CA-C5AB0DC85B11").encode()).digest())
            if accept != expected:
                raise HistoryError("app-server WebSocket 握手校验失败。")
            if (self._header(response, b"upgrade").lower() != b"websocket"
                    or b"upgrade" not in self._header(response, b"connection").lower().split(b", ")):
                raise HistoryError("app-server WebSocket Upgrade 响应无效。")
        except BaseException:
            self.socket.close()
            raise

    def close(self) -> None:
        with contextlib.suppress(OSError):
            self.socket.close()

    def send(self, message: dict[str, Any]) -> None:
        payload = json.dumps(message, ensure_ascii=False, separators=(",", ":")).encode()
        self._send_frame(0x1, payload)

    def receive(self) -> dict[str, Any]:
        fragments = bytearray()
        fragmented = False
        while True:
            if time.monotonic() >= self.deadline:
                raise HistoryError("app-server 列表请求超时。")
            first = self._read_exact(2)
            final = bool(first[0] & 0x80)
            opcode = first[0] & 0x0F
            if first[0] & 0x70 or first[1] & 0x80:
                raise HistoryError("app-server 返回未协商扩展或带 mask 的服务器帧。")
            length = first[1] & 0x7F
            if length == 126:
                length = struct.unpack(">H", self._read_exact(2))[0]
            elif length == 127:
                length = struct.unpack(">Q", self._read_exact(8))[0]
            if length + len(fragments) > 16 * 1024 * 1024:
                raise HistoryError("app-server 响应过大。")
            if opcode >= 0x8 and (not final or length > 125):
                raise HistoryError("app-server 返回无效控制帧。")
            payload = self._read_exact(length)
            if opcode == 0x9:  # ping
                self._send_frame(0xA, payload)
                continue
            if opcode == 0x8:
                raise HistoryError("app-server 已关闭连接。")
            if opcode == 0xA:  # pong
                continue
            if (opcode == 0x1 and fragmented) or (opcode == 0x0 and not fragmented) or opcode not in (0x0, 0x1):
                raise HistoryError("app-server 返回无效或非文本帧。")
            fragments.extend(payload)
            if not final:
                fragmented = True
                continue
            try:
                result = json.loads(fragments.decode("utf-8"))
            except (UnicodeDecodeError, json.JSONDecodeError):
                raise HistoryError("app-server 响应格式不兼容。") from None
            if not isinstance(result, dict):
                raise HistoryError("app-server 响应格式不兼容。")
            return result

    def _read_exact(self, count: int) -> bytes:
        chunks = bytearray()
        while len(chunks) < count:
            try:
                self._set_remaining_timeout()
                chunk = self.socket.recv(count - len(chunks))
            except socket.timeout:
                raise HistoryError("app-server 列表请求超时。") from None
            if not chunk:
                raise HistoryError("没有可用的 app-server 连接。")
            chunks.extend(chunk)
        return bytes(chunks)

    def _read_http_headers(self) -> bytes:
        data = bytearray()
        while b"\r\n\r\n" not in data:
            data.extend(self._read_exact(1))
            if len(data) > 64 * 1024:
                raise HistoryError("app-server WebSocket 握手响应过大。")
        return bytes(data)

    @staticmethod
    def _header(response: bytes, name: bytes) -> bytes:
        prefix = name.lower() + b":"
        for line in response.split(b"\r\n"):
            if line.lower().startswith(prefix):
                return line.split(b":", 1)[1].strip()
        return b""

    def _send_frame(self, opcode: int, payload: bytes) -> None:
        # Client-to-server frames must be masked (RFC 6455 section 5.3).
        mask = os.urandom(4)
        masked = bytes(byte ^ mask[index % 4] for index, byte in enumerate(payload))
        length = len(payload)
        if length < 126:
            header = bytes((0x80 | opcode, 0x80 | length))
        elif length < 65536:
            header = bytes((0x80 | opcode, 0x80 | 126)) + struct.pack(">H", length)
        else:
            header = bytes((0x80 | opcode, 0x80 | 127)) + struct.pack(">Q", length)
        self._set_remaining_timeout()
        self.socket.sendall(header + mask + masked)

    def _set_remaining_timeout(self) -> None:
        remaining = self.deadline - time.monotonic()
        if remaining <= 0:
            raise HistoryError("app-server 列表请求超时。")
        self.socket.settimeout(remaining)

    def call(self, method: str, params: dict[str, Any]) -> dict[str, Any]:
        self.counter += 1
        self.send({"id": self.counter, "method": method, "params": params})
        while time.monotonic() < self.deadline:
            message = self.receive()
            if "method" in message:
                # A history-only client never grants interactive requests, even
                # if the server's request ID happens to match our own request ID.
                if "id" in message:
                    self.send({"id": message["id"], "error": {"code": -32601, "message": "Read-only client"}})
                continue
            if message.get("id") != self.counter:
                continue
            if "error" in message or not isinstance(message.get("result"), dict):
                # Error text may contain operator configuration: do not echo it.
                raise HistoryError(f"app-server {method} 失败或版本不兼容。")
            return message["result"]
        raise HistoryError("app-server 列表请求超时。")


def read_api(home: Path, executable: str, include_archived: bool, include_non_interactive: bool) -> list[dict[str, Any]]:
    with contextlib.closing(RpcClient(executable, home)) as client:
        client.call("initialize", {"clientInfo": {"name": "codex_history", "version": "0.1.0"}})
        client.send({"method": "initialized", "params": {}})
        rows = []
        for archived in ([False, True] if include_archived else [False]):
            cursor = None
            seen = set()
            while True:
                page = client.call("thread/list", {
                    "modelProviders": [], "sourceKinds": list(ALL_SOURCES if include_non_interactive else INTERACTIVE_SOURCES),
                    "sortKey": "updated_at", "sortDirection": "desc", "limit": 100,
                    "useStateDbOnly": True, "archived": archived, "cursor": cursor,
                })
                if not isinstance(page.get("data"), list):
                    raise HistoryError("app-server 列表格式不兼容。")
                for row in page["data"]:
                    rows.append({
                        "id": row["id"], "name": row.get("name"), "title": row.get("preview"),
                        "model_provider": row.get("modelProvider"), "model": row.get("model"),
                        "cwd": row.get("cwd"), "updated_at": row.get("updatedAt", 0),
                        "source": row.get("source", "unknown"), "archived": archived,
                        "rollout_path": row.get("path"), "status": row.get("status", {}).get("type", "unknown"),
                    })
                cursor = page.get("nextCursor")
                if cursor is None:
                    break
                if not isinstance(cursor, str) or cursor in seen:
                    raise HistoryError("app-server 返回重复或无效分页游标。")
                seen.add(cursor)
        return rows


def load_history(home: Path, backend: str, executable: str, include_archived: bool = False,
                 include_non_interactive: bool = False) -> tuple[list[dict[str, Any]], str]:
    local = None
    if backend != "sqlite":
        try:
            rows = read_api(home, executable, include_archived, include_non_interactive)
            source = "app-server"
        except (HistoryError, OSError, ValueError, KeyError, TypeError, AttributeError):
            if backend == "app-server":
                raise HistoryError("app-server 查询失败；未启动服务。可显式使用 --backend sqlite。") from None
            print("app-server 查询失败，保底使用只读 SQLite；可运行 codex app-server daemon start 后重试。", file=sys.stderr)
            rows = local = read_sqlite(home)
            source = "sqlite"
    else:
        rows = local = read_sqlite(home)
        source = "sqlite"
    if local is None:
        try:
            local = read_sqlite(home)
        except HistoryError:
            local = []
    by_id = {row["id"]: row for row in local}
    names = {str(row.get("id")): row.get("thread_name") for row in xmsg._jsonl_records(home / "session_index.jsonl")}
    result = []
    for original in rows:
        row = dict(original)
        for key in ("model", "cwd", "model_provider", "rollout_path", "name"):
            if not row.get(key):
                if key == "model" and row.get("model_provider") != by_id.get(row["id"], {}).get("model_provider"):
                    continue
                row[key] = by_id.get(row["id"], {}).get(key)
        row["name"] = row.get("name") or names.get(row["id"]) or row.get("title") or "未命名"
        row.setdefault("status", "unknown")  # No PID evidence is NOT proof a thread is idle.
        if row.get("archived") and not include_archived:
            continue
        if source == "sqlite" and not include_non_interactive and row.get("source") not in INTERACTIVE_SOURCES:
            continue
        result.append(row)
    return sorted(result, key=lambda r: (int(r.get("updated_at") or 0), r["id"]), reverse=True), source


def filter_history(rows: list[dict[str, Any]], provider: str | None, query: str, cwd: str | None) -> list[dict[str, Any]]:
    return [r for r in rows if (not provider or r["model_provider"] == provider)
            and (not cwd or r["cwd"] == cwd)
            and (not query or query.casefold() in " ".join(str(r.get(k) or "") for k in
                                                         ("id", "name", "model_provider", "model", "cwd")).casefold())]


def row_text(row: dict[str, Any]) -> str:
    try:
        updated = datetime.fromtimestamp(int(row.get("updated_at") or 0)).astimezone().strftime("%m-%d %H:%M")
    except (ValueError, OverflowError, OSError):
        updated = "未知时间"
    state = "归档" if row.get("archived") else row.get("status", "unknown")
    return "  ".join(clean(x) for x in (
        updated, f"[{row.get('model_provider') or '?'}/{row.get('model') or '?'}]",
        row["name"], row.get("cwd"), f"({state})", row["id"],
    ))


def resolve_thread(rows: list[dict[str, Any]], target: str) -> dict[str, Any]:
    # A UUID-shaped typo must not accidentally resolve to someone's custom name.
    try:
        target_id = str(uuid.UUID(target))
    except ValueError:
        target_id = None
    exact = [r for r in rows if r["id"] == (target_id or target)]
    if exact:
        return exact[0]
    if target_id:
        raise HistoryError("没有该完整会话 ID。")
    named = [r for r in rows if str(r["name"]).casefold() == target.casefold()]
    matches = named or ([r for r in rows if r["id"].startswith(target)] if len(target) >= 4 else [])
    if len(matches) == 1:
        return matches[0]
    if not matches:
        raise HistoryError("没有匹配的会话；请先用 list 查看，或检查过滤条件。")
    raise HistoryError("名称或 ID 前缀有歧义，请指定完整 ID：\n" + "\n".join(row_text(r) for r in matches))


def choose_thread(rows: list[dict[str, Any]], use_fzf: bool = True) -> dict[str, Any] | None:
    if not rows:
        raise HistoryError("没有匹配的会话。")
    if not sys.stdin.isatty() or not sys.stdout.isatty():
        raise HistoryError("交互选择需要终端；脚本请提供完整 ID，并用 --dry-run 预览。")
    fzf = shutil.which("fzf") if use_fzf else None
    if fzf:
        # Ignore ambient fzf bindings/preview commands: rows are data, not shell.
        env = {k: v for k, v in os.environ.items() if not k.startswith("FZF_DEFAULT_")}
        proc = subprocess.run(
            [fzf, "--no-multi", "--no-sort", "--delimiter=\t", "--with-nth=2..", "--prompt=Codex 历史 > "],
            input="".join(f"{r['id']}\t{row_text(r)}\n" for r in rows), text=True,
            stdout=subprocess.PIPE, env=env,
        )
        if proc.returncode in (1, 130):
            return None
        if proc.returncode:
            raise HistoryError("fzf 失败；可使用 --no-fzf 编号选择。")
        selected_id = proc.stdout.strip().split("\t", 1)[0]
        for row in rows:
            if row["id"] == selected_id:
                return row
        raise HistoryError("fzf 返回未知会话 ID。")
    for index, row in enumerate(rows, 1):
        print(f"{index:>3}. {row_text(row)}")
    answer = input("输入编号（回车取消）：").strip()
    if not answer:
        return None
    if not answer.isdigit() or not 1 <= int(answer) <= len(rows):
        raise HistoryError("无效编号。")
    return rows[int(answer) - 1]


def configured_providers(home: Path, profile: str | None) -> set[str]:
    paths = [home / "config.toml"]
    if profile:
        if Path(profile).name != profile or profile in (".", ".."):
            raise HistoryError("--profile 必须是配置名称，不能是路径。")
        paths.append(home / f"{profile}.config.toml")
        if not paths[-1].is_file():
            raise HistoryError("找不到该 profile 的 <名称>.config.toml。")
    providers = {"openai", "ollama", "lmstudio"}
    for path in paths:
        if not path.exists():
            continue
        try:
            with path.open("rb") as stream:
                config = tomllib.load(stream)
            providers.update(config.get("model_providers", {}).keys())
        except (OSError, ValueError, AttributeError):
            # TOML parser errors can include credential-bearing source lines.
            raise HistoryError("无法解析 Codex provider 配置；为保护凭据不输出原文。") from None
    return providers


def live_evidence(row: dict[str, Any], peer_db: Path) -> str | None:
    if row["id"] == os.environ.get("CODEX_THREAD_ID"):
        return "当前 Codex 会话"
    if row.get("status") in ("active", "idle", "systemError"):
        return "app-server 已加载该会话"
    if not peer_db.is_file():
        return None
    try:
        with contextlib.closing(sqlite3.connect(peer_db.resolve().as_uri() + "?mode=ro", uri=True, timeout=0.2)) as db:
            peer = db.execute("SELECT pid, pid_start FROM peers WHERE session_id=? AND tool='codex'", (row["id"],)).fetchone()
            if peer and peer[0] and peer[1] is not None and xmsg.pid_start_time(int(peer[0])) == int(peer[1]):
                return "xmsg 记录的会话进程仍在运行（PID + 启动时间一致）"
    except sqlite3.Error:
        pass
    return None


def resume_command(row: dict[str, Any], args: argparse.Namespace, home: Path) -> list[str]:
    try:
        thread_id = str(uuid.UUID(row["id"]))
    except (ValueError, TypeError, AttributeError):
        raise HistoryError("记录不是完整 UUID，拒绝恢复。") from None
    if row.get("archived"):
        raise HistoryError("该会话已归档；请先在 Codex 中显式取消归档，本工具不会自动修改归档状态。")
    provider = args.switch_provider or row.get("model_provider")
    if not provider:
        raise HistoryError("历史缺少 provider；请显式指定 --switch-provider 和 --model。")
    model = args.model or row.get("model")
    if not model or (args.switch_provider and args.switch_provider != row.get("model_provider") and not args.model):
        raise HistoryError("历史缺少模型或正在更换 provider；请显式指定 --model。")
    if any(unicodedata.category(c).startswith("C") for c in str(provider) + str(model)):
        raise HistoryError("provider/model 包含控制字符，拒绝恢复。")
    if provider not in configured_providers(home, args.profile):
        raise HistoryError(f"provider {clean(provider)!r} 不在用户配置中；请修复配置、指定 --profile 或显式更换 provider。")
    cwd_value = args.cd or row.get("cwd")
    if not cwd_value or not Path(cwd_value).is_dir():
        raise HistoryError("原工作目录不存在；请用 --cd 指定目录，不会静默进入当前目录。")
    command = [args.codex_bin, "resume", thread_id, "-c", "model_provider=" + json.dumps(provider, ensure_ascii=False),
               "--model", model, "--cd", str(Path(cwd_value).resolve())]
    if args.profile:
        command.extend(["--profile", args.profile])
    return command


def parser() -> argparse.ArgumentParser:
    common = argparse.ArgumentParser(add_help=False)
    common.add_argument("--home", type=Path, default=Path(os.environ.get("CODEX_HOME", Path.home() / ".codex")))
    common.add_argument("--backend", choices=("auto", "app-server", "sqlite"), default="auto")
    common.add_argument("--codex-bin", default=os.environ.get("XMSG_CODEX_BIN") or "codex")
    common.add_argument("--provider", help="仅筛选，不更换 provider")
    common.add_argument("--query", default="", help="名称、ID、provider、model、目录的大小写不敏感片段")
    common.add_argument("--cwd", help="只列指定工作目录")
    common.add_argument("--include-archived", action="store_true")
    common.add_argument("--include-non-interactive", action="store_true")
    root = argparse.ArgumentParser(description=__doc__)
    commands = root.add_subparsers(dest="command", required=True)
    listing = commands.add_parser("list", parents=[common], help="跨 provider 只读列出历史")
    listing.add_argument("--json", action="store_true")
    resume = commands.add_parser("resume", parents=[common], help="选择后调用官方 codex resume")
    resume.add_argument("target", nargs="?", help="完整 UUID、自定义名或唯一 ID 前缀")
    resume.add_argument("--switch-provider", help="显式改用另一 provider；需确认并指定 --model")
    resume.add_argument("--model")
    resume.add_argument("--profile")
    resume.add_argument("--cd", help="覆盖已不存在或不适用的工作目录")
    resume.add_argument("--no-fzf", action="store_true")
    resume.add_argument("--dry-run", action="store_true", help="只输出 argv，不启动 Codex")
    export = commands.add_parser("export", parents=[common], help="把历史会话导出为 provider-neutral Markdown")
    export.add_argument("target", nargs="?", help="完整 UUID、自定义名或唯一 ID 前缀")
    export.add_argument("--full", action="store_true", help="保留完整工具输出和思路摘要；仍不导出加密 reasoning")
    export.add_argument("--output", type=Path, help="输出文件；省略时写 stdout")
    export.add_argument("--no-fzf", action="store_true")
    return root


def main(argv: list[str] | None = None) -> int:
    args = parser().parse_args(argv)
    home = args.home.expanduser().resolve()
    try:
        rows, backend = load_history(home, args.backend, args.codex_bin, args.include_archived, args.include_non_interactive)
        rows = filter_history(rows, args.provider, args.query, str(Path(args.cwd).resolve()) if args.cwd else None)
        if args.command == "list":
            if args.json:
                print(json.dumps({"backend": backend, "threads": rows}, ensure_ascii=False, indent=2))
            else:
                print("更新时间  [provider/model]  名称  工作目录  状态  完整 ID")
                for row in rows:
                    print(row_text(row))
                print(f"共 {len(rows)} 条；来源 {backend}；unknown 表示未确认进程状态。", file=sys.stderr)
            return 0
        row = resolve_thread(rows, args.target) if args.target else choose_thread(rows, not args.no_fzf)
        if row is None:
            return 0
        if args.command == "export":
            text = export_transcript(row, home, full=args.full)
            if args.output:
                output = args.output.expanduser().resolve()
                output.parent.mkdir(parents=True, exist_ok=True)
                output.write_text(text, encoding="utf-8", newline="\n")
                print(f"已导出到 {output}")
            else:
                print(text, end="")
            return 0
        command = resume_command(row, args, home)
        switching = bool(args.switch_provider and args.switch_provider != row.get("model_provider"))
        evidence = live_evidence(row, xmsg.DB_PATH)
        warnings = []
        if switching:
            warnings.append(f"历史上下文将发送给新 provider：{clean(row.get('model_provider'))} → {clean(args.switch_provider)}")
        if evidence:
            warnings.append(evidence + "；请先退出原终端中的会话，避免并发恢复。")
        if args.dry_run:
            print(json.dumps({"argv": command, "env": {"CODEX_HOME": str(home)}, "warnings": warnings}, ensure_ascii=False, indent=2))
            return 0
        if not sys.stdin.isatty() or not sys.stdout.isatty():
            raise HistoryError("恢复需要交互终端；可使用 --dry-run 预览。")
        if evidence:
            raise HistoryError(warnings[-1])
        print(row_text(row))
        print("将执行：" + clean(shlex.join(command)))
        if switching:
            print(warnings[0], file=sys.stderr)
            if input("确认跨 provider 发送历史？输入 yes：").strip() != "yes":
                return 0
        elif input("恢复此会话？[y/N] ").strip().lower() != "y":
            return 0
        os.execvpe(command[0], command, {**os.environ, "CODEX_HOME": str(home)})
        return 0
    except (HistoryError, OSError) as exc:
        detail = str(exc) if isinstance(exc, HistoryError) else type(exc).__name__
        print("codex-history: " + "\n".join(clean(line) for line in detail.splitlines()), file=sys.stderr)
        return 2
    except (KeyboardInterrupt, EOFError):
        return 130


if __name__ == "__main__":
    sys.exit(main())
