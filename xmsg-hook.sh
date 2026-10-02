#!/usr/bin/env bash
# Hook entry point for xmsg. Wired into ~/.claude/settings.json,
# ~/.codex/hooks.json and ~/.gemini/config/hooks.json. Reads the host's native
# JSON payload on stdin and may print one hook-output object on stdout.
#
# Usage: xmsg-hook.sh <tool> [event]
#   <tool>  claude | codex | agy
#   [event] hook event name, only for hosts whose payload carries none:
#           Antigravity (agy) passes it as an argument instead
#           (xmsg-hook.sh agy PreInvocation | ... Stop).
#           Claude/Codex payloads carry hook_event_name, so they omit it.
#
# This wrapper exists for exactly one reason: fail-open has to hold for
# failures Python cannot catch from inside itself - the implementation file
# being deleted, the interpreter dying, or the work hanging. So:
#
#   * no `set -e` (a failing pipeline must not abort the script)
#   * every step tolerates failure with `|| true`
#   * `timeout` bounds the work well under the host's own hook timeout
#   * stderr is discarded, and the exit code is unconditionally 0
#
# A delivery that does not happen costs one message. A hook that errors or
# hangs costs the whole session, so the trade is not close.
#
# XMSG_NO_FAILOPEN=1 disables all of it (no timeout, errors propagate, real
# exit code). That is only for proving the safety net does something.
impl="${XMSG_IMPL:-$HOME/agent-msg/xmsg.py}"
tool="${1:-unknown}"
event="${2:-}"
budget="${XMSG_HOOK_TIMEOUT:-3}"

extra=()
[[ -n "$event" ]] && extra=(--event "$event")

if [[ "${XMSG_NO_FAILOPEN:-}" == "1" ]]; then
  exec /usr/bin/python3 "$impl" hook --tool "$tool" ${extra[@]+"${extra[@]}"}
fi

payload="$(cat 2>/dev/null)" || payload=""
out="$(printf '%s' "$payload" | timeout "$budget" /usr/bin/python3 "$impl" hook --tool "$tool" ${extra[@]+"${extra[@]}"} 2>/dev/null)" || out=""
[[ -n "$out" ]] && printf '%s' "$out"
exit 0
