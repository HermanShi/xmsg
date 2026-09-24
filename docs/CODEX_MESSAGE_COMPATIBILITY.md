# Codex 与 Claude Code 消息投递兼容性及版本依据

本文记录 xmsg 使用的 Codex / Claude Code 消息能力、源码版本边界和复核方法。
路由顺序见 [README](../README.md#codex-版本与能力选择)，实现入口为
[`codex_delivery.py`](../codex_delivery.py) 与 [`xmsg.py`](../xmsg.py)。

## 如何理解最低版本

下表的“首次正式版”是 **OpenAI 官方仓库中，首次包含该能力引入提交的 stable tag**，
不包括 alpha / beta / RC。它不等于本机最早安装的版本，也不表示每一个后续构建都已做过端到端验证。
源码语义对照基准为 `rust-v0.156.1`；发行版、第三方构建、功能开关及运行状态仍须现场检查。

判断分为四层：

1. 引入提交说明具体增加了什么，不能拿名字相近的另一项能力代替。
2. 正式 tag 的提交祖先关系确定发行边界；相邻旧 tag 用来验证负边界。
3. 运行中的服务和客户端必须实际提供对应能力，不能仅检查 PATH 中刚升级的 CLI。
4. API 接受、写入 socket、进入队列、模型收到、终端显示，是不同的验证结果。

版本号比较应遵守预发行语义：`0.151.0-alpha.1` 不能因为前三段相同就满足 `>= 0.151.0`。
无法解析的版本、同版本号的自定义构建、缺失的能力字段，均不能推断为已支持。

## Codex：20 项能力的正式版边界

源码来自 [openai/codex](https://github.com/openai/codex)。表中路径相对于该仓库，
指向**引入提交当时**的文件；同一单元格的短路径沿用前一个完整路径的对应目录。
后续重构可能移动文件，例如 `protocol/v2.rs` 后来拆成 `protocol/v2/`。
“前一正式 tag”指按版本排序紧邻的 stable tag，可能是上一小版本的补丁版。

| 能力 | 首次正式版 | 前一正式 tag | 引入提交 / PR | 主要源码路径 |
| --- | --- | --- | --- | --- |
| `turn/start` V2 | 0.56.0 | 0.55.0 | [`658255492`](https://github.com/openai/codex/commit/6582554926e9c474afc287c091039fdaa2eacecd) / #6216 | `codex-rs/app-server-protocol/src/protocol/common.rs`、`protocol/v2.rs`；`codex-rs/app-server/src/codex_message_processor.rs` |
| `thread/loaded/list` | 0.80.0 | 0.79.0 | [`5b7707dfb`](https://github.com/openai/codex/commit/5b7707dfb1a3001288a4d6817ca39f6f0cce8358) / #8902 | `codex-rs/app-server-protocol/src/protocol/v2.rs`；`codex-rs/app-server/src/codex_message_processor.rs` |
| `turn/steer`，含 `expectedTurnId` | 0.99.0 | 0.98.0 | [`0d8b2b74c`](https://github.com/openai/codex/commit/0d8b2b74c46aeb7c691fcaf96156ed7927ee1d16) / #10821 | `codex-rs/app-server-protocol/src/protocol/v2.rs`；`codex-rs/app-server/src/codex_message_processor.rs` |
| `thread/read`、`thread/list` 的运行时 `status` | 0.105.0 | 0.104.0 | [`1f54496c4`](https://github.com/openai/codex/commit/1f54496c48ffb9678095cc91d40556faa57c99fb) / #11786 | `codex-rs/app-server-protocol/src/protocol/v2.rs`；`codex-rs/app-server/src/thread_status.rs` |
| Hooks 引擎的 `SessionStart`、`Stop` 事件 | 0.114.0 | 0.113.0 | [`244b2d53f`](https://github.com/openai/codex/commit/244b2d53f40938ffba96acf0ca7a559473b842f1) / #13276 | `codex-rs/hooks/src/engine/dispatcher.rs`、`events/session_start.rs`、`events/stop.rs` |
| Stop continuation 与 `stop_hook_active` | 0.115.0 | 0.114.0 | [`9a44a7e49`](https://github.com/openai/codex/commit/9a44a7e499f18eaed5d06aabb5acf9184deb06b8) / #14532 | `codex-rs/hooks/src/events/stop.rs`、`engine/output_parser.rs`；`codex-rs/core/src/codex.rs` |
| shell `PreToolUse` 事件 | 0.117.0 | 0.116.0 | [`73bbb07ba`](https://github.com/openai/codex/commit/73bbb07ba8302932a5462811bc68da0ef66ce50a) / #15211 | `codex-rs/hooks/src/events/pre_tool_use.rs`、`engine/dispatcher.rs` |
| `thread/turns/list`，含 `limit`、`sortDirection` | 0.122.0 | 0.121.0 | [`eaf78e43f`](https://github.com/openai/codex/commit/eaf78e43f2e95b978622b97c1f656236c7cd8927) / #17305 | `codex-rs/app-server-protocol/src/protocol/v2.rs`；`codex-rs/app-server/src/codex_message_processor.rs` |
| Unix socket transport | 0.125.0 | 0.124.0 | [`8a0ab3fc1`](https://github.com/openai/codex/commit/8a0ab3fc135022db644adec2ef02e2e0624d0d7d) / #18255 | `codex-rs/app-server/src/transport/unix_socket.rs` |
| Unix socket transport 改用 WebSocket upgrade | 0.126.0 | 0.125.0 | [`687c5d908`](https://github.com/openai/codex/commit/687c5d9081f373166a06c2f18e7f634f9a0ff44b) / #19244 | `codex-rs/app-server/src/transport/unix_socket.rs`、`unix_socket_tests.rs` |
| `PreToolUse.additionalContext` | 0.129.0 | 0.128.0 | [`af86be529`](https://github.com/openai/codex/commit/af86be529c209fba11603f9df0586875bf075e79) / #20692 | `codex-rs/hooks/src/events/pre_tool_use.rs`、`engine/output_parser.rs` |
| `thread/turns/list` 的 `itemsView` 请求参数 | 0.130.0 | 0.129.0 | [`0d0835dd5`](https://github.com/openai/codex/commit/0d0835dd537b57913627e877f29031151b49429e) / #21566 | `codex-rs/app-server-protocol/src/protocol/v2/thread.rs`、`tests.rs` |
| managed app-server daemon 生命周期 | 0.131.0 | 0.130.0 | [`0c8d42525`](https://github.com/openai/codex/commit/0c8d42525effe7c0208fcfe052a5aece8941cc17) / #20718 | `codex-rs/app-server-daemon/src/lib.rs`；`codex-rs/cli/src/main.rs` |
| `thread/read.canAcceptDirectInput` | 0.145.0 | 0.144.6 | [`3f0669dbd`](https://github.com/openai/codex/commit/3f0669dbd65c7cd369c84fd44b5696f871142fea) / #33841 | `codex-rs/app-server-protocol/src/protocol/v2/thread_data.rs`；`codex-rs/app-server/src/request_processors/thread_processor.rs` |
| `canAcceptDirectInput` 扩展到 `thread/list` | 0.147.0 | 0.146.1 | [`9a6668f67`](https://github.com/openai/codex/commit/9a6668f674d74b35418fa534b3b6285a315d0765) / #35944 | `codex-rs/app-server/src/request_processors/thread_enrichment.rs`、`thread_processor.rs` |
| 实验性 `thread/queue/*` API | 0.148.0 | 0.147.0 | [`9341b3831`](https://github.com/openai/codex/commit/9341b38310c73957e1313eab3f7c4034689bdec9) / #38456 | `codex-rs/app-server-protocol/src/protocol/common.rs`、`protocol/v2/thread.rs`；`codex-rs/app-server/src/request_processors/thread_queue_processor.rs` |
| `codex queue --thread … --message …` CLI | 0.149.0 | 0.148.0 | [`83d015375`](https://github.com/openai/codex/commit/83d015375e578e369c115b06aea631f266226a4f) / #39092 | `codex-rs/cli/src/queue_cmd.rs`、`main.rs` |
| `codex_tui.send_message_to_thread` 初版 | 0.150.0 | 0.149.1 | [`a8468330b`](https://github.com/openai/codex/commit/a8468330bb5f45e9f4d2ec630b01ea8c52908be3) / #40308 | `codex-rs/tui/src/dynamic_tools.rs`、`dynamic_tools_mcp.rs` |
| `turn/start.toolOutput` | 0.151.0 | 0.150.1 | [`e56e4922e`](https://github.com/openai/codex/commit/e56e4922ebb71b868aba275aa85fbcd87b69034f) / #41002 | `codex-rs/app-server-protocol/src/protocol/v2/turn.rs`；`codex-rs/app-server/src/request_processors/turn_processor.rs` |
| TUI 委派改用 tool output，并显示其发送来源与正文 | 0.151.0 | 0.150.1 | [`72c96598c`](https://github.com/openai/codex/commit/72c96598c6b62126896f11ff1619a19b68cbc4d9) / #41046 | `codex-rs/tui/src/dynamic_tools.rs`、`chatwidget/replay.rs`、`thread_transcript.rs` |

20 项引入提交均满足：首次正式 tag 包含该提交，表中前一正式 tag 不包含该提交。
这个祖先关系结论须结合引入 diff 理解，不能单独用来证明一个未知实现没有等价能力。

## Codex：协议与显示语义

### 服务、客户端和会话版本分开判断

`codex --version` 只说明本次执行的二进制。后台 daemon 可能仍运行旧包，
已打开的 TUI 也可能仍运行旧包。应分别读取实际 server 的初始化响应或 daemon 版本响应，
以及接收会话对应的运行二进制版本。

`thread.cliVersion` 的源码注释是 **“Version of the CLI that created the thread.”**
它是创建时的历史信息，不能证明当前 TUI 支持某种显示。
对应定义在 `rust-v0.156.1:codex-rs/app-server-protocol/src/protocol/v2/thread_data.rs`。
无法可靠建立 thread 与运行客户端之间的关系时，须采用保守兼容策略，不能拿创建版本补齐证据。

managed daemon 自 0.131.0 引入，不代表从此每个平台上的启动方式、socket 路径或进程管理方式都相同。
Unix socket 自 0.125.0 存在，也不代表该版本已经接受 WebSocket handshake；后者自 0.126.0 引入。
连接、初始化和只读查询失败时，不能通过自动启动或恢复另一个会话来声称原目标已接通。

### 能力与实验开关

每条 app-server 连接先发 `initialize`，再发 `initialized`。
在初始化的 `capabilities` 中声明 `experimentalApi: true` 才能使用需要它的实验能力。
截至 0.156.1，`thread.canAcceptDirectInput` 和 `thread/queue/*` 仍带实验标记。
`thread/turns/list` 的实验标记曾变化；应以目标版本的 schema 和响应为准，不能仅依据最新文档。

`canAcceptDirectInput` 是实际字段名。`false` 表示拒绝直接输入，`null` 或缺失表示能力未知，
均不能当作 `true`。例如父代理拥有的 multi-agent v2 子线程会拒绝直接输入，
不能因为线程存在或状态为 `active` 就绕过所有权限制。

`thread/loaded/list` 返回 `data: string[]` 和 `nextCursor`。读完整分页后才能断言某线程不在列表中。
磁盘上有 rollout、数据库里有记录、PID 仍存活，都不能单独证明该 thread 已被当前 server 加载。

查忙碌线程的最新 turn 可以使用：

```json
{"method":"thread/turns/list","params":{"threadId":"TARGET","limit":1,"sortDirection":"desc","itemsView":"notLoaded"}}
```

其中 `limit`、`sortDirection` 自 0.122.0 引入，`itemsView` **请求参数**自 0.130.0 引入。
0.129.0 的 [`9e0c191c1` / #21063](https://github.com/openai/codex/commit/9e0c191c133deaab89a17ef714e916bafe4dbc23)
只增加了 Turn **响应**中的 `itemsView` 元数据和枚举；不能把它误当成请求过滤能力。
`turn/steer.expectedTurnId` 必须匹配当前活动 turn；活动状态可能在读写之间变化，明确拒绝与结果不明要分别处理。

### 0.150 → 0.151：相同工具名，权限语义不同

0.150.0 引入 `codex_tui.send_message_to_thread`，初版将委派正文作为 `UserInput` 发送。
0.151.0 的 #41002 增加 `turn/start.toolOutput`，#41046 随后把 TUI 委派切到该入口，
避免把工具/同伴消息提升为用户输入权限；源码明确不为旧版本兼容而降回用户权限。

当前入口形状为：

```json
{
  "method": "turn/start",
  "params": {
    "threadId": "TARGET",
    "input": [],
    "toolOutput": {
      "name": "send_message_to_thread",
      "namespace": "codex_tui",
      "output": "<codex_delegation>\n  <source_thread_id>SOURCE</source_thread_id>\n  <input>MESSAGE</input>\n</codex_delegation>"
    }
  }
}
```

`toolOutput` 不能与非空 `input` 混用，`name` 不能为空。
官方 [app-server turns 文档](https://developers.openai.com/codex/app-server/#turns)说明：
独立 tool output 以 `functionCallOutput` 记录；已有普通 turn 活动时，会为该 turn 排入输出。
这与把用户输入排到下一轮的 `codex queue` 不是同一语义。

TUI 的 `parse_delegated_tool_output` 只接受以下显示格式：

- namespace 是 `codex_tui` 或 `codex_app`；name 是 `create_thread` 或 `send_message_to_thread`。
- 正文严格匹配上述 `<codex_delegation>` 结构、换行和缩进。
- source 与正文按 `&` → `&amp;`、`<` → `&lt;`、`>` → `&gt;` 的顺序转义。

只发送自定义 namespace 的 `functionCallOutput` 不会自动获得这套 TUI 显示。
复用该结构也不等于真正调用了 Codex 原生 MCP 工具；来源必须如实标注，不能伪造发送会话或权限。
服务支持 tool output 与接收 TUI 支持上述显示，两者都需要至少 0.151.0 的对应能力。

### 队列与 hook 的独立限制

`thread/queue/*` 包括 `add`、`list`、`update`、`delete`、`reorder`、`start`，
以及 `thread/queue/changed` 通知；它们与稍后增加的 `codex queue` CLI 分别有版本边界。
排队成功只说明已接收入队，不能推出目标会话仍在线或用户已经看到。

0.114.0 的 hook 引擎、0.115.0 的 Stop continuation、0.117.0 的 shell PreToolUse、
0.129.0 的 PreToolUse 上下文注入，也必须分开判断。
事件存在不表示它已支持 `additionalContext`，更不表示所有工具类型在该早期版本都触发事件。
运行环境还须已配置、启用并信任对应 hook。完全 idle 的会话没有正在触发的 hook，
不能把“以后执行工具时会收到”描述成已即时送达。

## Claude Code：公开功能公告与内部 UDS 分开记录

依据为 [官方 CHANGELOG](https://github.com/anthropics/claude-code/blob/main/CHANGELOG.md)
和运行包中可检查到的协议实现。Claude 的 `cc-socks` 属于未公开的内部接口，
没有可据以承诺兼容性的公开协议版本表。

| 版本 | 官方变更记录说明什么 | 不能据此推断什么 |
| --- | --- | --- |
| 2.1.162 | 已记录 `CLAUDE_CODE_TMPDIR` / `TMPDIR` 深路径导致跨会话 `SendMessage` 失效的修复 | 这不是首次引入公告；不能推出当前 UDS envelope 自此稳定 |
| 2.1.166 | 跨会话转发不再携带用户权限 | 不能把发送者的权限模式声明当成普通用户授权 |
| 2.1.224 | 宣布 macOS / Linux 跨会话 `SendMessage`、`ListAgents`，并增加 `crossSessionInbound`、`dialogExpiry` | 前面的版本已有相关实现，224 不能当内部 `cc-socks` 首次版本 |
| 2.1.232 | 加固共享 `/tmp` 下的 socket 目录检查 | socket 路径存在不等于目录或对端已获信任 |
| 2.1.239 | 宣布 Windows 跨会话消息能力 | 不能照搬 Linux 的 `/run/user/...` 路径及认证假设 |
| 2.1.243 | 修复 namespace / rootless 环境问题；不完整行连接 30 秒超时 | 建立连接不代表已经提交完整消息 |
| 2.1.247 | 默认折叠为 `Message from @<sender>: <first line>` 单行预览，`Ctrl+O` 展开 | 折叠不等于消息消失 |
| 2.1.248 | 扩展至 Bedrock / Vertex / Foundry、禁用 telemetry 的环境；增加私有 `/tmp` 回退等 | 版本足够也不能跳过实际 socket 和设置检查 |
| 2.1.271 | held 消息增加发送侧通知，`SendMessage` 结果不再暗示已读 | accepted / held / read 不是同一个状态 |

2.1.281 原生包的静态检查可找到 `cc-socks`、`verifiedPeerPid`、
`takeInboundEnqueueTurn`、`from-mode` 等实现标识。
**这仅证明实现存在，不证明任意消息已被真正接收或显示。**
内部 UDS 最早支持当前 envelope 的准确版本仍未知；不得用 2.1.224 或本机已安装版本伪造该边界。

UDS 投递必须结合运行版本、PID 启动时间、实际 socket、目录/对端校验与消息处理结果。
`from-mode` 只能来自发送方真实权限记录；对端可能按策略 hold 或 refuse。
发生写入后断连、超时或 ACK 丢失时，不能假定“未收到”并自动重复投递。

## 可复核的源码命令

以下命令在 **Bash** 中执行。使用自行选择的源码目录，不修改运行中的 Codex 安装，
也不执行模型请求。若已有完整官方仓库，可直接使用它。

```bash
git clone --filter=blob:none --no-checkout https://github.com/openai/codex.git codex-source
cd codex-source
git fetch --filter=blob:none --tags origin
```

单项复核示例：

```bash
git show --stat 72c96598c
git show 72c96598c -- codex-rs/tui/src/dynamic_tools.rs codex-rs/tui/src/chatwidget/replay.rs
git tag --contains 72c96598c |
  grep -E '^rust-v[0-9]+\.[0-9]+\.[0-9]+$' |
  sort -V |
  head -1
```

最后一条应输出 `rust-v0.151.0`。下面批量复核全部 20 行的相邻正式 tag。
`merge-base --is-ancestor` 的 0 表示包含，1 表示不包含，大于 1 是命令错误；脚本分别处理，
不把仓库不完整或对象缺失当作负边界。

```bash
while read -r version intro previous; do
  if git merge-base --is-ancestor "$intro" "rust-v$version"; then
    first_status=0
  else
    first_status=$?
  fi
  if git merge-base --is-ancestor "$intro" "rust-v$previous"; then
    previous_status=0
  else
    previous_status=$?
  fi
  if [ "$first_status" -ne 0 ] || [ "$previous_status" -ne 1 ]; then
    printf 'FAIL %s %s previous=%s status=%s/%s\n' \
      "$version" "$intro" "$previous" "$first_status" "$previous_status"
    exit 1
  fi
  printf 'OK %s %s previous=%s\n' "$version" "$intro" "$previous"
done <<'VERSIONS'
0.56.0 658255492 0.55.0
0.80.0 5b7707dfb 0.79.0
0.99.0 0d8b2b74c 0.98.0
0.105.0 1f54496c4 0.104.0
0.114.0 244b2d53f 0.113.0
0.115.0 9a44a7e49 0.114.0
0.117.0 73bbb07ba 0.116.0
0.122.0 eaf78e43f 0.121.0
0.125.0 8a0ab3fc1 0.124.0
0.126.0 687c5d908 0.125.0
0.129.0 af86be529 0.128.0
0.130.0 0d0835dd5 0.129.0
0.131.0 0c8d42525 0.130.0
0.145.0 3f0669dbd 0.144.6
0.147.0 9a6668f67 0.146.1
0.148.0 9341b3831 0.147.0
0.149.0 83d015375 0.148.0
0.150.0 a8468330b 0.149.1
0.151.0 e56e4922e 0.150.1
0.151.0 72c96598c 0.150.1
VERSIONS
```

权限/显示行为还要直接对照文件，不能只看 tag 祖先关系：

```bash
# 此查询应报告该路径不存在；这是已知负样本，不要隐藏其它 git 错误。
git cat-file -e rust-v0.149.1:codex-rs/tui/src/dynamic_tools.rs

# 0.150.1 仍使用 input: vec![UserInput::Text ...]。
git show rust-v0.150.1:codex-rs/tui/src/dynamic_tools.rs |
  rg -n 'send_message_to_thread|input: vec!|tool_output'

# 0.151.0 出现 tool_output 和 parse_delegated_tool_output。
git show rust-v0.151.0:codex-rs/tui/src/dynamic_tools.rs |
  rg -n 'parse_delegated_tool_output|input: Vec::new|tool_output'

git show rust-v0.151.0:codex-rs/tui/src/chatwidget/replay.rs |
  rg -n -C 8 'FunctionCallOutput|Sent by Codex'
```

## 运行时验证判据

只读探测应先确认服务版本、目标已加载、允许直接输入、所需字段和消息上限。
要证明完整投递，使用专门测试会话及唯一 nonce，分别覆盖 idle、active、旧版本/未知版本降级，
并同时核对持久化历史、接收方返回的 nonce 和终端显示。
发送工具返回成功、hook 数据库标记已投递、日志中有 accepted，均不能替代这些终态证据。

定向回归入口见 [`tests/test_visible_delivery.py`](../tests/test_visible_delivery.py)。
单元测试能验证路由、版本解析与拒绝/结果不明的处理；不能代替真实目标版本上的接收和 UI 验证。
