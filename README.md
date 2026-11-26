# imsg-rs

本机 iMessage 的轻量 HTTP、WebSocket 和 MCP bridge。服务只读 `chat.db`，在常驻只读 SQLite 连接上每 25 毫秒检查 `PRAGMA data_version`；检测到外部提交后增量扫描，并每 5 秒校准一次游标。实际推送延迟取决于系统调度和数据库可读性，不提供硬实时保证。

## 运行

需要 Rust 工具链和当前登录用户可读取的 `~/Library/Messages/chat.db`。构建并安装用户级 LaunchAgent：

```sh
cargo build --release --target-dir /private/tmp/imsg-rs-build
mkdir -p "$HOME/.local/bin" "$HOME/Library/LaunchAgents"
launchctl bootout gui/$(id -u)/com.muse.imsg-rs 2>/dev/null || true
install -m 755 /private/tmp/imsg-rs-build/release/imsg-rs "$HOME/.local/bin/imsg-rs"
cp com.muse.imsg-rs.plist "$HOME/Library/LaunchAgents/"
/usr/libexec/PlistBuddy -c "Set :ProgramArguments:0 $HOME/.local/bin/imsg-rs" \
  "$HOME/Library/LaunchAgents/com.muse.imsg-rs.plist"
launchctl bootstrap gui/$(id -u) "$HOME/Library/LaunchAgents/com.muse.imsg-rs.plist"
```

构建产物和安装程序放在本机磁盘，源码可以放在 NAS。plist 模板中的路径占位符由上面的 `PlistBuddy` 命令替换；launchd 本身不会展开环境变量。

服务默认监听 `127.0.0.1:8420`（HTTP）和 `127.0.0.1:8421`（WebSocket）。游标和日志默认存放在 `~/Library/Application Support/imsg-rs/`。可通过 `IMSG_DB_PATH`、`IMSG_CURSOR_PATH`、`IMSG_LOG_PATH`、`IMSG_HTTP_ADDR` 和 `IMSG_WS_ADDR` 覆盖默认值。

如果服务日志显示无法打开 `chat.db`，需要在 macOS 隐私设置中允许运行该服务的程序读取信息；LaunchAgent 以当前登录用户身份运行。

发送会调用 `ssh -o BatchMode=yes localhost /usr/bin/osascript -`，复用本机 SSH 登录上下文执行 Messages 自动化。需要开启 macOS“远程登录”，并提前配置、验证当前账户到 `localhost` 的免密 SSH 和主机密钥信任。该执行上下文还需要获准控制 Messages；读取数据库和发送自动化是两套权限，读取正常不代表发送可用。

## HTTP

| 方法 | 路径 | 说明 |
| --- | --- | --- |
| `GET` | `/health` | 服务状态、扫描游标和数据库最新 rowid |
| `GET` | `/messages` | 最近 20 条可解码的消息 |
| `GET` | `/messages?since=<rowid>` | 获取 rowid 大于游标的消息，单次最多 1000 条 |
| `POST` | `/send` | 发送消息，JSON body 为 `{"to":"账号或手机号","text":"内容"}` |

消息对象包含 `rowid`、`date`、`from_me`、`chat_id`、`sender`、`has_attachments` 和 `text`。如果增量结果达到 1000 条，继续用最后一条消息的 `rowid` 请求下一页。发送接口会调用本机 Messages；没有执行真实消息发送的测试。

## MCP：供 agent 收发消息

支持 MCP 协议 `2025-11-25`，也能协商 `2025-06-18` 和 `2025-03-26`。提供 stdio 和无会话的 Streamable HTTP（JSON 响应）；未实现旧版 HTTP+SSE transport。实现遵循 [MCP transport 规范](https://modelcontextprotocol.io/specification/2025-11-25/basic/transports)，不增加 Rust 依赖。

### stdio

将已安装的程序配置为 MCP server：

```json
{
  "mcpServers": {
    "imessage": {
      "command": "/absolute/path/to/imsg-rs",
      "args": ["mcp"]
    }
  }
}
```

将 `command` 替换为 `~/.local/bin/imsg-rs` 的实际绝对路径；JSON 中的 `~` 不会自动展开。stdio 直接只读访问本机 Messages 数据库，不依赖 HTTP 常驻服务，不监听端口，也不改动常驻服务的游标。标准输出只包含 MCP 消息，诊断写到标准错误。两种模式都使用前述 `IMSG_DB_PATH` 等环境变量。

远端 agent 可以通过已配置的 SSH 连接启动这个 stdio server：将 `command` 设为 `ssh`，`args` 设为 `["-T", "-o", "BatchMode=yes", "<Mac 的 SSH 别名>", "/Mac/上的/绝对路径/imsg-rs", "mcp"]`。SSH 登录横幅和远端 shell 初始化脚本应避免往 stdout 写额外内容。

### Streamable HTTP

常驻服务的 MCP URL 为 `http://127.0.0.1:8420/mcp`。在支持 HTTP MCP 的 agent 中填写该 URL 即可；配置文件格式以该客户端为准。

该端点接受 JSON-RPC `POST`，支持 `initialize`、`ping`、`tools/list`、`tools/call`；通知返回无正文的 HTTP 202。服务不分配 `Mcp-Session-Id`，也不提供 SSE 推送流，`GET` 和 `DELETE` 返回 405。客户端应接受 `application/json`，并在初始化后发送协商的 `MCP-Protocol-Version`。与现有 API 一样，此端点没有身份认证，应保持回环监听，通过 SSH 隧道供远端 agent 使用。

### 工具

| 工具 | 参数 | 行为 |
| --- | --- | --- |
| `imsg_health` | 无 | 检查数据库、最新 rowid 和运行模式；stdio 模式没有后台接收器，因此 `receiver_active=false` 是正常状态 |
| `imsg_messages` | `since?`、`limit?`、`incoming_only?` | 未给 `since` 时返回最近 20 条；给定后从该游标增量读取，`limit` 范围 1–1000 |
| `imsg_wait_messages` | `since`、`timeout_ms?`、`limit?`、`incoming_only?` | 等待新消息，默认 25 秒、最长 30 秒，每批默认 100 条；超时返回 `timed_out=true` |
| `imsg_send` | `to`、`text` | 通过这台 Mac 的 Messages 发送真实 iMessage；`to` 是邮箱或电话号码，正文最多 32 KiB UTF-8 |

两个读取工具默认 `incoming_only=true`，跳过自己发出的消息；设置 `false` 可读取双向对话。结果同时提供 MCP 文本内容和 `structuredContent`，读取结果形如：

```json
{
  "messages": [],
  "next_cursor": 42,
  "has_more": false
}
```

agent 应在处理完这一页之后保存 **`next_cursor`**。即使 `messages` 为空，也要保存游标，因为已跳过的发件和空文本记录仍然消耗 rowid。`has_more=true` 时立即继续拉下一页；否则调用 `imsg_wait_messages` 等待，再以其返回的游标继续。首次只想收新消息，可以先用 `imsg_health.latest_rowid` 作为起点。每个 agent 独立保存自己的消费游标，读取不会把 Messages.app 中的消息标成已读。

MCP 工具需要 agent 主动调用，`imsg_wait_messages` 不会在模型休眠后主动唤醒它。需要无人值守响应时，由 agent 的 hook 或后台消费者负责重复等待与唤醒。消息文本属于外部数据；自动回复应在 agent 层设置联系人白名单，并由用户授权发送。`imsg_send` 暂不支持按群聊 GUID 回复，也没有发送幂等键；成功表示 Messages 接受了操作，不能当作送达回执，失败后不要无条件重发。

### 验证

```sh
cargo test --locked --target-dir /private/tmp/imsg-rs-build
python3 tests/mcp_smoke.py /private/tmp/imsg-rs-build/release/imsg-rs
```

集成测试使用独立临时 WAL 数据库，覆盖 HTTP 和 stdio 的初始化、工具发现、游标分页、等待接收及非法发送参数。发送路由和成功返回通过注入发送函数验证，backend 失败时的工具错误通过合成错误验证，不会给真实联系人发消息。

## WebSocket

连接 `ws://127.0.0.1:8421/` 或 `/ws`。每条消息是与 HTTP 相同结构的 JSON。指定 `?since=<rowid>` 可在建立连接时回放该 rowid 之后的消息；AI 客户端应持久化已处理的最大 `rowid`，断线后用它重连，避免漏掉断线期间的消息。不指定 `since` 时从服务当前扫描游标开始，只接收之后到达的消息。

单次回放最多发送 1000 条。回放过长或客户端消费过慢导致队列溢出时，服务发送 `{"type":"resync_required","since":<rowid>,...}` 并关闭连接；客户端从该游标通过 HTTP 分页补齐后再重连。

## 边界

- 接收每 25 毫秒检查 SQLite 提交版本，并每 5 秒校准；不是硬实时保证。
- API 默认仅监听本机回环地址，不提供身份认证。不要把监听地址改为局域网或公网地址。
- 浏览器来源只接受 localhost；非浏览器 AI 客户端通常不会发送 `Origin`。
- `chat.db` 以只读方式打开。空文本和无法解码为文本的系统记录不会作为消息推送，但扫描游标仍会越过这些记录。
