# imsg-rs

本机 iMessage 的轻量 HTTP 和 WebSocket bridge。服务只读 `chat.db`，在常驻只读 SQLite 连接上每 25 毫秒检查 `PRAGMA data_version`；检测到外部提交后增量扫描，并每 5 秒校准一次游标。实际推送延迟取决于系统调度和数据库可读性，不提供硬实时保证。

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

## HTTP

| 方法 | 路径 | 说明 |
| --- | --- | --- |
| `GET` | `/health` | 服务状态、扫描游标和数据库最新 rowid |
| `GET` | `/messages` | 最近 20 条可解码的消息 |
| `GET` | `/messages?since=<rowid>` | 获取 rowid 大于游标的消息，单次最多 1000 条 |
| `POST` | `/send` | 发送消息，JSON body 为 `{"to":"账号或手机号","text":"内容"}` |

消息对象包含 `rowid`、`date`、`from_me`、`chat_id`、`sender`、`has_attachments` 和 `text`。如果增量结果达到 1000 条，继续用最后一条消息的 `rowid` 请求下一页。发送接口会调用本机 Messages；没有执行真实消息发送的测试。

## WebSocket

连接 `ws://127.0.0.1:8421/` 或 `/ws`。每条消息是与 HTTP 相同结构的 JSON。指定 `?since=<rowid>` 可在建立连接时回放该 rowid 之后的消息；AI 客户端应持久化已处理的最大 `rowid`，断线后用它重连，避免漏掉断线期间的消息。不指定 `since` 时从服务当前扫描游标开始，只接收之后到达的消息。

单次回放最多发送 1000 条。回放过长或客户端消费过慢导致队列溢出时，服务发送 `{"type":"resync_required","since":<rowid>,...}` 并关闭连接；客户端从该游标通过 HTTP 分页补齐后再重连。

## 边界

- 接收每 25 毫秒检查 SQLite 提交版本，并每 5 秒校准；不是硬实时保证。
- API 默认仅监听本机回环地址，不提供身份认证。不要把监听地址改为局域网或公网地址。
- 浏览器来源只接受 localhost；非浏览器 AI 客户端通常不会发送 `Origin`。
- `chat.db` 以只读方式打开。空文本和无法解码为文本的系统记录不会作为消息推送，但扫描游标仍会越过这些记录。
