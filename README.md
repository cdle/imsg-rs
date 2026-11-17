# imsg-rs

轻量的本机 iMessage ↔ HTTP 服务。轮询 `chat.db` 收消息，通过 `ssh localhost` + osascript 发消息，让 AI 或其他程序用一个 HTTP 接口接入 iMessage。

## 为什么这么写

- 直接跑 osascript 时，TCC 会把自动化请求归因到 ChatGPT/codex 宿主链（magpie），而 magpie 没有合适的 entitlement，导致一直 -1712 超时。
- 通过 `ssh localhost` 起 osascript，自动化请求的"责任进程"变成 sshd，不受那层嵌套 GUI 链影响，发送实测可用。
- 不依赖 Matrix、不依赖 macOS 14+，纯本机轮询。

## 运行

```sh
cargo build --release
cp com.muse.imsg-rs.plist ~/Library/LaunchAgents/
launchctl bootstrap gui/501 ~/Library/LaunchAgents/com.muse.imsg-rs.plist
```

launchd 会常驻并自动重启。

## API

| 方法 | 路径 | 说明 |
|---|---|---|
| GET  | `/health`   | 存活检查 |
| GET  | `/messages` | 最近 20 条（含游标前后的收发记录） |
| POST | `/send`     | `{"to":"...","text":"..."}`，to 是 iMessage 账号（邮箱或手机号） |

## 数据

- 收：FSEvents 监听 `~/Library/Messages`，写入后毫秒级读 `chat.db`（只读，实测事件触发 <30ms），另有 5 秒兜底轮询；游标存 `cursor.txt`，空文本（已读回执等）已过滤。
- 发：写 `/tmp/imsg-rs-send.scpt` 后经 `ssh localhost` 执行，超时 15s。
- 日志：`imsg-rs.log`（收到的消息与错误）。

## 已知边界

- 收依赖 FSEvents；极端情况下事件丢失会由 5 秒兜底轮询补上。
- 发送用的是固定 iMessage 账户 `ACCOUNT_ID`（本机当前唯一 iMessage 账户），如果账户变了需要改 `ACCOUNT_ID`。
- 所有流量都在 127.0.0.1:8420，没有鉴权；只在本机用。
