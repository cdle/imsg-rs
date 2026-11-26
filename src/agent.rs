//! MCP transport adapters and the message tools shared by HTTP and stdio.

use super::*;
use serde_json::{json, Value};

#[derive(Serialize)]
struct MessagePage {
    messages: Vec<Msg>,
    next_cursor: i64,
    has_more: bool,
}

fn message_page(
    path: &Path,
    since: Option<i64>,
    limit: usize,
    incoming_only: bool,
) -> Result<MessagePage, String> {
    // Fix an upper bound before scanning: a concurrent commit belongs to the
    // next page and must never be skipped when next_cursor is saved.
    let upper_bound = latest_rowid(path)?;
    let mut messages = Vec::new();
    if let Some(since) = since {
        let mut cursor = since;
        while cursor < upper_bound {
            let rows = query_rows(path, cursor, MAX_BATCH_ROWS)?;
            if rows.is_empty() {
                break;
            }
            for row in rows {
                if row.rowid > upper_bound {
                    break;
                }
                cursor = row.rowid;
                if incoming_only && row.from_me {
                    continue;
                }
                if let Some(message) = decode_row(row) {
                    messages.push(message);
                    if messages.len() == limit {
                        return Ok(MessagePage {
                            messages,
                            next_cursor: cursor,
                            has_more: cursor < upper_bound,
                        });
                    }
                }
            }
            if cursor >= upper_bound {
                break;
            }
            // A row beyond the snapshot can be the only row left after a
            // concurrent delete. Do not repeatedly query that same row.
            if query_rows(path, cursor, 1)?
                .first()
                .is_none_or(|row| row.rowid > upper_bound)
            {
                break;
            }
        }
        Ok(MessagePage {
            messages,
            next_cursor: since.max(upper_bound),
            has_more: false,
        })
    } else {
        let mut cursor = upper_bound;
        loop {
            let rows = query_rows_ordered(path, cursor, MAX_BATCH_ROWS, true)?;
            let row_count = rows.len();
            for row in rows {
                cursor = row.rowid.saturating_sub(1);
                if incoming_only && row.from_me {
                    continue;
                }
                if let Some(message) = decode_row(row) {
                    messages.push(message);
                    if messages.len() == limit {
                        messages.reverse();
                        return Ok(MessagePage {
                            messages,
                            next_cursor: upper_bound,
                            has_more: false,
                        });
                    }
                }
            }
            if row_count < MAX_BATCH_ROWS as usize || cursor == 0 {
                break;
            }
        }
        messages.reverse();
        Ok(MessagePage {
            messages,
            next_cursor: upper_bound,
            has_more: false,
        })
    }
}

fn wait_messages(
    path: &Path,
    since: i64,
    limit: usize,
    incoming_only: bool,
    timeout_ms: u64,
) -> Result<Value, String> {
    let started = Instant::now();
    let timeout = Duration::from_millis(timeout_ms);
    let mut identity = database_identity(path)?;
    let mut connection = open_database(path)?;
    let mut version = database_version(&connection)?;
    if database_identity(path)? != identity {
        return Err(
            "Messages database changed while opening monitor; retry with the same cursor".into(),
        );
    }
    let mut page = message_page(path, Some(since), limit, incoming_only)?;
    let mut next_identity_check = Instant::now() + DATABASE_IDENTITY_CHECK_INTERVAL;
    let mut next_scan = Instant::now() + FALLBACK_SCAN_INTERVAL;

    while page.messages.is_empty() && started.elapsed() < timeout {
        std::thread::sleep(DATABASE_POLL_INTERVAL.min(timeout.saturating_sub(started.elapsed())));
        let now = Instant::now();
        if now >= next_identity_check {
            let current_identity = database_identity(path)?;
            if current_identity != identity {
                connection = open_database(path)?;
                version = database_version(&connection)?;
                if database_identity(path)? != current_identity {
                    return Err("Messages database changed while opening monitor; retry with the same cursor".into());
                }
                identity = current_identity;
                // A replacement must be scanned even if its data_version
                // happens to equal the old connection's version.
                next_scan = now;
            }
            next_identity_check = now + DATABASE_IDENTITY_CHECK_INTERVAL;
        }
        let current_version = database_version(&connection)?;
        if current_version != version || now >= next_scan || started.elapsed() >= timeout {
            version = current_version;
            page = message_page(path, Some(page.next_cursor), limit, incoming_only)?;
            next_scan = Instant::now() + FALLBACK_SCAN_INTERVAL;
        }
    }
    let timed_out = page.messages.is_empty();
    let mut result =
        serde_json::to_value(page).map_err(|error| format!("encode messages: {error}"))?;
    result["timed_out"] = json!(timed_out);
    Ok(result)
}

fn execute_tool(
    app: &App,
    call: mcp::ToolCall,
    mode: &str,
    send: impl FnOnce(&str, &str) -> Result<(), String>,
) -> Result<Value, String> {
    match call {
        mcp::ToolCall::Health => Ok(json!({
            "ok": true,
            "mode": mode,
            "latest_rowid": latest_rowid(&app.db_path)?,
            "receiver_active": app.receiver_active.load(Ordering::Acquire) == 1
        })),
        mcp::ToolCall::Messages {
            since,
            limit,
            incoming_only,
        } => serde_json::to_value(message_page(&app.db_path, since, limit, incoming_only)?)
            .map_err(|error| format!("encode messages: {error}")),
        mcp::ToolCall::WaitMessages {
            since,
            limit,
            timeout_ms,
            incoming_only,
        } => wait_messages(&app.db_path, since, limit, incoming_only, timeout_ms),
        mcp::ToolCall::Send { to, text } => {
            let body = SendBody { to, text };
            validate_send_body(&body)?;
            if body.text.is_empty() {
                return Err("text cannot be empty".into());
            }
            send(&body.to, &body.text)?;
            Ok(json!({"ok": true}))
        }
    }
}

fn rpc_error(code: i64, message: &str) -> String {
    json!({"jsonrpc": "2.0", "id": null, "error": {"code": code, "message": message}}).to_string()
}

fn local_host(request: &Request) -> bool {
    request
        .headers()
        .iter()
        .filter(|header| header.field.equiv("Host"))
        .all(|header| {
            header
                .value
                .as_str()
                .parse::<tungstenite::http::uri::Authority>()
                .is_ok_and(|authority| {
                    matches!(
                        authority.host().to_ascii_lowercase().as_str(),
                        "localhost" | "127.0.0.1" | "::1" | "[::1]"
                    )
                })
        })
}

pub(super) fn serve_http(mut request: Request, app: Arc<App>) {
    if !local_host(&request) {
        let _ = request.respond(json_response(
            403,
            rpc_error(-32600, "Host must be localhost"),
        ));
        return;
    }
    if request.method().as_str() != "POST" {
        let response = json_response(405, rpc_error(-32600, "MCP endpoint accepts POST requests"))
            .with_header(Header::from_bytes("Allow", "POST").expect("valid header"));
        let _ = request.respond(response);
        return;
    }
    if request.headers().iter().any(|header| {
        header.field.equiv("MCP-Protocol-Version")
            && !mcp::protocol_supported(header.value.as_str())
    }) {
        let _ = request.respond(json_response(
            400,
            rpc_error(-32600, "unsupported MCP-Protocol-Version"),
        ));
        return;
    }
    let content_type = request
        .headers()
        .iter()
        .find(|header| header.field.equiv("Content-Type"));
    if !content_type.is_some_and(|header| {
        header
            .value
            .as_str()
            .split(';')
            .next()
            .is_some_and(|media| media.trim().eq_ignore_ascii_case("application/json"))
    }) {
        let _ = request.respond(json_response(
            415,
            rpc_error(-32600, "Content-Type must be application/json"),
        ));
        return;
    }
    let accept = request
        .headers()
        .iter()
        .find(|header| header.field.equiv("Accept"));
    if accept.is_some_and(|header| {
        !header.value.as_str().split(',').any(|media| {
            matches!(
                media.split(';').next().unwrap_or_default().trim(),
                "application/json" | "application/*" | "*/*"
            )
        })
    }) {
        let _ = request.respond(json_response(
            406,
            rpc_error(-32600, "Accept must include application/json"),
        ));
        return;
    }

    let mut body = Vec::new();
    let received = request
        .as_reader()
        .take((MAX_HTTP_BODY + 1) as u64)
        .read_to_end(&mut body);
    if received.is_err() || body.len() > MAX_HTTP_BODY {
        let status = if body.len() > MAX_HTTP_BODY { 413 } else { 400 };
        let _ = request.respond(json_response(
            status,
            rpc_error(-32600, "MCP request unreadable or exceeds 64 KiB"),
        ));
        return;
    }
    let input = match serde_json::from_slice(&body) {
        Ok(input) => input,
        Err(_) => {
            let _ = request.respond(json_response(400, rpc_error(-32700, "Parse error")));
            return;
        }
    };
    match mcp::dispatch(input, |call| {
        execute_tool(&app, call, "http", send_imessage)
    }) {
        Some(output) => {
            let _ = request.respond(json_response(200, output.to_string()));
        }
        None => {
            let _ = request.respond(Response::empty(StatusCode(202)));
        }
    }
}

pub(super) fn serve_stdio(app: &App) -> std::io::Result<()> {
    mcp::serve_stdio(std::io::stdin().lock(), std::io::stdout().lock(), |call| {
        execute_tool(app, call, "stdio", send_imessage)
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn filtered_pages_advance_past_outgoing_and_empty_records() {
        let (path, writer) = crate::tests::scratch_database();
        let page = message_page(&path, Some(0), 1, true).unwrap();
        assert_eq!(page.messages.len(), 1);
        assert_eq!(page.next_cursor, 1);
        assert!(page.has_more);
        let next = message_page(&path, Some(page.next_cursor), 1, true).unwrap();
        assert!(next.messages.is_empty());
        assert_eq!(next.next_cursor, 3);
        assert!(!next.has_more);
        let recent = message_page(&path, None, 20, false).unwrap();
        assert_eq!(recent.messages.len(), 2);
        assert_eq!(recent.next_cursor, 3);
        drop(writer);
        fs::remove_file(path).unwrap();
    }

    #[test]
    fn wait_timeout_keeps_cursor_for_filtered_records() {
        let (path, writer) = crate::tests::scratch_database();
        let result = wait_messages(&path, 1, 20, true, 20).unwrap();
        assert_eq!(result["timed_out"], true);
        assert_eq!(result["next_cursor"], 3);
        assert_eq!(result["messages"].as_array().unwrap().len(), 0);
        drop(writer);
        fs::remove_file(path).unwrap();
    }

    #[test]
    fn mcp_send_routes_validated_recipient_and_text() {
        let app = App::from_env();
        let request = json!({"jsonrpc":"2.0", "id": 1, "method":"tools/call", "params": {
            "name":"imsg_send", "arguments": {"to":"agent@example.invalid", "text":"hello\n测试"}
        }});
        let mut sent = None;
        let result = mcp::dispatch(request, |call| {
            execute_tool(&app, call, "test", |to, text| {
                sent = Some((to.to_owned(), text.to_owned()));
                Ok(())
            })
        })
        .unwrap();
        assert_eq!(
            sent,
            Some(("agent@example.invalid".to_owned(), "hello\n测试".to_owned()))
        );
        assert_eq!(result["result"]["structuredContent"]["ok"], true);
    }
}
