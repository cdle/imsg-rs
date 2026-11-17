use imessage_core::typedstream::extract_text;
use rusqlite::{Connection, OpenFlags, Row};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fs::{self, OpenOptions};
use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream};
use std::os::unix::ffi::OsStrExt;
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::sync::atomic::{AtomicI64, AtomicU64, AtomicUsize, Ordering};
use std::sync::{mpsc, Arc, Mutex, MutexGuard};
use std::time::{Duration, Instant};
use tiny_http::{Header, Request, Response, Server, StatusCode};
use tungstenite::protocol::WebSocketConfig;
use tungstenite::{Error as WsError, Message, WebSocket};

const DEFAULT_HTTP_ADDR: &str = "127.0.0.1:8420";
const DEFAULT_WS_ADDR: &str = "127.0.0.1:8421";
const FALLBACK_INTERVAL: Duration = Duration::from_secs(5);
const SEND_TIMEOUT: Duration = Duration::from_secs(15);
const MAX_HTTP_BODY: usize = 64 * 1024;
const MAX_SEND_TEXT: usize = 32 * 1024;
const MAX_BATCH_ROWS: i64 = 1000;
const MAX_HTTP_MESSAGES: usize = 1000;
const WS_QUEUE_CAPACITY: usize = 256;
const MAX_HTTP_CONNECTIONS: usize = 32;
const MAX_WS_CONNECTIONS: usize = 64;
const WS_READ_PAUSE: Duration = Duration::from_millis(10);

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
struct Msg {
    rowid: i64,
    date: String,
    from_me: bool,
    chat_id: String,
    sender: Option<String>,
    has_attachments: bool,
    text: String,
}

#[derive(Deserialize)]
struct SendBody {
    to: String,
    text: String,
}

struct DbRow {
    rowid: i64,
    date: String,
    from_me: bool,
    text: Option<String>,
    attributed_body: Option<Vec<u8>>,
    chat_id: String,
    sender: Option<String>,
    has_attachments: bool,
}

struct App {
    db_path: PathBuf,
    cursor_path: PathBuf,
    log_path: PathBuf,
    cursor: AtomicI64,
    next_subscriber: AtomicU64,
    subscribers: Mutex<HashMap<u64, mpsc::SyncSender<String>>>,
    watcher_active: AtomicUsize,
    http_connections: Arc<AtomicUsize>,
    ws_connections: Arc<AtomicUsize>,
}

impl App {
    fn from_env() -> Self {
        let home = home_directory();
        let state_dir = home.join("Library/Application Support/imsg-rs");
        let cursor_path = env_path("IMSG_CURSOR_PATH", &state_dir.join("cursor.txt"));
        let cursor = read_cursor(&cursor_path);
        Self {
            db_path: env_path("IMSG_DB_PATH", &home.join("Library/Messages/chat.db")),
            cursor_path,
            log_path: env_path("IMSG_LOG_PATH", &state_dir.join("imsg-rs.log")),
            cursor: AtomicI64::new(cursor),
            next_subscriber: AtomicU64::new(1),
            subscribers: Mutex::new(HashMap::new()),
            watcher_active: AtomicUsize::new(0),
            http_connections: Arc::new(AtomicUsize::new(0)),
            ws_connections: Arc::new(AtomicUsize::new(0)),
        }
    }

    fn log(&self, line: &str) {
        append_log(&self.log_path, line);
    }
}

fn home_directory() -> PathBuf {
    if let Some(home) = std::env::var_os("HOME").filter(|value| !value.is_empty()) {
        return PathBuf::from(home);
    }

    unsafe {
        let account = libc::getpwuid(libc::getuid());
        if !account.is_null() && !(*account).pw_dir.is_null() {
            return PathBuf::from(std::ffi::OsStr::from_bytes(
                std::ffi::CStr::from_ptr((*account).pw_dir).to_bytes(),
            ));
        }
    }
    PathBuf::from(".")
}

fn env_path(key: &str, fallback: &Path) -> PathBuf {
    std::env::var_os(key)
        .filter(|value| !value.is_empty())
        .map(PathBuf::from)
        .unwrap_or_else(|| fallback.to_path_buf())
}

fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
}

fn append_log(path: &Path, line: &str) {
    if let Some(parent) = path.parent() {
        let _ = fs::create_dir_all(parent);
    }
    if let Ok(mut file) = OpenOptions::new().create(true).append(true).open(path) {
        let _ = writeln!(file, "{line}");
    }
}

fn read_cursor(path: &Path) -> i64 {
    fs::read_to_string(path)
        .ok()
        .and_then(|value| value.trim().parse::<i64>().ok())
        .filter(|value| *value >= 0)
        .unwrap_or(0)
}

fn write_cursor(path: &Path, value: i64) -> Result<(), String> {
    let parent = path
        .parent()
        .ok_or_else(|| "cursor path has no parent".to_string())?;
    fs::create_dir_all(parent).map_err(|error| format!("create cursor directory: {error}"))?;
    let temporary = path.with_extension("tmp");
    let mut file = OpenOptions::new()
        .create(true)
        .truncate(true)
        .write(true)
        .open(&temporary)
        .map_err(|error| format!("open cursor temporary file: {error}"))?;
    writeln!(file, "{value}").map_err(|error| format!("write cursor: {error}"))?;
    file.sync_all()
        .map_err(|error| format!("sync cursor: {error}"))?;
    fs::rename(&temporary, path).map_err(|error| format!("replace cursor: {error}"))
}

fn open_database(path: &Path) -> Result<Connection, String> {
    let connection = Connection::open_with_flags(path, OpenFlags::SQLITE_OPEN_READ_ONLY)
        .map_err(|error| format!("open chat database: {error}"))?;
    connection
        .busy_timeout(Duration::from_secs(2))
        .map_err(|error| format!("configure chat database: {error}"))?;
    Ok(connection)
}

fn map_db_row(row: &Row<'_>) -> rusqlite::Result<DbRow> {
    let sender = row
        .get::<_, Option<String>>(6)?
        .filter(|value| !value.is_empty());
    Ok(DbRow {
        rowid: row.get(0)?,
        date: row.get::<_, Option<String>>(1)?.unwrap_or_default(),
        from_me: row.get::<_, i64>(2)? != 0,
        text: row.get(3)?,
        attributed_body: row.get(4)?,
        chat_id: row.get::<_, Option<String>>(5)?.unwrap_or_default(),
        sender,
        has_attachments: row.get::<_, i64>(7)? != 0,
    })
}

fn decode_row(row: DbRow) -> Option<Msg> {
    let text = row
        .text
        .filter(|value| !value.is_empty())
        .or_else(|| row.attributed_body.as_deref().and_then(extract_text))?;
    if text.is_empty() {
        return None;
    }
    Some(Msg {
        rowid: row.rowid,
        date: row.date,
        from_me: row.from_me,
        chat_id: row.chat_id,
        sender: row.sender,
        has_attachments: row.has_attachments,
        text,
    })
}

const QUERY_ROWS_SELECT: &str = "SELECT m.ROWID,
        datetime(m.date / 1000000000 + 978307200, 'unixepoch', 'localtime'),
        m.is_from_me,
        m.text,
        m.attributedBody,
        COALESCE((SELECT c.guid
                  FROM chat_message_join cmj
                  JOIN chat c ON c.ROWID = cmj.chat_id
                  WHERE cmj.message_id = m.ROWID
                  ORDER BY c.ROWID LIMIT 1),
                 (SELECT c.chat_identifier
                  FROM chat_message_join cmj
                  JOIN chat c ON c.ROWID = cmj.chat_id
                  WHERE cmj.message_id = m.ROWID
                  ORDER BY c.ROWID LIMIT 1), ''),
        (SELECT h.id FROM handle h WHERE h.ROWID = m.handle_id),
        COALESCE(m.cache_has_attachments, 0)
 FROM message m";

fn query_rows(path: &Path, since: i64, limit: i64) -> Result<Vec<DbRow>, String> {
    query_rows_ordered(path, since, limit, false)
}

fn query_rows_ordered(
    path: &Path,
    cursor: i64,
    limit: i64,
    descending: bool,
) -> Result<Vec<DbRow>, String> {
    let connection = open_database(path)?;
    let comparison = if descending { "<=" } else { ">" };
    let direction = if descending { "DESC" } else { "ASC" };
    let query = format!(
        "{QUERY_ROWS_SELECT} WHERE m.ROWID {comparison} ?1 ORDER BY m.ROWID {direction} LIMIT ?2"
    );
    let mut statement = connection
        .prepare(&query)
        .map_err(|error| format!("prepare chat query: {error}"))?;
    let rows = statement
        .query_map([cursor, limit], map_db_row)
        .map_err(|error| format!("query chat database: {error}"))?;
    rows.collect::<rusqlite::Result<Vec<_>>>()
        .map_err(|error| format!("read chat row: {error}"))
}

fn latest_rowid(path: &Path) -> Result<i64, String> {
    let connection = open_database(path)?;
    connection
        .query_row("SELECT COALESCE(MAX(ROWID), 0) FROM message", [], |row| {
            row.get(0)
        })
        .map_err(|error| format!("read latest message row: {error}"))
}

fn messages_since(path: &Path, since: i64, limit: usize) -> Result<Vec<Msg>, String> {
    let mut messages = Vec::new();
    let mut cursor = since;
    loop {
        let rows = query_rows(path, cursor, MAX_BATCH_ROWS)?;
        let row_count = rows.len();
        for row in rows {
            cursor = row.rowid;
            if let Some(message) = decode_row(row) {
                messages.push(message);
                if messages.len() == limit {
                    return Ok(messages);
                }
            }
        }
        if row_count < MAX_BATCH_ROWS as usize {
            break;
        }
    }
    Ok(messages)
}

fn recent_messages(path: &Path, count: usize) -> Result<Vec<Msg>, String> {
    let mut messages = Vec::with_capacity(count);
    let mut cursor = latest_rowid(path)?;
    loop {
        let rows = query_rows_ordered(path, cursor, MAX_BATCH_ROWS, true)?;
        let row_count = rows.len();
        for row in rows {
            cursor = row.rowid.saturating_sub(1);
            if let Some(message) = decode_row(row) {
                messages.push(message);
                if messages.len() == count {
                    messages.reverse();
                    return Ok(messages);
                }
            }
        }
        if row_count < MAX_BATCH_ROWS as usize || cursor == 0 {
            break;
        }
    }
    messages.reverse();
    Ok(messages)
}

fn broadcast(app: &App, message: &Msg) {
    let Ok(payload) = serde_json::to_string(message) else {
        app.log("could not serialize a message for WebSocket delivery");
        return;
    };
    lock(&app.subscribers).retain(|_, subscriber| match subscriber.try_send(payload.clone()) {
        Ok(()) => true,
        Err(mpsc::TrySendError::Disconnected(_)) => false,
        Err(mpsc::TrySendError::Full(_)) => false,
    });
}

fn process_new_messages(app: &App, cursor: &mut i64) -> bool {
    match query_rows(&app.db_path, *cursor, MAX_BATCH_ROWS) {
        Ok(rows) => {
            let more_pending = rows.len() == MAX_BATCH_ROWS as usize;
            if let Some(last) = rows.last() {
                let next_cursor = last.rowid;
                let mut delivered = 0usize;
                for row in rows {
                    if let Some(message) = decode_row(row) {
                        broadcast(app, &message);
                        delivered += 1;
                    }
                }
                if next_cursor > *cursor {
                    if let Err(error) = write_cursor(&app.cursor_path, next_cursor) {
                        app.log(&format!("could not persist message cursor: {error}"));
                    }
                    *cursor = next_cursor;
                    app.cursor.store(next_cursor, Ordering::Release);
                }
                app.log(&format!(
                    "scanned through rowid={next_cursor}, delivered={delivered}"
                ));
            }
            more_pending
        }
        Err(error) => {
            app.log(&format!("message scan failed: {error}"));
            false
        }
    }
}

fn is_chat_database_event(event: &notify::Event) -> bool {
    event.paths.iter().any(|path| {
        matches!(
            path.file_name().and_then(|name| name.to_str()),
            Some("chat.db" | "chat.db-wal" | "chat.db-shm")
        )
    })
}

fn receive_loop(app: Arc<App>) {
    use notify::{RecursiveMode, Watcher};

    let (event_tx, event_rx) = mpsc::channel();
    let mut watcher = match notify::recommended_watcher(event_tx) {
        Ok(watcher) => Some(watcher),
        Err(error) => {
            app.log(&format!(
                "file watcher unavailable; using fallback scan: {error}"
            ));
            None
        }
    };
    if let Some(active_watcher) = watcher.as_mut() {
        let parent = app.db_path.parent().unwrap_or_else(|| Path::new("/"));
        match active_watcher.watch(parent, RecursiveMode::NonRecursive) {
            Ok(()) => {
                app.watcher_active.store(1, Ordering::Release);
                app.log("watching the Messages database directory");
            }
            Err(error) => {
                app.log(&format!("file watch failed; using fallback scan: {error}"));
                watcher = None;
            }
        }
    }

    let mut cursor = app.cursor.load(Ordering::Acquire);
    loop {
        if process_new_messages(&app, &mut cursor) {
            continue;
        }
        if watcher.is_none() {
            std::thread::sleep(FALLBACK_INTERVAL);
            continue;
        }
        match event_rx.recv_timeout(FALLBACK_INTERVAL) {
            Ok(Ok(event)) if is_chat_database_event(&event) => {
                let mut watcher_error = None;
                loop {
                    match event_rx.try_recv() {
                        Ok(Ok(_)) => {}
                        Ok(Err(error)) => {
                            watcher_error = Some(error);
                            break;
                        }
                        Err(_) => break,
                    }
                }
                if let Some(error) = watcher_error {
                    app.log(&format!("file watcher reported an error: {error}"));
                    app.watcher_active.store(0, Ordering::Release);
                    watcher = None;
                }
            }
            Ok(Ok(_)) => continue,
            Ok(Err(error)) => {
                app.log(&format!("file watcher reported an error: {error}"));
                app.watcher_active.store(0, Ordering::Release);
                watcher = None;
            }
            Err(mpsc::RecvTimeoutError::Timeout) => {}
            Err(mpsc::RecvTimeoutError::Disconnected) => {
                app.log("file watcher stopped; using fallback scan");
                app.watcher_active.store(0, Ordering::Release);
                watcher = None;
            }
        }
    }
}

fn parse_since(query: Option<&str>) -> Result<Option<i64>, String> {
    let Some(query) = query else {
        return Ok(None);
    };
    for pair in query.split('&') {
        if let Some((key, value)) = pair.split_once('=') {
            if key == "since" {
                let parsed = value
                    .parse::<i64>()
                    .map_err(|_| "since must be a non-negative row id".to_string())?;
                if parsed < 0 {
                    return Err("since must be a non-negative row id".to_string());
                }
                return Ok(Some(parsed));
            }
        }
    }
    Ok(None)
}

fn json_response(status: u16, payload: String) -> Response<std::io::Cursor<Vec<u8>>> {
    let content_type = Header::from_bytes("Content-Type", "application/json; charset=utf-8")
        .expect("valid content type header");
    Response::from_string(payload)
        .with_status_code(StatusCode(status))
        .with_header(content_type)
}

fn error_json(message: &str) -> String {
    serde_json::json!({"error": message}).to_string()
}

fn origin_is_local(origin: &str) -> bool {
    let Ok(uri) = origin.parse::<tungstenite::http::Uri>() else {
        return false;
    };
    if !matches!(uri.scheme_str(), Some("http" | "https")) {
        return false;
    }
    matches!(
        uri.host().map(str::to_ascii_lowercase).as_deref(),
        Some("localhost" | "127.0.0.1" | "::1" | "[::1]")
    )
}

fn http_origin_allowed(request: &Request) -> bool {
    request
        .headers()
        .iter()
        .find(|header| header.field.equiv("Origin"))
        .is_none_or(|header| origin_is_local(header.value.as_str()))
}

fn take_connection_slot(counter: &AtomicUsize, max: usize) -> bool {
    let mut current = counter.load(Ordering::Acquire);
    loop {
        if current >= max {
            return false;
        }
        match counter.compare_exchange_weak(
            current,
            current + 1,
            Ordering::AcqRel,
            Ordering::Acquire,
        ) {
            Ok(_) => return true,
            Err(actual) => current = actual,
        }
    }
}

struct ConnectionSlot(Arc<AtomicUsize>);

impl Drop for ConnectionSlot {
    fn drop(&mut self) {
        self.0.fetch_sub(1, Ordering::AcqRel);
    }
}

fn serve_http(mut request: Request, app: Arc<App>) {
    if !http_origin_allowed(&request) {
        let _ = request.respond(json_response(403, error_json("origin is not allowed")));
        return;
    }
    let path_and_query = request.url().to_string();
    let (path, query) = path_and_query
        .split_once('?')
        .map_or((path_and_query.as_str(), None), |(path, query)| {
            (path, Some(query))
        });
    let method = request.method().as_str();
    let (status, payload) = match (method, path) {
        ("GET", "/health") => match latest_rowid(&app.db_path) {
            Ok(latest) => (
                200,
                serde_json::json!({
                    "ok": true,
                    "watcher": app.watcher_active.load(Ordering::Acquire) == 1,
                    "cursor": app.cursor.load(Ordering::Acquire),
                    "latest_rowid": latest
                })
                .to_string(),
            ),
            Err(error) => (503, error_json(&error)),
        },
        ("GET", "/messages") => match parse_since(query) {
            Err(error) => (400, error_json(&error)),
            Ok(Some(since)) => match messages_since(&app.db_path, since, MAX_HTTP_MESSAGES) {
                Ok(messages) => (
                    200,
                    serde_json::to_string(&messages).unwrap_or_else(|_| "[]".to_string()),
                ),
                Err(error) => (500, error_json(&error)),
            },
            Ok(None) => match recent_messages(&app.db_path, 20) {
                Ok(messages) => (
                    200,
                    serde_json::to_string(&messages).unwrap_or_else(|_| "[]".to_string()),
                ),
                Err(error) => (500, error_json(&error)),
            },
        },
        ("POST", "/send") => {
            let mut body = Vec::new();
            match request
                .as_reader()
                .take((MAX_HTTP_BODY + 1) as u64)
                .read_to_end(&mut body)
            {
                Err(error) => (400, error_json(&format!("could not read request: {error}"))),
                Ok(_) if body.len() > MAX_HTTP_BODY => {
                    (413, error_json("request body exceeds 64 KiB"))
                }
                Ok(_) => match serde_json::from_slice::<SendBody>(&body) {
                    Err(error) => (400, error_json(&format!("bad JSON: {error}"))),
                    Ok(body) => match validate_send_body(&body) {
                        Err(error) => (400, error_json(&error)),
                        Ok(()) => match send_imessage(&body.to, &body.text) {
                            Ok(()) => (200, r#"{"ok":true}"#.to_string()),
                            Err(error) => (502, error_json(&error)),
                        },
                    },
                },
            }
        }
        _ => (404, error_json("not found")),
    };
    let _ = request.respond(json_response(status, payload));
}

fn validate_send_body(body: &SendBody) -> Result<(), String> {
    if body.to.trim().is_empty() || body.to.chars().count() > 320 {
        return Err("to must be a non-empty iMessage address (max 320 characters)".to_string());
    }
    if body.to.chars().any(char::is_control) {
        return Err("to cannot contain control characters".to_string());
    }
    if body.text.len() > MAX_SEND_TEXT {
        return Err("text exceeds 32 KiB".to_string());
    }
    if body
        .text
        .chars()
        .any(|character| character.is_control() && !matches!(character, '\n' | '\r' | '\t'))
    {
        return Err("text contains an unsupported control character".to_string());
    }
    Ok(())
}

fn apple_script_literal(value: &str) -> String {
    let mut quoted = String::with_capacity(value.len() + 2);
    quoted.push('"');
    for character in value.chars() {
        match character {
            '\\' => quoted.push_str("\\\\"),
            '"' => quoted.push_str("\\\""),
            '\n' => quoted.push_str("\\n"),
            '\r' => quoted.push_str("\\r"),
            '\t' => quoted.push_str("\\t"),
            other => quoted.push(other),
        }
    }
    quoted.push('"');
    quoted
}

fn send_imessage(to: &str, text: &str) -> Result<(), String> {
    let script = format!(
        "tell application \"Messages\"\n\
         set svc to first service whose service type is iMessage\n\
         set part to participant {} of svc\n\
         send {} to part\n\
         end tell\n",
        apple_script_literal(to),
        apple_script_literal(text),
    );
    let mut child = Command::new("/usr/bin/ssh")
        .args([
            "-o",
            "BatchMode=yes",
            "-o",
            "ConnectTimeout=5",
            "-o",
            "ConnectionAttempts=1",
            "localhost",
            "/usr/bin/osascript",
            "-",
        ])
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .map_err(|error| format!("start ssh/osascript: {error}"))?;
    let stdout = child.stdout.take().ok_or("missing ssh stdout")?;
    let stderr = child.stderr.take().ok_or("missing ssh stderr")?;
    let stdout_reader = std::thread::spawn(move || read_capped(stdout));
    let stderr_reader = std::thread::spawn(move || read_capped(stderr));
    let started = Instant::now();
    if let Some(mut stdin) = child.stdin.take() {
        if let Err(error) = stdin.write_all(script.as_bytes()) {
            let _ = child.kill();
            let _ = child.wait();
            return Err(format!("write osascript input: {error}"));
        }
    }

    loop {
        match child.try_wait() {
            Ok(Some(status)) => {
                let output = stdout_reader.join().unwrap_or_default();
                let error = stderr_reader.join().unwrap_or_default();
                if status.success() {
                    return Ok(());
                }
                let detail = String::from_utf8_lossy(&error).trim().to_string();
                let detail = if detail.is_empty() {
                    String::from_utf8_lossy(&output).trim().to_string()
                } else {
                    detail
                };
                return Err(format!("osascript failed ({status}): {detail}"));
            }
            Ok(None) if started.elapsed() < SEND_TIMEOUT => {
                std::thread::sleep(Duration::from_millis(100));
            }
            Ok(None) => {
                let _ = child.kill();
                let _ = child.wait();
                let _ = stdout_reader.join();
                let _ = stderr_reader.join();
                return Err(format!(
                    "osascript timed out after {} seconds",
                    SEND_TIMEOUT.as_secs()
                ));
            }
            Err(error) => {
                let _ = child.kill();
                let _ = child.wait();
                let _ = stdout_reader.join();
                let _ = stderr_reader.join();
                return Err(format!("wait for osascript: {error}"));
            }
        }
    }
}

fn read_capped(mut reader: impl Read) -> Vec<u8> {
    let mut output = Vec::new();
    let _ = reader.by_ref().take(16 * 1024).read_to_end(&mut output);
    output
}

fn parse_ws_since(
    request: &tungstenite::handshake::server::Request,
) -> Result<Option<i64>, String> {
    let path = request.uri().path();
    if path != "/" && path != "/ws" {
        return Err("WebSocket endpoint is /ws".to_string());
    }
    parse_since(request.uri().query())
}

fn websocket_loop(app: Arc<App>, address: String) {
    let listener = match TcpListener::bind(&address) {
        Ok(listener) => listener,
        Err(error) => {
            app.log(&format!("WebSocket bind failed on {address}: {error}"));
            return;
        }
    };
    app.log(&format!("WebSocket listening on {address}"));
    for stream in listener.incoming() {
        let Ok(stream) = stream else {
            continue;
        };
        if !take_connection_slot(&app.ws_connections, MAX_WS_CONNECTIONS) {
            drop(stream);
            continue;
        }
        let connection_app = Arc::clone(&app);
        let connection_slot = ConnectionSlot(Arc::clone(&app.ws_connections));
        let _ = std::thread::Builder::new()
            .name("imsg-ws-client".to_string())
            .spawn(move || {
                let _slot = connection_slot;
                serve_websocket(stream, connection_app);
            });
    }
}

fn serve_websocket(stream: TcpStream, app: Arc<App>) {
    let _ = stream.set_nodelay(true);
    let _ = stream.set_read_timeout(Some(Duration::from_secs(5)));
    let _ = stream.set_write_timeout(Some(Duration::from_secs(5)));
    let requested_since = Arc::new(Mutex::new(Ok(None)));
    let since_capture = Arc::clone(&requested_since);
    let config = WebSocketConfig {
        max_message_size: Some(64 * 1024),
        max_frame_size: Some(64 * 1024),
        ..WebSocketConfig::default()
    };
    let mut websocket = match tungstenite::accept_hdr_with_config(
        stream,
        move |request: &tungstenite::handshake::server::Request,
              response: tungstenite::handshake::server::Response| {
            let origin = request.headers().get("origin");
            let since = match origin {
                Some(origin) if !origin.to_str().is_ok_and(origin_is_local) => {
                    Err("origin is not allowed".to_string())
                }
                _ => parse_ws_since(request),
            };
            *lock(&since_capture) = since.clone();
            since.map(|_| response).map_err(|error| {
                tungstenite::http::Response::builder()
                    .status(400)
                    .body(Some(error))
                    .expect("valid WebSocket error response")
            })
        },
        Some(config),
    ) {
        Ok(websocket) => websocket,
        Err(error) => {
            app.log(&format!("WebSocket handshake failed: {error}"));
            return;
        }
    };
    let _ = websocket.get_mut().set_read_timeout(None);
    let _ = websocket
        .get_mut()
        .set_write_timeout(Some(Duration::from_secs(2)));
    let _ = websocket.get_mut().set_nonblocking(true);

    let since = lock(&requested_since)
        .clone()
        .unwrap_or(None)
        .unwrap_or_else(|| app.cursor.load(Ordering::Acquire));
    let id = app.next_subscriber.fetch_add(1, Ordering::Relaxed);
    let (sender, receiver) = mpsc::sync_channel(WS_QUEUE_CAPACITY);
    lock(&app.subscribers).insert(id, sender);

    let replay = match messages_since(&app.db_path, since, MAX_HTTP_MESSAGES + 1) {
        Ok(messages) => messages,
        Err(error) => {
            let _ = websocket.send(Message::Text(error_json(&error)));
            lock(&app.subscribers).remove(&id);
            return;
        }
    };
    let mut last_sent = since;
    let replay_truncated = replay.len() > MAX_HTTP_MESSAGES;
    for message in replay.into_iter().take(MAX_HTTP_MESSAGES) {
        if message.rowid <= last_sent {
            continue;
        }
        if send_ws_message(&mut websocket, &message).is_err() {
            lock(&app.subscribers).remove(&id);
            return;
        }
        last_sent = message.rowid;
    }
    if replay_truncated {
        let payload = serde_json::json!({
            "type": "resync_required",
            "since": last_sent,
            "reason": "replay exceeds 1000 messages; continue with GET /messages?since=<rowid>"
        })
        .to_string();
        let _ = websocket.send(Message::Text(payload));
        lock(&app.subscribers).remove(&id);
        return;
    }

    loop {
        match websocket.read() {
            Ok(Message::Close(_)) => break,
            Ok(_) => {
                if websocket.flush().is_err() {
                    break;
                }
            }
            Err(WsError::Io(error)) if error.kind() == std::io::ErrorKind::WouldBlock => {}
            Err(WsError::Io(error)) if error.kind() == std::io::ErrorKind::Interrupted => continue,
            Err(WsError::ConnectionClosed | WsError::AlreadyClosed) => break,
            Err(error) => {
                app.log(&format!("WebSocket client closed: {error}"));
                break;
            }
        }

        let mut channel_closed = false;
        loop {
            match receiver.try_recv() {
                Ok(payload) => {
                    let Ok(message) = serde_json::from_str::<Msg>(&payload) else {
                        continue;
                    };
                    if message.rowid <= last_sent {
                        continue;
                    }
                    if websocket.send(Message::Text(payload)).is_err() {
                        channel_closed = true;
                        break;
                    }
                    last_sent = message.rowid;
                }
                Err(mpsc::TryRecvError::Empty) => break,
                Err(mpsc::TryRecvError::Disconnected) => {
                    channel_closed = true;
                    break;
                }
            }
        }
        if channel_closed {
            let payload = serde_json::json!({
                "type": "resync_required",
                "since": last_sent,
                "reason": "subscriber queue overflowed or closed"
            })
            .to_string();
            let _ = websocket.send(Message::Text(payload));
            break;
        }
        std::thread::sleep(WS_READ_PAUSE);
    }

    lock(&app.subscribers).remove(&id);
}

fn send_ws_message(websocket: &mut WebSocket<TcpStream>, message: &Msg) -> Result<(), WsError> {
    websocket.send(Message::Text(
        serde_json::to_string(message).expect("message serialization is infallible"),
    ))
}

fn main() {
    let app = Arc::new(App::from_env());
    app.log("imsg-rs starting");

    let receiver_app = Arc::clone(&app);
    let _ = std::thread::Builder::new()
        .name("imsg-receiver".to_string())
        .spawn(move || receive_loop(receiver_app));

    let ws_app = Arc::clone(&app);
    let ws_address = std::env::var("IMSG_WS_ADDR").unwrap_or_else(|_| DEFAULT_WS_ADDR.to_string());
    let _ = std::thread::Builder::new()
        .name("imsg-websocket".to_string())
        .spawn(move || websocket_loop(ws_app, ws_address));

    let http_address =
        std::env::var("IMSG_HTTP_ADDR").unwrap_or_else(|_| DEFAULT_HTTP_ADDR.to_string());
    let server = Server::http(&http_address).unwrap_or_else(|error| {
        panic!("bind HTTP server on {http_address}: {error}");
    });
    app.log(&format!("HTTP listening on {http_address}"));

    for request in server.incoming_requests() {
        if !take_connection_slot(&app.http_connections, MAX_HTTP_CONNECTIONS) {
            let _ = request.respond(json_response(503, error_json("server is busy")));
            continue;
        }
        let request_app = Arc::clone(&app);
        let connection_slot = ConnectionSlot(Arc::clone(&app.http_connections));
        let _ = std::thread::Builder::new()
            .name("imsg-http-request".to_string())
            .spawn(move || {
                let _slot = connection_slot;
                serve_http(request, request_app);
            });
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::AtomicUsize;

    fn scratch_database() -> (PathBuf, Connection) {
        static NEXT_DB: AtomicUsize = AtomicUsize::new(0);
        let path = std::env::temp_dir().join(format!(
            "imsg-rs-test-{}-{}.sqlite",
            std::process::id(),
            NEXT_DB.fetch_add(1, Ordering::Relaxed)
        ));
        let connection = Connection::open(&path).unwrap();
        connection
            .execute_batch(
                "CREATE TABLE message (
                    ROWID INTEGER PRIMARY KEY,
                    date INTEGER NOT NULL,
                    is_from_me INTEGER NOT NULL,
                    text TEXT,
                    attributedBody BLOB,
                    handle_id INTEGER,
                    cache_has_attachments INTEGER
                );
                CREATE TABLE chat (ROWID INTEGER PRIMARY KEY, guid TEXT, chat_identifier TEXT);
                CREATE TABLE chat_message_join (chat_id INTEGER, message_id INTEGER);
                CREATE TABLE handle (ROWID INTEGER PRIMARY KEY, id TEXT);
                INSERT INTO chat VALUES (1, 'iMessage;-;chat-1', 'contact@example.com');
                INSERT INTO handle VALUES (1, 'contact@example.com');
                INSERT INTO message VALUES (1, 700000000000000000, 0, 'plain', NULL, 1, 0);
                INSERT INTO message VALUES (3, 700000000000000002, 0, NULL, NULL, 1, 0);
                INSERT INTO chat_message_join VALUES (1, 1);
                INSERT INTO chat_message_join VALUES (1, 2);
                INSERT INTO chat_message_join VALUES (1, 3);",
            )
            .unwrap();
        let attributed_body = hex_to_bytes(
            "040B73747265616D747970656481E803840140848484124E5341747472696275746564537472696E67008484084E534F626A656374008592848484084E53537472696E67019484012B3248612120546865204C69204B612D7368696E6720636C61737369632E20576F72647320746F206C69766520627920F09F988486840269490130928484840C4E5344696374696F6E617279009484016901928496961D5F5F6B494D4D657373616765506172744174747269627574654E616D658692848484084E534E756D626572008484074E5356616C7565009484012A84999900868686",
        );
        connection
            .execute(
                "INSERT INTO message VALUES (2, 700000000000000001, 1, '', ?1, 1, 0)",
                [attributed_body],
            )
            .unwrap();
        (path, connection)
    }

    fn hex_to_bytes(hex: &str) -> Vec<u8> {
        (0..hex.len())
            .step_by(2)
            .map(|index| u8::from_str_radix(&hex[index..index + 2], 16).unwrap())
            .collect()
    }

    #[test]
    fn scans_text_and_attributed_body_while_advancing_over_empty_rows() {
        let (path, connection) = scratch_database();
        drop(connection);
        let rows = query_rows(&path, 0, MAX_BATCH_ROWS).unwrap();
        let high_water = rows.last().unwrap().rowid;
        let messages = rows.into_iter().filter_map(decode_row).collect::<Vec<_>>();
        assert_eq!(high_water, 3);
        assert_eq!(messages.len(), 2);
        assert_eq!(messages[0].text, "plain");
        assert_eq!(
            messages[1].text,
            "Ha! The Li Ka-shing classic. Words to live by 😄"
        );
        assert_eq!(messages[0].chat_id, "iMessage;-;chat-1");
        assert_eq!(messages[0].sender.as_deref(), Some("contact@example.com"));
        let _ = fs::remove_file(path);
    }

    #[test]
    fn broadcasts_to_each_client_and_drops_a_slow_client() {
        let app = App {
            db_path: PathBuf::new(),
            cursor_path: PathBuf::new(),
            log_path: std::env::temp_dir().join("imsg-rs-broadcast-test.log"),
            cursor: AtomicI64::new(0),
            next_subscriber: AtomicU64::new(1),
            subscribers: Mutex::new(HashMap::new()),
            watcher_active: AtomicUsize::new(0),
            http_connections: Arc::new(AtomicUsize::new(0)),
            ws_connections: Arc::new(AtomicUsize::new(0)),
        };
        let (slow_sender, _slow_receiver) = mpsc::sync_channel(1);
        let (fast_sender, fast_receiver) = mpsc::sync_channel(2);
        lock(&app.subscribers).insert(1, slow_sender);
        lock(&app.subscribers).insert(2, fast_sender);
        let message = Msg {
            rowid: 1,
            date: String::new(),
            from_me: false,
            chat_id: String::new(),
            sender: None,
            has_attachments: false,
            text: "hello".to_string(),
        };

        broadcast(&app, &message);
        assert_eq!(
            fast_receiver.try_recv().unwrap(),
            serde_json::to_string(&message).unwrap()
        );
        broadcast(&app, &message);
        assert_eq!(
            fast_receiver.try_recv().unwrap(),
            serde_json::to_string(&message).unwrap()
        );
        assert_eq!(lock(&app.subscribers).len(), 1);
    }

    #[test]
    fn parses_replay_cursors_without_silently_accepting_invalid_values() {
        assert_eq!(parse_since(None).unwrap(), None);
        assert_eq!(parse_since(Some("since=42&unused=x")).unwrap(), Some(42));
        assert!(parse_since(Some("since=wat")).is_err());
        assert!(parse_since(Some("since=-1")).is_err());
    }

    #[test]
    fn apple_script_literals_escape_quotes_and_line_breaks() {
        assert_eq!(apple_script_literal("a\\b\"c\nd"), "\"a\\\\b\\\"c\\nd\"");
    }

    #[test]
    fn accepts_only_local_web_origins() {
        assert!(origin_is_local("http://localhost:8420"));
        assert!(origin_is_local("https://127.0.0.1:8421"));
        assert!(!origin_is_local("https://localhost.example"));
        assert!(!origin_is_local("null"));
        assert!(!origin_is_local("file://localhost"));
    }
}
