use rusqlite::Connection;
use serde::Deserialize;
use std::fs::{self, File, OpenOptions};
use std::io::Write;
use std::path::PathBuf;
use std::process::{Command, Stdio};
use std::time::Duration;
use tiny_http::{Header, Response, Server};

const DB_PATH: &str = "$HOME/Library/Messages/chat.db";
const CURSOR_PATH: &str = "$PROJECT_ROOT/cursor.txt";
const ACCOUNT_ID: &str = "ACCOUNT_ID";
const FALLBACK_POLL_INTERVAL: Duration = Duration::from_secs(5);
const WS_PORT: u16 = 8421;
const SEND_TIMEOUT_SECS: u64 = 15;

lazy_static::lazy_static! {
    static ref WS_BROADCAST: (crossbeam_channel::Sender<String>, crossbeam_channel::Receiver<String>) = {
        crossbeam_channel::bounded::<String>(1024)
    };
}

#[derive(Deserialize)]
struct SendBody {
    to: String,
    text: String,
}

#[derive(serde::Serialize)]
struct Msg {
    rowid: i64,
    date: String,
    from_me: bool,
    text: String,
}

fn read_cursor() -> i64 {
    fs::read_to_string(CURSOR_PATH)
        .ok()
        .and_then(|s| s.trim().parse().ok())
        .unwrap_or(0)
}

fn write_cursor(v: i64) {
    if let Ok(mut f) = File::create(CURSOR_PATH) {
        let _ = writeln!(f, "{}", v);
    }
}

fn poll_once(cur: i64) -> Result<Vec<Msg>, String> {
    let conn = Connection::open_with_flags(DB_PATH, rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY)
        .map_err(|e| e.to_string())?;
    let mut stmt = conn
        .prepare(
            "SELECT m.rowid,
                    datetime(m.date/1000000000 + 978307200, 'unixepoch', 'localtime'),
                    m.is_from_me,
                    COALESCE(m.text, '')
             FROM message m
             WHERE m.rowid > ?1 AND COALESCE(m.text, '') != ''
             ORDER BY m.rowid ASC",
        )
        .map_err(|e| e.to_string())?;
    let rows = stmt
        .query_map([cur], |r| {
            Ok(Msg {
                rowid: r.get(0)?,
                date: r.get(1)?,
                from_me: r.get::<_, i64>(2)? != 0,
                text: r.get(3)?,
            })
        })
        .map_err(|e| e.to_string())?;
    let mut out = Vec::new();
    for row in rows {
        out.push(row.map_err(|e| e.to_string())?);
    }
    Ok(out)
}

/// Run osascript over `ssh localhost` so the responsible process is sshd,
/// avoiding TCC attribution issues of nested GUI app launch chains.
fn send_imessage(to: &str, text: &str) -> Result<(), String> {
    let script = format!(
        "tell application \"Messages\"\n\
         set acct to first account whose id is \"{}\"\n\
         set part to participant \"{}\" of acct\n\
         send \"{}\" to part\n\
         end tell",
        ACCOUNT_ID,
        to.replace('\\', "\\\\").replace('"', "\\\""),
        text.replace('\\', "\\\\").replace('"', "\\\""),
    );
    let tmp = "/tmp/imsg-rs-send.scpt";
    fs::write(tmp, &script).map_err(|e| format!("write script: {e}"))?;
    let mut child = Command::new("/usr/bin/ssh")
        .args([
            "-o", "BatchMode=yes",
            "-o", "ConnectTimeout=5",
            "localhost",
            "/usr/bin/osascript",
            tmp,
        ])
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .map_err(|e| format!("spawn ssh: {e}"))?;
    let deadline = std::time::Instant::now() + Duration::from_secs(SEND_TIMEOUT_SECS);
    loop {
        match child.try_wait() {
            Ok(Some(status)) => {
                if status.success() {
                    return Ok(());
                }
                let mut err = String::new();
                if let Some(mut e) = child.stderr.take() {
                    use std::io::Read;
                    let _ = e.read_to_string(&mut err);
                }
                return Err(format!("osascript failed: {}: {}", status, err.trim()));
            }
            Ok(None) => {
                if std::time::Instant::now() > deadline {
                    let _ = child.kill();
                    let _ = child.wait();
                    return Err(format!("osascript timed out after {SEND_TIMEOUT_SECS}s"));
                }
                std::thread::sleep(Duration::from_millis(100));
            }
            Err(e) => return Err(format!("wait: {e}")),
        }
    }
}

fn append_log(line: &str) {
    let log_path = PathBuf::from("$PROJECT_ROOT/imsg-rs.log");
    if let Ok(mut f) = OpenOptions::new().create(true).append(true).open(&log_path) {
        let _ = writeln!(f, "{}", line);
    }
}

fn process_new_messages() {
    let cur = read_cursor();
    match poll_once(cur) {
        Ok(msgs) => {
            if !msgs.is_empty() {
                for m in &msgs {
                    append_log(&format!(
                        "recv rowid={} from_me={} text={:?}",
                        m.rowid, m.from_me, m.text
                    ));
                    if let Ok(json) = serde_json::to_string(m) {
                        let _ = WS_BROADCAST.0.send(json);
                    }
                }
                write_cursor(msgs.last().unwrap().rowid);
            }
        }
        Err(e) => append_log(&format!("poll error: {e}")),
    }
}

fn main() {
    let server = Server::http("127.0.0.1:8420").expect("bind 127.0.0.1:8420");
    append_log("imsg-rs started on 127.0.0.1:8420");
    // WebSocket server on a separate port. Each accepted connection gets a
    // tungstenite WebSocket over the raw TcpStream, then subscribes to the
    // broadcast channel for new messages.
    std::thread::spawn(move || {
        use std::net::TcpListener;
        use tungstenite::{accept as ws_accept, Message};
        let listener = match TcpListener::bind(("127.0.0.1", WS_PORT)) {
            Ok(l) => l,
            Err(e) => {
                append_log(&format!("ws bind failed: {e}"));
                return;
            }
        };
        append_log(&format!("ws listening on 127.0.0.1:{WS_PORT}"));
        for stream in listener.incoming() {
            let stream = match stream {
                Ok(s) => s,
                Err(_) => continue,
            };
            let _ = stream.set_nodelay(true);
            let mut ws = match ws_accept(stream) {
                Ok(ws) => ws,
                Err(e) => {
                    append_log(&format!("ws handshake failed: {e}"));
                    continue;
                }
            };
            let rx = WS_BROADCAST.1.clone();
            std::thread::spawn(move || loop {
                // Read any incoming control frames (close/ping) non-blockingly.
                match rx.recv_timeout(Duration::from_millis(200)) {
                    Ok(json) => {
                        if ws.send(Message::Text(json)).is_err() {
                            break;
                        }
                    }
                    Err(crossbeam_channel::RecvTimeoutError::Timeout) => {}
                    Err(crossbeam_channel::RecvTimeoutError::Disconnected) => break,
                }
                match ws.read() {
                    Ok(Message::Close(_)) | Err(_) => break,
                    Ok(_) => {}
                }
            });
        }
    });
    // Fallback polling loop; primary path is the FSEvents watcher below.
    std::thread::spawn(move || loop {
        process_new_messages();
        std::thread::sleep(FALLBACK_POLL_INTERVAL);
    });

    // Event-driven receive: FSEvents fires on writes under ~/Library/Messages,
    // so new rows are picked up within milliseconds of the DB write.
    std::thread::spawn(move || {
        use notify::{RecursiveMode, Watcher};
        let (tx, rx) = std::sync::mpsc::channel();
        let mut watcher = match notify::recommended_watcher(tx) {
            Ok(w) => w,
            Err(e) => {
                append_log(&format!("watcher init failed, fallback polling only: {e}"));
                return;
            }
        };
        if let Err(e) = watcher.watch(
            std::path::Path::new("$HOME/Library/Messages"),
            RecursiveMode::Recursive,
        ) {
            append_log(&format!("watcher watch failed, fallback polling only: {e}"));
            return;
        }
        append_log("fs watcher active");
        for ev in rx {
            if ev.is_ok() {
                process_new_messages();
            }
        }
    });

    for mut req in server.incoming_requests() {
        let url = req.url().to_string();
        let method = req.method().to_string();
        let mut body = String::new();
        let _ = req.as_reader().read_to_string(&mut body);
        let (status, payload) = match (method.as_str(), url.split('?').next().unwrap_or("")) {
            ("GET", "/health") => (200, r#"{"ok":true}"#.to_string()),
            ("GET", "/messages") => {
                // Optional ?since=<rowid> for incremental fetch. Default is the
                // last 20 rows before the cursor. The caller tracks the highest
                // rowid it has seen and passes it back as `since`.
                let since = url.split_once('?').and_then(|(_, q)| {
                    q.split('&').find_map(|kv| {
                        let (k, v) = kv.split_once('=')?;
                        (k == "since").then_some(v).and_then(|v| v.parse::<i64>().ok())
                    })
                });
                let start = since.unwrap_or_else(|| read_cursor().saturating_sub(20).max(0));
                match poll_once(start) {
                    Ok(v) => (200, serde_json::to_string(&v).unwrap_or_default()),
                    Err(e) => (500, format!("{{\"error\":\"{}\"}}", e.replace('"', "'"))),
                }
            }
            ("POST", "/send") => {
                match serde_json::from_str::<SendBody>(&body) {
                    Ok(b) => match send_imessage(&b.to, &b.text) {
                        Ok(()) => (200, r#"{"ok":true}"#.to_string()),
                        Err(e) => (500, format!("{{\"error\":\"{}\"}}", e.replace('"', "'"))),
                    },
                    Err(e) => (400, format!("{{\"error\":\"bad json: {}\"}}", e)),
                }
            }
            _ => (404, r#"{"error":"not found"}"#.to_string()),
        };
        let header = Header::from_bytes(b"Content-Type", b"application/json").unwrap();
        let resp = Response::from_string(payload)
            .with_status_code(status)
            .with_header(header);
        let _ = req.respond(resp);
    }
}
