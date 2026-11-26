#!/usr/bin/env python3
"""Exercise MCP transports against a temporary SQLite WAL database.

Run: python3 tests/mcp_smoke.py /path/to/imsg-rs
Only synthetic messages are read. The send tool is tested with invalid input;
this script never sends an iMessage or changes the running LaunchAgent.
"""

import argparse
import http.client
import json
import os
import pathlib
import select
import socket
import sqlite3
import subprocess
import tempfile
import threading
import time


PROTOCOL = "2025-11-25"
TOOLS = {"imsg_health", "imsg_messages", "imsg_wait_messages", "imsg_send"}


def free_port():
    with socket.socket() as listener:
        listener.bind(("127.0.0.1", 0))
        return listener.getsockname()[1]


def fixture(path):
    writer = sqlite3.connect(path)
    assert writer.execute("PRAGMA journal_mode=WAL").fetchone()[0] == "wal"
    writer.executescript(
        """
        CREATE TABLE message (
          ROWID INTEGER PRIMARY KEY, date INTEGER NOT NULL,
          is_from_me INTEGER NOT NULL, text TEXT, attributedBody BLOB,
          handle_id INTEGER, cache_has_attachments INTEGER);
        CREATE TABLE chat (
          ROWID INTEGER PRIMARY KEY, guid TEXT, chat_identifier TEXT);
        CREATE TABLE chat_message_join (chat_id INTEGER, message_id INTEGER);
        CREATE TABLE handle (ROWID INTEGER PRIMARY KEY, id TEXT);
        INSERT INTO chat VALUES
          (1, 'iMessage;-;fixture', 'fixture@example.invalid');
        INSERT INTO handle VALUES (1, 'fixture@example.invalid');
        """
    )
    for rowid, from_me, text in [
        (1, 0, "incoming one"),
        (2, 1, "outgoing two"),
        (3, 0, None),
        (4, 0, "incoming four"),
        (5, 1, "outgoing five"),
        (6, 0, ""),
    ]:
        insert(writer, rowid, text, from_me)
    writer.commit()
    return writer


def insert(writer, rowid, text, from_me=0):
    writer.execute(
        "INSERT INTO message VALUES (?, ?, ?, ?, NULL, 1, 0)",
        (rowid, 800000000000000000 + rowid, from_me, text),
    )
    writer.execute("INSERT INTO chat_message_join VALUES (1, ?)", (rowid,))


def request(port, body=None, method="POST", headers=None):
    connection = http.client.HTTPConnection("127.0.0.1", port, timeout=8)
    common = {
        "Content-Type": "application/json",
        "Accept": "application/json, text/event-stream",
        "MCP-Protocol-Version": PROTOCOL,
    }
    common.update(headers or {})
    if isinstance(body, (dict, list)):
        body = json.dumps(body).encode()
    try:
        connection.request(method, "/mcp", body=body, headers=common)
        response = connection.getresponse()
        payload = response.read()
        return response.status, payload
    finally:
        connection.close()


def rpc_request(method, params=None, request_id=1):
    value = {"jsonrpc": "2.0", "id": request_id, "method": method}
    if params is not None:
        value["params"] = params
    return value


def initialize(rpc):
    response = rpc(
        "initialize",
        {
            "protocolVersion": PROTOCOL,
            "capabilities": {},
            "clientInfo": {"name": "imsg-rs-smoke", "version": "1"},
        },
    )
    result = response["result"]
    assert result["protocolVersion"] == PROTOCOL, result
    assert result["serverInfo"]["name"] == "imsg-rs", result
    assert "tools" in result["capabilities"], result


def tool_result(response):
    assert "error" not in response, response
    result = response["result"]
    assert not result.get("isError", False), result
    structured = result["structuredContent"]
    assert isinstance(structured, dict), result
    text = next(item["text"] for item in result["content"] if item["type"] == "text")
    assert json.loads(text) == structured, result
    return structured


def check_tools(rpc):
    response = rpc("tools/list", {})
    definitions = response["result"]["tools"]
    assert {tool["name"] for tool in definitions} == TOOLS, definitions
    by_name = {tool["name"]: tool for tool in definitions}
    assert by_name["imsg_send"]["annotations"]["readOnlyHint"] is False
    for name in TOOLS - {"imsg_send"}:
        assert by_name[name]["annotations"]["readOnlyHint"] is True
    assert rpc("ping", {})["result"] == {}


def call(rpc, name, arguments):
    return rpc("tools/call", {"name": name, "arguments": arguments})


def check_reading(rpc):
    first = tool_result(call(rpc, "imsg_messages", {"since": 0, "limit": 1}))
    assert [message["rowid"] for message in first["messages"]] == [1], first
    assert first["next_cursor"] == 1 and first["has_more"] is True, first
    second = tool_result(
        call(rpc, "imsg_messages", {"since": first["next_cursor"], "limit": 1})
    )
    assert [message["rowid"] for message in second["messages"]] == [4], second
    assert second["next_cursor"] == 4 and second["has_more"] is True, second
    final = tool_result(call(rpc, "imsg_messages", {"since": 4, "limit": 10}))
    assert final["messages"] == [] and final["next_cursor"] == 6, final
    assert final["has_more"] is False, final
    recent = tool_result(call(rpc, "imsg_messages", {"incoming_only": False}))
    assert [message["rowid"] for message in recent["messages"]] == [1, 2, 4, 5], recent
    assert recent["next_cursor"] == 6, recent
    assert recent["messages"][0]["sender"] == "fixture@example.invalid", recent


def check_errors(rpc):
    assert rpc("unknown-method")["error"]["code"] == -32601
    assert call(rpc, "unknown-tool", {})["error"]["code"] == -32602
    assert call(rpc, "imsg_messages", {"since": -1})["error"]["code"] == -32602
    assert call(rpc, "imsg_messages", {"limit": 0})["error"]["code"] == -32602
    assert call(rpc, "imsg_wait_messages", {"since": 0, "timeout_ms": 30001})["error"]["code"] == -32602
    assert call(rpc, "imsg_send", {"to": "", "text": "must never send"})["error"]["code"] == -32602


def terminate(process):
    if process.poll() is not None:
        return
    process.terminate()
    try:
        process.wait(timeout=5)
    except subprocess.TimeoutExpired:
        process.kill()
        process.wait(timeout=5)


def http_checks(binary, root, writer, environment):
    port, ws_port = free_port(), free_port()
    while ws_port == port:
        ws_port = free_port()
    environment = dict(environment, IMSG_HTTP_ADDR=f"127.0.0.1:{port}", IMSG_WS_ADDR=f"127.0.0.1:{ws_port}")
    with (root / "http-process.log").open("wb") as output:
        process = subprocess.Popen([binary], env=environment, stdout=output, stderr=output)
        try:
            deadline = time.monotonic() + 10
            while True:
                if process.poll() is not None:
                    raise AssertionError((root / "http-process.log").read_text())
                try:
                    status, _ = request(port, method="GET")
                    assert status == 405, status
                    break
                except OSError:
                    if time.monotonic() >= deadline:
                        raise
                    time.sleep(0.05)

            next_id = 0

            def rpc(method, params=None):
                nonlocal next_id
                next_id += 1
                status, raw = request(port, rpc_request(method, params, next_id))
                assert status == 200, (status, raw)
                response = json.loads(raw)
                assert response["jsonrpc"] == "2.0" and response["id"] == next_id, response
                return response

            initialize(rpc)
            status, payload = request(port, {"jsonrpc": "2.0", "method": "notifications/initialized"})
            assert status == 202 and payload == b"", (status, payload)
            check_tools(rpc)
            check_reading(rpc)
            check_errors(rpc)
            health = tool_result(call(rpc, "imsg_health", {}))
            assert health["ok"] is True and health["latest_rowid"] == 6, health
            assert request(port, rpc_request("ping"), headers={"Origin": "https://evil.example"})[0] == 403
            assert request(port, rpc_request("ping"), headers={"Host": "evil.example"})[0] == 403
            assert request(port, rpc_request("ping"), headers={"Content-Type": "text/plain"})[0] == 415
            assert request(port, rpc_request("ping"), headers={"MCP-Protocol-Version": "unsupported"})[0] == 400
            assert request(port, rpc_request("ping"), headers={"Accept": "text/event-stream"})[0] == 406
            status, malformed = request(port, b"{broken")
            assert status == 400 and json.loads(malformed)["error"]["code"] == -32700

            result = {}

            def wait():
                try:
                    result["response"] = call(rpc, "imsg_wait_messages", {"since": 6, "timeout_ms": 4000})
                    result["received_at"] = time.monotonic()
                except BaseException as error:
                    result["error"] = error

            worker = threading.Thread(target=wait)
            worker.start()
            try:
                time.sleep(0.15)
                insert(writer, 7, "new incoming message")
                committed_at = time.monotonic()
                writer.commit()
            finally:
                worker.join(timeout=7)
            assert not worker.is_alive(), "HTTP wait tool did not finish"
            if "error" in result:
                raise result["error"]
            page = tool_result(result["response"])
            assert [message["rowid"] for message in page["messages"]] == [7], page
            assert page["next_cursor"] == 7 and page["timed_out"] is False, page
            latency = (result["received_at"] - committed_at) * 1000

            insert(writer, 8, "outgoing ignored", 1)
            insert(writer, 9, None)
            writer.commit()
            timeout = tool_result(call(rpc, "imsg_wait_messages", {"since": 7, "timeout_ms": 0}))
            assert timeout["messages"] == [] and timeout["next_cursor"] == 9, timeout
            assert timeout["timed_out"] is True, timeout
            return {"http": "passed", "http_wait_delivery_ms": round(latency, 3)}
        finally:
            terminate(process)


def stdio_checks(binary, root, writer, environment):
    port, ws_port = free_port(), free_port()
    cursor_path = root / "stdio-cursor.txt"
    cursor_path.write_text("100\n")
    environment = dict(environment, IMSG_CURSOR_PATH=str(cursor_path), IMSG_LOG_PATH=str(root / "stdio.log"), IMSG_HTTP_ADDR=f"127.0.0.1:{port}", IMSG_WS_ADDR=f"127.0.0.1:{ws_port}")
    with (root / "stdio-stderr.log").open("wb") as errors:
        process = subprocess.Popen([binary, "mcp"], env=environment, stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=errors)
        try:
            next_id = 0

            def send(value):
                process.stdin.write(json.dumps(value).encode() + b"\n")
                process.stdin.flush()

            def receive(request_id):
                ready, _, _ = select.select([process.stdout], [], [], 8)
                assert ready, "stdio response timed out"
                line = process.stdout.readline()
                assert line, ("stdio closed", (root / "stdio-stderr.log").read_text())
                value = json.loads(line)
                assert value["jsonrpc"] == "2.0" and value["id"] == request_id, value
                return value

            def rpc(method, params=None):
                nonlocal next_id
                next_id += 1
                send(rpc_request(method, params, next_id))
                return receive(next_id)

            initialize(rpc)
            send({"jsonrpc": "2.0", "method": "notifications/initialized"})
            check_tools(rpc)
            check_errors(rpc)
            health = tool_result(call(rpc, "imsg_health", {}))
            assert health["ok"] is True and health["latest_rowid"] == 9, health
            page = tool_result(call(rpc, "imsg_messages", {"since": 6}))
            assert [message["rowid"] for message in page["messages"]] == [7], page
            assert page["next_cursor"] == 9, page

            next_id += 1
            send(rpc_request("tools/call", {"name": "imsg_wait_messages", "arguments": {"since": 9, "timeout_ms": 4000}}, next_id))
            time.sleep(0.15)
            insert(writer, 10, "stdio incoming message")
            committed_at = time.monotonic()
            writer.commit()
            page = tool_result(receive(next_id))
            latency = (time.monotonic() - committed_at) * 1000
            assert [message["rowid"] for message in page["messages"]] == [10], page
            assert page["next_cursor"] == 10 and page["timed_out"] is False, page
            timeout = tool_result(call(rpc, "imsg_wait_messages", {"since": 10, "timeout_ms": 0}))
            assert timeout["messages"] == [] and timeout["next_cursor"] == 10, timeout
            assert timeout["timed_out"] is True, timeout
            for endpoint in [port, ws_port]:
                with socket.socket() as connection:
                    connection.settimeout(0.5)
                    assert connection.connect_ex(("127.0.0.1", endpoint)) != 0, "stdio unexpectedly opened a listener"
            process.stdin.close()
            assert process.wait(timeout=5) == 0
            assert process.stdout.read() == b"", "non-protocol data on stdout"
            assert cursor_path.read_text() == "100\n", "stdio changed the daemon cursor"
            assert not (root / "stdio.log").exists(), "stdio unexpectedly started the daemon logger"
            return {"stdio": "passed", "stdio_wait_delivery_ms": round(latency, 3)}
        finally:
            terminate(process)
            process.stdout.close()
            if not process.stdin.closed:
                process.stdin.close()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("binary", nargs="?", default=str(pathlib.Path(__file__).resolve().parents[1] / "target/release/imsg-rs"))
    options = parser.parse_args()
    binary = str(pathlib.Path(options.binary).resolve())
    # /private/tmp is local APFS on macOS, unlike a task's TMPDIR which can be SMB.
    with tempfile.TemporaryDirectory(prefix="imsg-mcp-smoke-", dir="/private/tmp") as directory:
        root = pathlib.Path(directory)
        database = root / "chat.db"
        writer = fixture(database)
        environment = dict(os.environ, IMSG_DB_PATH=str(database), IMSG_CURSOR_PATH=str(root / "http-cursor.txt"), IMSG_LOG_PATH=str(root / "http.log"))
        try:
            result = http_checks(binary, root, writer, environment)
            result.update(stdio_checks(binary, root, writer, environment))
            result["send_test"] = "invalid input rejected; no real messages sent"
            print(json.dumps(result, ensure_ascii=False))
        finally:
            writer.close()


if __name__ == "__main__":
    main()
