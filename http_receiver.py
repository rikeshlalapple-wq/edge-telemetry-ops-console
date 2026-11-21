import os
import json
import signal
import sys
import threading
from datetime import datetime, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
if BASE_DIR not in sys.path:
    sys.path.insert(0, BASE_DIR)

import db_resilience as dbr
import log_resilience as lr
import payload_contract as pc

DB_PATH = os.path.join(BASE_DIR, "ingested_telemetry.db")
LOG_PATH = os.path.join(BASE_DIR, "logs", "http_receiver.log")
HOST = "127.0.0.1"
PORT = 8090

logger = lr.get_logger("http_receiver", LOG_PATH)
_server: ThreadingHTTPServer | None = None
_shutdown_lock = threading.Lock()
_shutting_down = False


def init_db():
    conn = dbr.connect_db(DB_PATH)
    cur = conn.cursor()
    cur.execute("""
        CREATE TABLE IF NOT EXISTS received_logs (
            log_id INTEGER PRIMARY KEY,
            schema_version INTEGER NOT NULL DEFAULT 1,
            timestamp TEXT,
            unit_id TEXT,
            metric_name TEXT,
            metric_value REAL,
            severity TEXT,
            notes TEXT,
            raw_json TEXT,
            received_at TEXT
        )
    """)
    cols = {row[1] for row in cur.execute("PRAGMA table_info(received_logs)").fetchall()}
    if "schema_version" not in cols:
        cur.execute("ALTER TABLE received_logs ADD COLUMN schema_version INTEGER NOT NULL DEFAULT 1")
    conn.commit()
    dbr.checkpoint_and_close(conn)


def ingest_records(records):
    conn = dbr.connect_db(DB_PATH)
    cur = conn.cursor()
    inserted = 0
    rejected = []
    now_iso = datetime.now(timezone.utc).isoformat()

    for idx, item in enumerate(records):
        ok, reason, norm = pc.validate_record(item)
        if not ok:
            rejected.append({"index": idx, "reason": reason})
            continue

        ts = norm["timestamp"] or now_iso
        cur.execute(
            """
            INSERT OR IGNORE INTO received_logs
            (log_id, schema_version, timestamp, unit_id, metric_name, metric_value, severity, notes, raw_json, received_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                norm["log_id"],
                norm["schema_version"],
                ts,
                norm["unit_id"],
                norm["metric_name"],
                norm["metric_value"],
                norm["severity"],
                norm["notes"],
                json.dumps(item),
                now_iso,
            ),
        )
        if cur.rowcount > 0:
            inserted += 1

    conn.commit()
    dbr.checkpoint_and_close(conn)
    return inserted, rejected


def graceful_shutdown(signum, _frame):
    global _shutting_down
    with _shutdown_lock:
        if _shutting_down:
            return
        _shutting_down = True

    sig_name = signal.Signals(signum).name if signum in signal.Signals._value2member_map_ else str(signum)
    logger.info(f"Received {sig_name} — running WAL checkpoint and shutting down receiver...")

    try:
        chk_conn = dbr.connect_db(DB_PATH)
        stats = dbr.checkpoint_and_close(chk_conn)
        logger.info(f"WAL checkpoint complete (busy, log, checkpointed)={stats}")
    except Exception as exc:
        logger.warning(f"WAL checkpoint warning on shutdown: {exc}")

    if _server is not None:
        threading.Thread(target=_server.shutdown, daemon=True).start()


class TelemetryHandler(BaseHTTPRequestHandler):
    def do_POST(self):
        length = int(self.headers.get("Content-Length", 0))
        body = self.rfile.read(length) if length > 0 else b"[]"
        try:
            payload = json.loads(body.decode("utf-8"))
            records = payload if isinstance(payload, list) else [payload]
            inserted, rejected = ingest_records(records)

            if rejected and inserted == 0 and len(records) == 1:
                status_code = 400
                status_label = "INVALID_PAYLOAD"
            else:
                status_code = 200
                status_label = "SUCCESS"

            resp_obj = {
                "status": status_label,
                "schema_version": pc.SCHEMA_VERSION,
                "message": "Payload processed",
                "inserted": inserted,
                "rejected_count": len(rejected),
                "rejected": rejected,
                "total_received": len(records),
            }
            resp = json.dumps(resp_obj).encode("utf-8")
            logger.info(f"POST {self.path} -> {status_code} inserted={inserted} rejected={len(rejected)}")
            self.send_response(status_code)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(resp)))
            self.end_headers()
            self.wfile.write(resp)
        except Exception as exc:
            logger.error(f"POST {self.path} failed: {exc}")
            err = json.dumps({"status": "ERROR", "message": str(exc)}).encode("utf-8")
            self.send_response(400)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(err)))
            self.end_headers()
            self.wfile.write(err)

    def log_message(self, format, *args):
        logger.info(f"{self.address_string()} - {format % args}")


if __name__ == "__main__":
    signal.signal(signal.SIGINT, graceful_shutdown)
    signal.signal(signal.SIGTERM, graceful_shutdown)
    init_db()
    _server = ThreadingHTTPServer((HOST, PORT), TelemetryHandler)
    logger.info(f"Listening on http://{HOST}:{PORT} -> {DB_PATH}")
    try:
        _server.serve_forever()
    finally:
        if _server is not None:
            _server.server_close()
        logger.info("Receiver stopped cleanly.")
