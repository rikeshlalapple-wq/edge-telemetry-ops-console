import sqlite3


def connect_db(db_path: str, timeout_sec: float = 10.0) -> sqlite3.Connection:
    conn = sqlite3.connect(db_path, timeout=timeout_sec)
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()
    cur.execute("PRAGMA journal_mode=WAL;")
    cur.execute("PRAGMA busy_timeout=5000;")
    cur.execute("PRAGMA synchronous=NORMAL;")
    conn.commit()
    return conn


def checkpoint_and_close(conn: sqlite3.Connection | None) -> tuple[int, int, int] | None:
    if conn is None:
        return None
    try:
        row = conn.execute("PRAGMA wal_checkpoint(TRUNCATE);").fetchone()
        conn.commit()
        return (int(row[0]), int(row[1]), int(row[2])) if row else None
    finally:
        conn.close()
