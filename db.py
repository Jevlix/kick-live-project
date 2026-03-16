import sqlite3
import os
from contextlib import contextmanager
from datetime import datetime
from pathlib import Path

DB_FILE = "data/kick_live.db"
DB_PATH = DB_FILE
LOGS_DIR = Path("data/logs")


def ensure_dirs():
    os.makedirs("data", exist_ok=True)
    LOGS_DIR.mkdir(parents=True, exist_ok=True)


@contextmanager
def db_cursor():
    conn = sqlite3.connect(DB_FILE)
    conn.row_factory = sqlite3.Row

    try:
        cur = conn.cursor()
        yield cur
        conn.commit()
    finally:
        conn.close()


def init_db():
    ensure_dirs()

    with db_cursor() as cur:
        # STREAMS
        cur.execute("""
            CREATE TABLE IF NOT EXISTS streams (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                streamer_name TEXT NOT NULL,
                channel_id INTEGER NOT NULL,
                started_at TEXT NOT NULL,
                ended_at TEXT,
                label_date TEXT NOT NULL,
                status TEXT NOT NULL DEFAULT 'live',
                reconnect_group TEXT,
                session_type TEXT NOT NULL DEFAULT 'stream',
                created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
                updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
            )
        """)

        # EVENTS
        cur.execute("""
            CREATE TABLE IF NOT EXISTS events (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                stream_id INTEGER NOT NULL,
                timestamp TEXT NOT NULL,
                event_name TEXT NOT NULL,
                event_type TEXT,
                username TEXT,
                target_username TEXT,
                moderator TEXT,
                message TEXT,
                reason TEXT,
                duration INTEGER,
                permanent INTEGER DEFAULT 0,
                session_type TEXT NOT NULL DEFAULT 'stream',
                raw_json TEXT,
                created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (stream_id) REFERENCES streams(id)
            )
        """)

        # STATE
        cur.execute("""
            CREATE TABLE IF NOT EXISTS state (
                key TEXT PRIMARY KEY,
                value TEXT
            )
        """)

        # MIGRATIONS
        try:
            cur.execute("""
                ALTER TABLE streams
                ADD COLUMN session_type TEXT NOT NULL DEFAULT 'stream'
            """)
        except sqlite3.OperationalError:
            pass

        try:
            cur.execute("""
                ALTER TABLE events
                ADD COLUMN session_type TEXT NOT NULL DEFAULT 'stream'
            """)
        except sqlite3.OperationalError:
            pass

        # INDEXES
        cur.execute("""
            CREATE INDEX IF NOT EXISTS idx_events_stream
            ON events(stream_id)
        """)

        cur.execute("""
            CREATE INDEX IF NOT EXISTS idx_events_user
            ON events(username)
        """)

        cur.execute("""
            CREATE INDEX IF NOT EXISTS idx_events_time
            ON events(timestamp)
        """)

        cur.execute("""
            CREATE INDEX IF NOT EXISTS idx_stream_type
            ON streams(session_type)
        """)

        cur.execute("""
            CREATE INDEX IF NOT EXISTS idx_stream_status
            ON streams(status)
        """)

        cur.execute("""
            CREATE INDEX IF NOT EXISTS idx_stream_label_date
            ON streams(label_date)
        """)


def create_stream(
    streamer_name,
    channel_id,
    started_at,
    label_date,
    reconnect_group=None,
    session_type="stream"
):
    with db_cursor() as cur:
        cur.execute("""
            INSERT INTO streams (
                streamer_name,
                channel_id,
                started_at,
                label_date,
                status,
                reconnect_group,
                session_type
            )
            VALUES (?, ?, ?, ?, 'live', ?, ?)
        """, (
            streamer_name,
            channel_id,
            started_at,
            label_date,
            reconnect_group,
            session_type
        ))
        return cur.lastrowid


def close_stream(stream_id, ended_at):
    with db_cursor() as cur:
        cur.execute("""
            UPDATE streams
            SET
                status='ended',
                ended_at=?,
                updated_at=CURRENT_TIMESTAMP
            WHERE id=?
        """, (ended_at, stream_id))


def add_event(
    stream_id,
    timestamp,
    event_name,
    event_type=None,
    username=None,
    target_username=None,
    moderator=None,
    message=None,
    reason=None,
    duration=None,
    permanent=0,
    session_type="stream",
    raw_json=None
):
    with db_cursor() as cur:
        cur.execute("""
            INSERT INTO events (
                stream_id,
                timestamp,
                event_name,
                event_type,
                username,
                target_username,
                moderator,
                message,
                reason,
                duration,
                permanent,
                session_type,
                raw_json
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            stream_id,
            timestamp,
            event_name,
            event_type,
            username,
            target_username,
            moderator,
            message,
            reason,
            duration,
            permanent,
            session_type,
            raw_json
        ))
        return cur.lastrowid


def get_active_stream():
    with db_cursor() as cur:
        cur.execute("""
            SELECT *
            FROM streams
            WHERE status='live'
              AND session_type='stream'
            ORDER BY id DESC
            LIMIT 1
        """)
        return cur.fetchone()


def get_today_offstream():
    today = datetime.utcnow().strftime("%Y-%m-%d")

    with db_cursor() as cur:
        cur.execute("""
            SELECT *
            FROM streams
            WHERE session_type='offstream'
              AND label_date=?
            ORDER BY id DESC
            LIMIT 1
        """, (today,))
        return cur.fetchone()


def create_offstream_session(streamer_name, channel_id):
    now = datetime.utcnow().isoformat()
    today = datetime.utcnow().strftime("%Y-%m-%d")

    return create_stream(
        streamer_name,
        channel_id,
        now,
        today,
        reconnect_group=None,
        session_type="offstream"
    )


def get_or_create_today_offstream(streamer_name, channel_id):
    existing = get_today_offstream()
    if existing:
        return existing["id"]

    return create_offstream_session(streamer_name, channel_id)


def get_stream(stream_id):
    with db_cursor() as cur:
        cur.execute("""
            SELECT *
            FROM streams
            WHERE id=?
        """, (stream_id,))
        return cur.fetchone()


def list_streams(limit=50):
    with db_cursor() as cur:
        cur.execute("""
            SELECT *
            FROM streams
            ORDER BY id DESC
            LIMIT ?
        """, (limit,))
        return cur.fetchall()


def get_last_events(limit=100):
    with db_cursor() as cur:
        cur.execute("""
            SELECT *
            FROM events
            ORDER BY id DESC
            LIMIT ?
        """, (limit,))
        return cur.fetchall()


def set_state(key, value):
    with db_cursor() as cur:
        cur.execute("""
            INSERT INTO state(key, value)
            VALUES (?, ?)
            ON CONFLICT(key)
            DO UPDATE SET value=excluded.value
        """, (key, str(value)))


def get_state(key, default=None):
    with db_cursor() as cur:
        cur.execute("""
            SELECT value
            FROM state
            WHERE key=?
        """, (key,))
        row = cur.fetchone()

        if not row:
            return default

        return row["value"]


if __name__ == "__main__":
    init_db()
    print("DB hazır.")