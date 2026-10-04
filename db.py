import json
import sqlite3
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterable

from config import DB_PATH, LOGS_DIR, DATA_DIR, SUMMARIES_DIR

DB_FILE = str(DB_PATH)


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def connect() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_FILE, timeout=30, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    conn.execute('PRAGMA journal_mode=WAL')
    conn.execute('PRAGMA synchronous=NORMAL')
    conn.execute('PRAGMA busy_timeout=30000')
    conn.execute('PRAGMA foreign_keys=ON')
    conn.execute('PRAGMA temp_store=MEMORY')
    return conn


def init_db():
    for p in (DATA_DIR, LOGS_DIR, SUMMARIES_DIR):
        p.mkdir(parents=True, exist_ok=True)
    conn = connect()
    try:
        conn.executescript('''
        CREATE TABLE IF NOT EXISTS streams (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            streamer_name TEXT NOT NULL,
            channel_id INTEGER NOT NULL,
            chatroom_id INTEGER,
            started_at TEXT NOT NULL,
            ended_at TEXT,
            label_date TEXT NOT NULL,
            status TEXT NOT NULL DEFAULT 'live',
            reconnect_group TEXT,
            session_type TEXT NOT NULL DEFAULT 'stream',
            title TEXT,
            category_id INTEGER,
            category_name TEXT,
            language TEXT,
            thumbnail TEXT,
            peak_viewers INTEGER DEFAULT 0,
            last_viewer_count INTEGER DEFAULT 0,
            created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
            updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
        );
        CREATE TABLE IF NOT EXISTS users (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            kick_user_id INTEGER,
            username TEXT NOT NULL,
            display_name TEXT,
            profile_picture TEXT,
            first_seen_at TEXT,
            last_seen_at TEXT,
            total_messages INTEGER NOT NULL DEFAULT 0,
            UNIQUE(kick_user_id),
            UNIQUE(username)
        );
        CREATE TABLE IF NOT EXISTS chat_messages (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            stream_id INTEGER NOT NULL,
            message_id TEXT,
            user_id INTEGER,
            username TEXT,
            timestamp TEXT NOT NULL,
            message TEXT,
            reply_to_message_id TEXT,
            emote_count INTEGER DEFAULT 0,
            raw_json TEXT,
            created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(message_id),
            FOREIGN KEY(stream_id) REFERENCES streams(id),
            FOREIGN KEY(user_id) REFERENCES users(id)
        );
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
            message_id TEXT,
            quantity INTEGER DEFAULT 1,
            raw_json TEXT,
            created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY(stream_id) REFERENCES streams(id)
        );
        CREATE TABLE IF NOT EXISTS stream_stats (
            stream_id INTEGER PRIMARY KEY,
            total_messages INTEGER DEFAULT 0,
            unique_chatters INTEGER DEFAULT 0,
            deleted_messages INTEGER DEFAULT 0,
            timeouts INTEGER DEFAULT 0,
            bans INTEGER DEFAULT 0,
            unbans INTEGER DEFAULT 0,
            subscriptions INTEGER DEFAULT 0,
            gift_subscriptions INTEGER DEFAULT 0,
            other_events INTEGER DEFAULT 0,
            peak_chat_per_minute INTEGER DEFAULT 0,
            peak_chat_per_second INTEGER DEFAULT 0,
            last_event_at TEXT,
            updated_at TEXT DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY(stream_id) REFERENCES streams(id)
        );
        CREATE TABLE IF NOT EXISTS chat_minute_stats (
            stream_id INTEGER NOT NULL,
            minute TEXT NOT NULL,
            messages INTEGER DEFAULT 0,
            unique_chatters INTEGER DEFAULT 0,
            PRIMARY KEY(stream_id, minute),
            FOREIGN KEY(stream_id) REFERENCES streams(id)
        );
        CREATE TABLE IF NOT EXISTS user_stream_stats (
            stream_id INTEGER NOT NULL,
            user_id INTEGER NOT NULL,
            username TEXT NOT NULL,
            messages INTEGER DEFAULT 0,
            first_message_at TEXT,
            last_message_at TEXT,
            PRIMARY KEY(stream_id, user_id),
            FOREIGN KEY(stream_id) REFERENCES streams(id),
            FOREIGN KEY(user_id) REFERENCES users(id)
        );
        CREATE TABLE IF NOT EXISTS word_stats (
            stream_id INTEGER NOT NULL,
            word TEXT NOT NULL,
            count INTEGER DEFAULT 0,
            PRIMARY KEY(stream_id, word),
            FOREIGN KEY(stream_id) REFERENCES streams(id)
        );
        CREATE TABLE IF NOT EXISTS emote_stats (
            stream_id INTEGER NOT NULL,
            emote_id TEXT NOT NULL,
            emote_name TEXT NOT NULL,
            count INTEGER DEFAULT 0,
            unique_users INTEGER DEFAULT 0,
            PRIMARY KEY(stream_id, emote_id, emote_name),
            FOREIGN KEY(stream_id) REFERENCES streams(id)
        );
        CREATE TABLE IF NOT EXISTS user_word_stats (
            stream_id INTEGER NOT NULL,
            user_id INTEGER NOT NULL,
            word TEXT NOT NULL,
            count INTEGER DEFAULT 0,
            PRIMARY KEY(stream_id, user_id, word),
            FOREIGN KEY(stream_id) REFERENCES streams(id),
            FOREIGN KEY(user_id) REFERENCES users(id)
        );
        CREATE TABLE IF NOT EXISTS user_emote_stats (
            stream_id INTEGER NOT NULL,
            user_id INTEGER NOT NULL,
            emote_id TEXT NOT NULL,
            emote_name TEXT NOT NULL,
            count INTEGER DEFAULT 0,
            PRIMARY KEY(stream_id, user_id, emote_id, emote_name),
            FOREIGN KEY(stream_id) REFERENCES streams(id),
            FOREIGN KEY(user_id) REFERENCES users(id)
        );
        CREATE TABLE IF NOT EXISTS spam_stats (
            stream_id INTEGER NOT NULL,
            message_key TEXT NOT NULL,
            message TEXT NOT NULL,
            count INTEGER DEFAULT 0,
            unique_users INTEGER DEFAULT 0,
            last_username TEXT,
            last_timestamp TEXT,
            usernames_json TEXT,
            PRIMARY KEY(stream_id, message_key),
            FOREIGN KEY(stream_id) REFERENCES streams(id)
        );
        CREATE TABLE IF NOT EXISTS stream_snapshots (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            stream_id INTEGER NOT NULL,
            captured_at TEXT NOT NULL,
            viewer_count INTEGER,
            title TEXT,
            category_id INTEGER,
            category_name TEXT,
            language TEXT,
            FOREIGN KEY(stream_id) REFERENCES streams(id)
        );
        CREATE TABLE IF NOT EXISTS chat_minute_users (
            stream_id INTEGER NOT NULL,
            minute TEXT NOT NULL,
            user_id INTEGER NOT NULL,
            PRIMARY KEY(stream_id, minute, user_id),
            FOREIGN KEY(stream_id) REFERENCES streams(id),
            FOREIGN KEY(user_id) REFERENCES users(id)
        );
        CREATE TABLE IF NOT EXISTS state (
            key TEXT PRIMARY KEY,
            value TEXT
        );
        CREATE INDEX IF NOT EXISTS idx_events_stream_time ON events(stream_id, timestamp DESC);
        CREATE INDEX IF NOT EXISTS idx_events_user ON events(username);
        CREATE INDEX IF NOT EXISTS idx_chat_stream_time ON chat_messages(stream_id, timestamp DESC);
        CREATE INDEX IF NOT EXISTS idx_chat_user ON chat_messages(user_id);
        CREATE INDEX IF NOT EXISTS idx_snapshots_stream_time ON stream_snapshots(stream_id, captured_at DESC);
        CREATE INDEX IF NOT EXISTS idx_user_words ON user_word_stats(stream_id, user_id, count DESC);
        CREATE INDEX IF NOT EXISTS idx_user_emotes ON user_emote_stats(stream_id, user_id, count DESC);
        CREATE INDEX IF NOT EXISTS idx_streams_status_type ON streams(status, session_type, id DESC);
        CREATE INDEX IF NOT EXISTS idx_events_stream_type_mod ON events(stream_id, event_type, moderator, timestamp DESC);
        CREATE INDEX IF NOT EXISTS idx_events_target ON events(stream_id, target_username, event_type, timestamp DESC);
        CREATE INDEX IF NOT EXISTS idx_emote_stream_count ON emote_stats(stream_id, count DESC);
        CREATE INDEX IF NOT EXISTS idx_word_stream_count ON word_stats(stream_id, count DESC);
        CREATE INDEX IF NOT EXISTS idx_spam_stream_count ON spam_stats(stream_id, count DESC);
        ''')
        # Lightweight schema migration for databases created by v2.
        event_cols = {r['name'] for r in conn.execute('PRAGMA table_info(events)').fetchall()}
        if 'quantity' not in event_cols:
            conn.execute('ALTER TABLE events ADD COLUMN quantity INTEGER DEFAULT 1')
        spam_cols = {r['name'] for r in conn.execute('PRAGMA table_info(spam_stats)').fetchall()}
        for name, typ in {'unique_users':'INTEGER DEFAULT 0','last_timestamp':'TEXT','usernames_json':'TEXT'}.items():
            if name not in spam_cols:
                conn.execute(f'ALTER TABLE spam_stats ADD COLUMN {name} {typ}')
        conn.commit()
    finally:
        conn.close()


def _ensure_stream_columns(conn):
    cols = {r['name'] for r in conn.execute('PRAGMA table_info(streams)').fetchall()}
    additions = {
        'chatroom_id': 'INTEGER', 'title': 'TEXT', 'category_id': 'INTEGER',
        'category_name': 'TEXT', 'language': 'TEXT', 'thumbnail': 'TEXT',
        'peak_viewers': 'INTEGER DEFAULT 0', 'last_viewer_count': 'INTEGER DEFAULT 0'
    }
    for name, typ in additions.items():
        if name not in cols:
            conn.execute(f'ALTER TABLE streams ADD COLUMN {name} {typ}')


def _scalar(value):
    if isinstance(value, dict):
        for key in ("url", "src", "thumbnail", "fullsize", "medium"):
            if value.get(key):
                return str(value[key])
        return json.dumps(value, ensure_ascii=False, separators=(",", ":"))
    if isinstance(value, (list, tuple)):
        return json.dumps(value, ensure_ascii=False, separators=(",", ":"))
    return value


def create_stream(meta: dict) -> int:
    conn = connect()
    try:
        _ensure_stream_columns(conn)
        cur = conn.execute('''INSERT INTO streams
            (streamer_name, channel_id, chatroom_id, started_at, label_date, status,
             session_type, title, category_id, category_name, language, thumbnail,
             last_viewer_count)
            VALUES (?, ?, ?, ?, ?, 'live', 'stream', ?, ?, ?, ?, ?, ?)''', (
            meta['slug'], meta['broadcaster_user_id'], meta.get('chatroom_id'),
            meta.get('started_at') or utc_now_iso(),
            (meta.get('started_at') or utc_now_iso())[:10],
            meta.get('title'), meta.get('category_id'), meta.get('category_name'),
            meta.get('language'), _scalar(meta.get('thumbnail')), meta.get('viewer_count') or 0
        ))
        stream_id = cur.lastrowid
        conn.execute('INSERT INTO stream_stats(stream_id) VALUES (?)', (stream_id,))
        conn.commit()
        return stream_id
    finally:
        conn.close()


def get_active_stream():
    conn = connect()
    try:
        return conn.execute("SELECT * FROM streams WHERE status='live' AND session_type='stream' ORDER BY id DESC LIMIT 1").fetchone()
    finally:
        conn.close()


def close_stream(stream_id: int, ended_at: str):
    conn = connect()
    try:
        conn.execute("UPDATE streams SET status='ended', ended_at=?, updated_at=CURRENT_TIMESTAMP WHERE id=?", (ended_at, stream_id))
        conn.commit()
    finally:
        conn.close()


def update_stream_metadata(stream_id: int, meta: dict):
    conn = connect()
    try:
        conn.execute('''UPDATE streams SET title=?, category_id=?, category_name=?, language=?, thumbnail=?,
                        last_viewer_count=?, peak_viewers=MAX(COALESCE(peak_viewers,0),?), updated_at=CURRENT_TIMESTAMP WHERE id=?''', (
            meta.get('title'), meta.get('category_id'), meta.get('category_name'), meta.get('language'),
            _scalar(meta.get('thumbnail')), meta.get('viewer_count') or 0, meta.get('viewer_count') or 0, stream_id))
        conn.execute('''INSERT INTO stream_snapshots(stream_id,captured_at,viewer_count,title,category_id,category_name,language)
                        VALUES(?,?,?,?,?,?,?)''', (stream_id, utc_now_iso(), meta.get('viewer_count'), meta.get('title'), meta.get('category_id'), meta.get('category_name'), meta.get('language')))
        conn.commit()
    finally:
        conn.close()


def get_state(key, default=None):
    conn = connect()
    try:
        row = conn.execute('SELECT value FROM state WHERE key=?', (key,)).fetchone()
        return row['value'] if row else default
    finally:
        conn.close()


def set_state(key, value):
    conn = connect()
    try:
        conn.execute('INSERT INTO state(key,value) VALUES(?,?) ON CONFLICT(key) DO UPDATE SET value=excluded.value', (key, str(value)))
        conn.commit()
    finally:
        conn.close()


def get_or_create_today_offstream(streamer_name, channel_id):
    today = datetime.now(timezone.utc).strftime('%Y-%m-%d')
    conn = connect()
    try:
        row = conn.execute("SELECT * FROM streams WHERE session_type='offstream' AND label_date=? ORDER BY id DESC LIMIT 1", (today,)).fetchone()
        if row: return row['id']
        cur = conn.execute('''INSERT INTO streams(streamer_name,channel_id,started_at,label_date,status,session_type)
                              VALUES(?,?,?,'{}','live','offstream')'''.format(today), (streamer_name, channel_id, utc_now_iso()))
        conn.commit()
        return cur.lastrowid
    finally:
        conn.close()


class BatchWriter:
    def __init__(self, conn: sqlite3.Connection):
        self.conn = conn

    def write_batch(self, batch: Iterable[dict]):
        batch = list(batch)
        if not batch: return
        for e in batch:
            self._write_one(e)
        self.conn.commit()

    def _get_user(self, e):
        uid = e.get('user_id')
        username = e.get('user') or 'Unknown'
        display = e.get('display_name') or username
        if uid is not None:
            row = self.conn.execute('SELECT id FROM users WHERE kick_user_id=?', (uid,)).fetchone()
            if row:
                user_id = row['id']
                self.conn.execute('UPDATE users SET username=?,display_name=?,profile_picture=?,last_seen_at=?,total_messages=total_messages+? WHERE id=?',
                                  (username, display, e.get('profile_picture'), e['t'], 1 if e.get('type') == 'chat' else 0, user_id))
                return user_id
        row = self.conn.execute('SELECT id FROM users WHERE username=?', (username,)).fetchone()
        if row:
            user_id = row['id']
            self.conn.execute('UPDATE users SET last_seen_at=?,total_messages=total_messages+? WHERE id=?', (e['t'], 1 if e.get('type') == 'chat' else 0, user_id))
            return user_id
        cur = self.conn.execute('''INSERT INTO users(kick_user_id,username,display_name,profile_picture,first_seen_at,last_seen_at,total_messages)
                                   VALUES(?,?,?,?,?,?,?)''', (uid, username, display, e.get('profile_picture'), e['t'], e['t'], 1 if e.get('type') == 'chat' else 0))
        return cur.lastrowid

    def _write_one(self, e):
        sid = e['stream_id']; ts = e['t']; et = e.get('type') or 'other'
        raw = json.dumps(e.get('raw', {}), ensure_ascii=False, separators=(',', ':')) if e.get('raw') is not None else None
        user_id = self._get_user(e) if e.get('user') else None
        message_id = e.get('message_id') or e.get('msg_id')
        if et == 'chat':
            cur = self.conn.execute('''INSERT OR IGNORE INTO chat_messages(stream_id,message_id,user_id,username,timestamp,message,reply_to_message_id,emote_count,raw_json)
                                       VALUES(?,?,?,?,?,?,?,?,?)''', (sid,message_id,user_id,e.get('user'),ts,e.get('msg'),e.get('reply_to_message_id'),len(e.get('emotes') or []),raw if e.get('keep_raw') else None))
            if cur.rowcount:
                self.conn.execute('''INSERT INTO stream_stats(stream_id,total_messages,last_event_at,updated_at) VALUES(?,?,?,CURRENT_TIMESTAMP)
                    ON CONFLICT(stream_id) DO UPDATE SET total_messages=total_messages+1,last_event_at=excluded.last_event_at,updated_at=CURRENT_TIMESTAMP''', (sid,1,ts))
                minute = ts[:16]
                self.conn.execute('''INSERT INTO chat_minute_stats(stream_id,minute,messages,unique_chatters) VALUES(?,?,1,0)
                    ON CONFLICT(stream_id,minute) DO UPDATE SET messages=messages+1''', (sid,minute))
                minute_user = self.conn.execute('INSERT OR IGNORE INTO chat_minute_users(stream_id,minute,user_id) VALUES(?,?,?)', (sid,minute,user_id))
                if minute_user.rowcount:
                    self.conn.execute('UPDATE chat_minute_stats SET unique_chatters=unique_chatters+1 WHERE stream_id=? AND minute=?', (sid,minute))
                if user_id:
                    self.conn.execute('''INSERT INTO user_stream_stats(stream_id,user_id,username,messages,first_message_at,last_message_at)
                        VALUES(?,?,?,?,?,?) ON CONFLICT(stream_id,user_id) DO UPDATE SET messages=messages+1,last_message_at=excluded.last_message_at,username=excluded.username''',
                        (sid,user_id,e.get('user') or 'Unknown',1,ts,ts))
                for word in e.get('words') or []:
                    self.conn.execute('''INSERT INTO word_stats(stream_id,word,count) VALUES(?,?,1) ON CONFLICT(stream_id,word) DO UPDATE SET count=count+1''', (sid,word))
                    if user_id:
                        self.conn.execute('''INSERT INTO user_word_stats(stream_id,user_id,word,count) VALUES(?,?,?,1)
                            ON CONFLICT(stream_id,user_id,word) DO UPDATE SET count=count+1''', (sid,user_id,word))
                seen_emotes=set()
                for em in e.get('emotes') or []:
                    if isinstance(em, dict):
                        eid=str(em.get('id') or em.get('emote_id') or '')
                        ename=str(em.get('name') or em.get('emote_name') or 'unknown')
                    else:
                        eid=str(em); ename='unknown'
                    if not eid: continue
                    self.conn.execute('''INSERT INTO emote_stats(stream_id,emote_id,emote_name,count) VALUES(?,?,?,1)
                        ON CONFLICT(stream_id,emote_id,emote_name) DO UPDATE SET count=count+1''', (sid,eid,ename))
                    if user_id:
                        self.conn.execute('''INSERT INTO user_emote_stats(stream_id,user_id,emote_id,emote_name,count) VALUES(?,?,?,?,1)
                            ON CONFLICT(stream_id,user_id,emote_id,emote_name) DO UPDATE SET count=count+1''', (sid,user_id,eid,ename))
                        seen_emotes.add((eid,ename))
                if e.get('spam_key') and int(e.get('spam_repeat_count') or 0) >= 3:
                    key=e['spam_key']; uname=e.get('user') or 'Unknown'
                    row=self.conn.execute('SELECT usernames_json,count FROM spam_stats WHERE stream_id=? AND message_key=?',(sid,key)).fetchone()
                    names=set(json.loads(row['usernames_json']) if row and row['usernames_json'] else [])
                    names.add(uname)
                    # Store every occurrence from the third consecutive repeat onward.
                    increment = int(e.get('spam_repeat_count') or 3) if not row else 1
                    self.conn.execute('''INSERT INTO spam_stats(stream_id,message_key,message,count,unique_users,last_username,last_timestamp,usernames_json) VALUES(?,?,?,?,?,?,?,?)
                        ON CONFLICT(stream_id,message_key) DO UPDATE SET count=count+1,unique_users=excluded.unique_users,last_username=excluded.last_username,last_timestamp=excluded.last_timestamp,usernames_json=excluded.usernames_json''',
                        (sid,key,e.get('msg') or '',increment,len(names),uname,ts,json.dumps(sorted(names),ensure_ascii=False)))
        else:
            quantity = max(1, int(e.get('quantity') or e.get('gift_count') or 1))
            self.conn.execute('''INSERT INTO events(stream_id,timestamp,event_name,event_type,username,target_username,moderator,message,reason,duration,permanent,session_type,message_id,quantity,raw_json)
                VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)''', (sid,ts,e.get('e') or 'UnknownEvent',et,e.get('user'),e.get('target_user'),e.get('mod'),e.get('msg'),e.get('reason'),e.get('duration'),1 if e.get('permanent') else 0,e.get('session_type','stream'),message_id,quantity,raw if e.get('keep_raw') else None))
            col = {'deleted':'deleted_messages','timeout':'timeouts','ban':'bans','unban':'unbans','subscription':'subscriptions','gift_sub':'gift_subscriptions'}.get(et,'other_events')
            increment = quantity if et == 'gift_sub' else 1
            self.conn.execute(f'''INSERT INTO stream_stats(stream_id,{col},last_event_at,updated_at) VALUES(?,?,?,CURRENT_TIMESTAMP)
                ON CONFLICT(stream_id) DO UPDATE SET {col}={col}+?,last_event_at=excluded.last_event_at,updated_at=CURRENT_TIMESTAMP''', (sid,increment,ts,increment))
