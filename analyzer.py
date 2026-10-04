"""Offline summary generator for the optimized schema.

The old project re-read every event on every dashboard refresh. This analyzer
uses the aggregate tables and only reads a bounded recent event window.
"""
import json
import sys
from pathlib import Path
from db import connect, init_db
from server import summary_for_stream_ids

OUT = Path('data/summaries')
OUT.mkdir(parents=True, exist_ok=True)


def analyze(stream_id: int | None = None):
    init_db()
    conn = connect()
    try:
        if stream_id:
            ids = [stream_id]
            date = conn.execute('SELECT label_date FROM streams WHERE id=?', (stream_id,)).fetchone()
            name = f'stream_{stream_id}.json'
        else:
            rows = conn.execute('SELECT id FROM streams ORDER BY id').fetchall()
            ids = [r['id'] for r in rows]
            name = 'all.json'
        summary = summary_for_stream_ids(conn, ids, event_limit=5000)
        path = OUT / name
        path.write_text(json.dumps(summary, ensure_ascii=False), encoding='utf-8')
        print(f'Analiz tamamlandı: {path}')
        return path
    finally:
        conn.close()


if __name__ == '__main__':
    sid = int(sys.argv[1]) if len(sys.argv) > 1 else None
    analyze(sid)
