import os
os.environ.setdefault('KICK_CHANNEL_SLUG', 'test')

from db import init_db
from recorder import KickRecorder


def test_import_and_db():
    init_db()
    r = KickRecorder()
    assert r.running is True
    assert r.queue.maxsize == 10000


def test_paths_are_absolute():
    from config import DB_PATH, BASE_DIR
    assert DB_PATH.is_absolute()
    assert BASE_DIR.is_absolute()
