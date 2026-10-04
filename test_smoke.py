import os
os.environ.setdefault('KICK_CHANNEL_SLUG', 'test')

from db import init_db
from recorder import KickRecorder


def test_import_and_db():
    init_db()
    r = KickRecorder()
    assert r.running is True
    assert r.queue.maxsize == 10000
