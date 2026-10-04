import asyncio
from datetime import datetime, timezone

from config import CHANNEL_SLUG, OFFLINE_GRACE_SECONDS
from db import close_stream, create_stream, get_active_stream, set_state, update_stream_metadata
from kick_api import KickAPI

api = KickAPI(CHANNEL_SLUG)


def now_iso():
    return datetime.now(timezone.utc).isoformat()


def ensure_channel():
    meta = api.resolve_channel(force=True)
    if not meta or not meta.get('broadcaster_user_id'):
        raise RuntimeError('Kanal çözülemedi. KICK_CHANNEL_SLUG doğru mu?')
    set_state('channel_slug', meta.get('slug') or CHANNEL_SLUG)
    set_state('channel_id', meta.get('broadcaster_user_id'))
    set_state('chatroom_id', meta.get('chatroom_id') or '')
    return meta


def ensure_live_stream(meta=None):
    active = get_active_stream()
    if active:
        if meta and meta.get('is_live'):
            update_stream_metadata(active['id'], meta)
        return active['id']
    meta = meta or api.resolve_channel(force=True)
    if not meta or not meta.get('is_live'):
        return None
    sid = create_stream(meta)
    set_state('active_stream_id', sid)
    set_state('offline_since', '')
    update_stream_metadata(sid, meta)
    print(f"[STREAM] LIVE başladı: #{sid} {meta.get('title') or ''}")
    return sid


def monitor_once():
    meta = api.resolve_channel(force=True)
    if not meta:
        print('[STREAM] API metadata alınamadı; mevcut state korunuyor.')
        return
    active = get_active_stream()
    if meta.get('is_live'):
        if not active:
            ensure_live_stream(meta)
        else:
            set_state('offline_since', '')
            update_stream_metadata(active['id'], meta)
        return
    if not active:
        return
    # Grace is tracked in state as an ISO timestamp.
    from db import get_state
    since = get_state('offline_since', '')
    if not since:
        set_state('offline_since', now_iso())
        print(f"[STREAM] Offline görüldü; {OFFLINE_GRACE_SECONDS}s grace başladı.")
        return
    try:
        dt = datetime.fromisoformat(since)
    except ValueError:
        set_state('offline_since', now_iso()); return
    if (datetime.now(timezone.utc) - dt).total_seconds() >= OFFLINE_GRACE_SECONDS:
        close_stream(active['id'], now_iso())
        set_state('active_stream_id', '')
        set_state('offline_since', '')
        print(f"[STREAM] Yayın kapandı: #{active['id']}")


def get_chatroom_id():
    meta = api.resolve_channel(force=False)
    return meta.get('chatroom_id') if meta else None


def get_channel_meta():
    return api.resolve_channel(force=False)


async def monitor_loop(stop_event: asyncio.Event):
    while not stop_event.is_set():
        try:
            await asyncio.to_thread(monitor_once)
        except Exception as e:
            print(f'[STREAM MONITOR] {e}')
        try:
            await asyncio.wait_for(stop_event.wait(), timeout=15)
        except asyncio.TimeoutError:
            pass
