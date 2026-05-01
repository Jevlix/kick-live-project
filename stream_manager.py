from datetime import datetime, timedelta
import subprocess
import os

from db import (
    get_active_stream,
    create_stream,
    close_stream,
    set_state,
    get_state,
)

STREAMER_NAME = "rraenee"
CHANNEL_ID = 26239555
CHATROOM_ID = 25951243
USER_ID = 27259381

KICK_CHANNEL_URL = f"https://kick.com/{STREAMER_NAME}"

OFFLINE_GRACE_MINUTES = 10
LIVE_CHECK_CACHE_SECONDS = 25

_last_live_check_at = None
_last_live_check_value = None
_last_live_check_source = "none"


def now_dt():
    return datetime.now()


def now_iso():
    return now_dt().isoformat()


def today_label():
    return now_dt().strftime("%Y-%m-%d")


def update_last_event():
    set_state("last_event_time", now_iso())


def update_socket_message_time():
    set_state("last_socket_message_time", now_iso())


def set_socket_connected(is_connected: bool):
    set_state("socket_connected", "1" if is_connected else "0")


def _set_cached_live_result(value, source):
    global _last_live_check_at, _last_live_check_value, _last_live_check_source

    _last_live_check_at = now_dt()
    _last_live_check_value = value
    _last_live_check_source = source

    set_state("last_live_check", now_iso())
    set_state("last_live_check_source", source)
    set_state(
        "last_live_check_value",
        "unknown" if value is None else ("live" if value else "offline")
    )


def _get_cached_live_result(force=False):
    global _last_live_check_at, _last_live_check_value, _last_live_check_source

    if force:
        return None

    if _last_live_check_at is None:
        return None

    age = now_dt() - _last_live_check_at
    if age.total_seconds() <= LIVE_CHECK_CACHE_SECONDS:
        return _last_live_check_value, _last_live_check_source

    return None


def _streamlink_live_check():
    """
    Streamlink canlı yayın varsa playable stream bulur.
    Offline ise genelde 'No playable streams found' döner.
    """
    try:
        cmd = [
            "python",
            "-m",
            "streamlink",
            "--json",
            KICK_CHANNEL_URL,
            "best"
        ]

        result = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            timeout=20
        )

        stdout = (result.stdout or "").strip()
        stderr = (result.stderr or "").strip()
        output = f"{stdout}\n{stderr}".lower()

        if result.returncode == 0:
            return True, "streamlink"

        if "no playable streams found" in output:
            return False, "streamlink"

        if "nostreamserror" in output:
            return False, "streamlink"

        if "error" in output:
            return None, "streamlink_error"

        return None, "streamlink_unknown"

    except subprocess.TimeoutExpired:
        return None, "streamlink_timeout"
    except Exception as e:
        print(f"[STREAMLINK CHECK HATASI] {e}")
        return None, "streamlink_exception"


def check_live_status(force=False):
    cached = _get_cached_live_result(force=force)
    if cached is not None:
        return cached

    value, source = _streamlink_live_check()
    _set_cached_live_result(value, source)
    return value, source


def ensure_live_stream(force=False):
    active = get_active_stream()
    if active:
        return active["id"]

    live_state, source = check_live_status(force=force)
    print(f"[LIVE CHECK] sonuç={live_state} kaynak={source}")

    if live_state is True:
        started = now_iso()
        label_date = today_label()

        stream_id = create_stream(
            STREAMER_NAME,
            CHANNEL_ID,
            started,
            label_date
        )

        set_state("active_stream_id", str(stream_id))
        set_state("offline_since", "")
        set_state("last_event_time", started)
        set_state("last_socket_message_time", started)
        set_state("socket_connected", "1")

        print(f"Yeni stream açıldı: {stream_id}")
        return stream_id

    return None


def _close_active_stream(active):
    close_stream(active["id"], now_iso())
    set_state("active_stream_id", "")
    set_state("offline_since", "")
    print(f"Stream kapandı: {active['id']}")
    
    # --- YENİ EKLENEN KISIM: OTOMATİK ANALİZ TETİKLEYİCİ ---
    label_date = active.get("label_date")
    if label_date:
        print(f"[{label_date}] Yayın sonlandı. İstatistik analizi arka planda başlatılıyor...")
        subprocess.Popen(["python", "analyzer.py", label_date])


def monitor_stream_status():
    active = get_active_stream()

    if not active:
        return

    live_state, source = check_live_status(force=True)
    print(f"[STREAM MONITOR] sonuç={live_state} kaynak={source}")

    if live_state is True:
        set_state("offline_since", "")
        return

    if live_state is False:
        offline_since = get_state("offline_since", "")

        if not offline_since:
            set_state("offline_since", now_iso())
            print(f"[STREAM] Offline algılandı, grace başladı. Stream id={active['id']}")
            return

        try:
            offline_dt = datetime.fromisoformat(offline_since)
        except ValueError:
            set_state("offline_since", now_iso())
            return

        if now_dt() - offline_dt >= timedelta(minutes=OFFLINE_GRACE_MINUTES):
            _close_active_stream(active)

        return

    # live durumu bilinmiyorsa fallback
    socket_connected = get_state("socket_connected", "0") == "1"
    last_socket_message_time = get_state("last_socket_message_time", "")
    last_event_time = get_state("last_event_time", "")

    ref_time = None
    for value in (last_socket_message_time, last_event_time):
        if value:
            try:
                parsed = datetime.fromisoformat(value)
                if ref_time is None or parsed > ref_time:
                    ref_time = parsed
            except ValueError:
                pass

    if socket_connected:
        set_state("offline_since", "")
        return

    if ref_time is None:
        offline_since = get_state("offline_since", "")
        if not offline_since:
            set_state("offline_since", now_iso())
            print(f"[STREAM] Referans zaman yok, grace başladı. Stream id={active['id']}")
            return

        try:
            offline_dt = datetime.fromisoformat(offline_since)
        except ValueError:
            set_state("offline_since", now_iso())
            return

        if now_dt() - offline_dt >= timedelta(minutes=OFFLINE_GRACE_MINUTES):
            _close_active_stream(active)

        return

    inactivity = now_dt() - ref_time

    if inactivity < timedelta(minutes=OFFLINE_GRACE_MINUTES):
        set_state("offline_since", "")
        return

    offline_since = get_state("offline_since", "")
    if not offline_since:
        set_state("offline_since", now_iso())
        print(f"[STREAM] Fallback offline grace başladı. Stream id={active['id']}")
        return

    try:
        offline_dt = datetime.fromisoformat(offline_since)
    except ValueError:
        set_state("offline_since", now_iso())
        return

    if now_dt() - offline_dt >= timedelta(minutes=OFFLINE_GRACE_MINUTES):
        _close_active_stream(active)


def get_active_stream_id():
    active = get_active_stream()
    if active:
        return active["id"]
    return None