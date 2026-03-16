import asyncio
import json
import sqlite3
from datetime import datetime
from typing import Set, Optional

import websockets
from fastapi import FastAPI, WebSocket, WebSocketDisconnect, Query
from fastapi.staticfiles import StaticFiles

from db import DB_PATH, init_db

KICK_PUSHER_KEY = "32cbd69e4b950bf97679"
KICK_WS_URL = f"wss://ws-us2.pusher.com/app/{KICK_PUSHER_KEY}?protocol=7&client=js&version=8.4.0&flash=false"

CHATROOM_ID = "25951243"

app = FastAPI(title="Kick Live Test")

connected_clients: Set[WebSocket] = set()
recent_messages = []

state = {
    "kick_connected": False,
    "kick_subscribed": False,
    "last_event": None,
    "last_error": None,
    "channel_id": CHATROOM_ID,
}


def now_iso():
    return datetime.utcnow().isoformat() + "Z"


def get_db_connection():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def row_to_dict(row):
    return dict(row) if row else None


def normalize_word(word: str) -> str:
    return "".join(ch for ch in (word or "").lower().strip() if ch.isalnum())


def format_human_date(date_str: Optional[str]) -> str:
    if not date_str:
        return "Bilinmeyen tarih"

    try:
        dt = datetime.strptime(date_str, "%Y-%m-%d")
        months = {
            1: "Ocak",
            2: "Şubat",
            3: "Mart",
            4: "Nisan",
            5: "Mayıs",
            6: "Haziran",
            7: "Temmuz",
            8: "Ağustos",
            9: "Eylül",
            10: "Ekim",
            11: "Kasım",
            12: "Aralık",
        }
        return f"{dt.day} {months[dt.month]}"
    except Exception:
        return date_str


def build_stream_label(stream_row):
    if not stream_row:
        return "Yayın"

    label_date = stream_row["label_date"]
    session_type = stream_row["session_type"]

    human = format_human_date(label_date)

    if session_type == "offstream":
        return f"{human} offstream"
    return f"{human} yayını"


def build_summary_from_events(events):
    stats = {
        "total_messages": 0,
        "unique_users": 0,
        "deleted_messages": 0,
        "timeouts": 0,
        "bans": 0,
        "unbans": 0,
        "subscriptions": 0,
        "gift_subscriptions": 0,
        "other_events": 0,
    }

    users_map = {}
    words_map = {}
    emotes_map = {}
    spam_map = {}
    moderation_actions = []
    user_message_cache = {}
    events_out = []

    for row in events:
        event_name = row["event_name"]
        event_type = row["event_type"]
        username = row["username"]
        message = row["message"]
        target_username = row["target_username"]
        moderator = row["moderator"]
        timestamp = row["timestamp"]
        reason = row["reason"]
        duration = row["duration"]
        session_type = row["session_type"] if "session_type" in row.keys() else "stream"

        if username:
            if username not in users_map:
                users_map[username] = {
                    "n": username,
                    "mc": 0,
                    "mod_received": {
                        "timeouts": 0,
                        "bans": 0,
                        "unbans": 0,
                        "deleted_messages": 0
                    },
                    "mod_history_received": []
                }

        if event_type == "chat":
            stats["total_messages"] += 1

            if username:
                users_map[username]["mc"] += 1

            if message:
                # kelimeler
                for raw_word in str(message).split():
                    word = normalize_word(raw_word)
                    if len(word) < 2:
                        continue

                    if word not in words_map:
                        words_map[word] = {
                            "w": word,
                            "c": 0,
                            "top_user_map": {}
                        }

                    words_map[word]["c"] += 1
                    if username:
                        words_map[word]["top_user_map"][username] = words_map[word]["top_user_map"].get(username, 0) + 1

                # emotes
                import re
                emote_matches = re.findall(r"\[emote:(\d+):([^\]]+)\]", str(message))
                for emote_id, emote_name in emote_matches:
                    key = f"{emote_id}:{emote_name}"
                    if key not in emotes_map:
                        emotes_map[key] = {
                            "id": emote_id,
                            "n": emote_name,
                            "c": 0,
                            "unique_users_set": set(),
                            "top_user_map": {}
                        }

                    emotes_map[key]["c"] += 1
                    if username:
                        emotes_map[key]["unique_users_set"].add(username)
                        emotes_map[key]["top_user_map"][username] = emotes_map[key]["top_user_map"].get(username, 0) + 1

                # basit spam tekrar algısı
                if username:
                    prev = user_message_cache.get(username)
                    if prev and prev == message:
                        key = str(message).strip().lower()
                        if key:
                            if key not in spam_map:
                                spam_map[key] = {
                                    "key": key,
                                    "m": message,
                                    "c": 0,
                                    "top_user_map": {}
                                }
                            spam_map[key]["c"] += 1
                            spam_map[key]["top_user_map"][username] = spam_map[key]["top_user_map"].get(username, 0) + 1

                    user_message_cache[username] = message

        elif event_type == "deleted":
            stats["deleted_messages"] += 1

            if target_username and target_username not in users_map:
                users_map[target_username] = {
                    "n": target_username,
                    "mc": 0,
                    "mod_received": {
                        "timeouts": 0,
                        "bans": 0,
                        "unbans": 0,
                        "deleted_messages": 0
                    },
                    "mod_history_received": []
                }

            if target_username:
                users_map[target_username]["mod_received"]["deleted_messages"] += 1
                users_map[target_username]["mod_history_received"].append({
                    "action": "deleted",
                    "mod": moderator,
                    "reason": reason,
                    "duration": duration,
                    "msg": message,
                    "t": timestamp
                })

            moderation_actions.append({
                "action": "deleted",
                "mod": moderator,
                "target": target_username,
                "reason": reason,
                "duration": duration,
                "msg": message,
                "t": timestamp
            })

        elif event_type == "ban":
            if row["permanent"]:
                stats["bans"] += 1
                mod_action = "ban"
            else:
                stats["timeouts"] += 1
                mod_action = "timeout"

            if target_username and target_username not in users_map:
                users_map[target_username] = {
                    "n": target_username,
                    "mc": 0,
                    "mod_received": {
                        "timeouts": 0,
                        "bans": 0,
                        "unbans": 0,
                        "deleted_messages": 0
                    },
                    "mod_history_received": []
                }

            if target_username:
                if mod_action == "ban":
                    users_map[target_username]["mod_received"]["bans"] += 1
                else:
                    users_map[target_username]["mod_received"]["timeouts"] += 1

                users_map[target_username]["mod_history_received"].append({
                    "action": mod_action,
                    "mod": moderator,
                    "reason": reason,
                    "duration": duration,
                    "msg": message,
                    "t": timestamp
                })

            moderation_actions.append({
                "action": mod_action,
                "mod": moderator,
                "target": target_username,
                "reason": reason,
                "duration": duration,
                "msg": message,
                "t": timestamp
            })

        elif event_type == "unban":
            stats["unbans"] += 1

            if target_username and target_username not in users_map:
                users_map[target_username] = {
                    "n": target_username,
                    "mc": 0,
                    "mod_received": {
                        "timeouts": 0,
                        "bans": 0,
                        "unbans": 0,
                        "deleted_messages": 0
                    },
                    "mod_history_received": []
                }

            if target_username:
                users_map[target_username]["mod_received"]["unbans"] += 1
                users_map[target_username]["mod_history_received"].append({
                    "action": "unban",
                    "mod": moderator,
                    "reason": reason,
                    "duration": duration,
                    "msg": message,
                    "t": timestamp
                })

            moderation_actions.append({
                "action": "unban",
                "mod": moderator,
                "target": target_username,
                "reason": reason,
                "duration": duration,
                "msg": message,
                "t": timestamp
            })

        elif event_type == "subscription":
            stats["subscriptions"] += 1

        elif event_type == "gift_sub":
            stats["gift_subscriptions"] += 1

        else:
            stats["other_events"] += 1

        events_out.append({
            "id": row["id"],
            "stream_id": row["stream_id"],
            "timestamp": row["timestamp"],
            "event_name": row["event_name"],
            "event_type": row["event_type"],
            "username": row["username"],
            "target_username": row["target_username"],
            "moderator": row["moderator"],
            "message": row["message"],
            "reason": row["reason"],
            "duration": row["duration"],
            "permanent": row["permanent"],
            "session_type": session_type,
        })

    stats["unique_users"] = len(users_map)

    users = sorted(users_map.values(), key=lambda x: x["mc"], reverse=True)[:200]

    words = []
    for word_data in words_map.values():
        top = sorted(
            [{"u": u, "c": c} for u, c in word_data["top_user_map"].items()],
            key=lambda x: x["c"],
            reverse=True
        )[:5]

        words.append({
            "w": word_data["w"],
            "c": word_data["c"],
            "top": top
        })

    words.sort(key=lambda x: x["c"], reverse=True)
    words = words[:200]

    emotes = []
    for emote_data in emotes_map.values():
        top = sorted(
            [{"u": u, "c": c} for u, c in emote_data["top_user_map"].items()],
            key=lambda x: x["c"],
            reverse=True
        )[:5]

        emotes.append({
            "id": emote_data["id"],
            "n": emote_data["n"],
            "c": emote_data["c"],
            "unique_users": len(emote_data["unique_users_set"]),
            "top": top
        })

    emotes.sort(key=lambda x: x["c"], reverse=True)
    emotes = emotes[:200]

    spam = []
    for spam_data in spam_map.values():
        if spam_data["c"] < 2:
            continue

        top = sorted(
            [{"u": u, "c": c} for u, c in spam_data["top_user_map"].items()],
            key=lambda x: x["c"],
            reverse=True
        )[:10]

        spam.append({
            "key": spam_data["key"],
            "m": spam_data["m"],
            "c": spam_data["c"],
            "top": top
        })

    spam.sort(key=lambda x: x["c"], reverse=True)
    spam = spam[:100]

    mods_map = {}
    for action in moderation_actions:
        mod_name = action["mod"] or "Unknown Mod"
        if mod_name not in mods_map:
            mods_map[mod_name] = {
                "n": mod_name,
                "total_actions": 0,
                "timeouts": 0,
                "bans": 0,
                "unbans": 0,
                "deleted_messages": 0,
                "logs": [],
                "top_targets_map": {},
                "top_reasons_map": {},
                "last_action_at": None
            }

        mod = mods_map[mod_name]
        mod["total_actions"] += 1

        if action["action"] == "timeout":
            mod["timeouts"] += 1
        elif action["action"] == "ban":
            mod["bans"] += 1
        elif action["action"] == "unban":
            mod["unbans"] += 1
        elif action["action"] == "deleted":
            mod["deleted_messages"] += 1

        if action["target"]:
            mod["top_targets_map"][action["target"]] = mod["top_targets_map"].get(action["target"], 0) + 1

        if action["reason"]:
            mod["top_reasons_map"][action["reason"]] = mod["top_reasons_map"].get(action["reason"], 0) + 1

        mod["logs"].append(action)

        if not mod["last_action_at"] or action["t"] > mod["last_action_at"]:
            mod["last_action_at"] = action["t"]

    mods = []
    for mod in mods_map.values():
        mod["top_targets"] = sorted(
            [{"n": k, "c": v} for k, v in mod["top_targets_map"].items()],
            key=lambda x: x["c"],
            reverse=True
        )[:10]

        mod["top_reasons"] = sorted(
            [{"r": k, "c": v} for k, v in mod["top_reasons_map"].items()],
            key=lambda x: x["c"],
            reverse=True
        )[:10]

        mod["logs"] = sorted(mod["logs"], key=lambda x: x["t"], reverse=True)[:120]

        del mod["top_targets_map"]
        del mod["top_reasons_map"]
        mods.append(mod)

    mods.sort(key=lambda x: x["total_actions"], reverse=True)

    moderation = {
        "summary": {
            "total_actions": len(moderation_actions),
            "timeouts": stats["timeouts"],
            "bans": stats["bans"],
            "unbans": stats["unbans"],
            "deleted_messages": stats["deleted_messages"],
        },
        "mods": mods[:100],
        "recent_actions": sorted(moderation_actions, key=lambda x: x["t"], reverse=True)[:150]
    }

    game_special = {
        "all_pool": words[:100],
        "top_5_users": users[:5],
        "top_5_words": words[:5],
        "top_5_emotes": emotes[:5]
    }

    return {
        "stats": stats,
        "users": users,
        "words": words,
        "emotes": emotes,
        "spam": spam,
        "moderation": moderation,
        "events": events_out,
        "game_special": game_special
    }


def push_recent(payload: dict):
    recent_messages.append(payload)
    if len(recent_messages) > 50:
        recent_messages.pop(0)


async def broadcast(payload: dict):
    dead = []
    for ws in connected_clients:
        try:
            await ws.send_text(json.dumps(payload, ensure_ascii=False))
        except Exception:
            dead.append(ws)

    for ws in dead:
        try:
            connected_clients.remove(ws)
        except KeyError:
            pass


async def send_system_message(message: str):
    payload = {
        "type": "system",
        "message": message,
        "time": now_iso()
    }
    push_recent(payload)
    await broadcast(payload)


@app.get("/health")
async def health():
    return {
        "ok": True,
        "state": state,
        "clients": len(connected_clients),
        "recent_count": len(recent_messages),
    }


@app.get("/api/streams")
async def api_streams(limit: int = Query(30, ge=1, le=200)):
    conn = get_db_connection()
    try:
        rows = conn.execute("""
            SELECT
                id,
                streamer_name,
                channel_id,
                started_at,
                ended_at,
                label_date,
                status,
                reconnect_group,
                session_type,
                created_at,
                updated_at
            FROM streams
            ORDER BY id DESC
            LIMIT ?
        """, (limit,)).fetchall()

        enriched = []
        for r in rows:
            d = dict(r)
            d["display_label"] = build_stream_label(r)
            enriched.append(d)

        return {
            "ok": True,
            "streams": enriched
        }
    finally:
        conn.close()


@app.get("/api/streams/active")
async def api_active_stream():
    conn = get_db_connection()
    try:
        row = conn.execute("""
            SELECT
                id,
                streamer_name,
                channel_id,
                started_at,
                ended_at,
                label_date,
                status,
                reconnect_group,
                session_type,
                created_at,
                updated_at
            FROM streams
            WHERE status = 'live'
              AND session_type = 'stream'
            ORDER BY id DESC
            LIMIT 1
        """).fetchone()

        data = row_to_dict(row)
        if data:
            data["display_label"] = build_stream_label(row)

        return {
            "ok": True,
            "stream": data
        }
    finally:
        conn.close()


@app.get("/api/streams/{stream_id}/events")
async def api_stream_events(
    stream_id: int,
    limit: int = Query(500, ge=1, le=5000)
):
    conn = get_db_connection()
    try:
        stream = conn.execute("""
            SELECT
                id,
                streamer_name,
                channel_id,
                started_at,
                ended_at,
                label_date,
                status,
                reconnect_group,
                session_type,
                created_at,
                updated_at
            FROM streams
            WHERE id = ?
            LIMIT 1
        """, (stream_id,)).fetchone()

        if not stream:
            return {
                "ok": False,
                "error": "stream_not_found"
            }

        rows = conn.execute("""
            SELECT
                id,
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
                raw_json,
                created_at
            FROM events
            WHERE stream_id = ?
            ORDER BY id DESC
            LIMIT ?
        """, (stream_id, limit)).fetchall()

        stream_dict = dict(stream)
        stream_dict["display_label"] = build_stream_label(stream)

        return {
            "ok": True,
            "stream": stream_dict,
            "events": [dict(r) for r in rows]
        }
    finally:
        conn.close()


@app.get("/api/data")
async def api_data(
    mode: str = Query("live"),
    stream_id: int | None = Query(None),
    date: str | None = Query(None),
    month: str | None = Query(None)
):
    conn = get_db_connection()
    try:
        selected_stream = None
        rows = []
        meta = {
            "label": "Seçili veri",
            "live_stream_active": False,
            "warning": None,
            "session_type": None,
        }

        if mode == "live":
            selected_stream = conn.execute("""
                SELECT *
                FROM streams
                WHERE status = 'live'
                  AND session_type = 'stream'
                ORDER BY id DESC
                LIMIT 1
            """).fetchone()

            if not selected_stream:
                meta["label"] = "Canlı yayın"
                meta["live_stream_active"] = False
                meta["warning"] = "Canlı yayın kapalı"
                return {
                    "ok": True,
                    "mode": mode,
                    "stream": None,
                    "meta": meta,
                    "summary": build_summary_from_events([])
                }

            rows = conn.execute("""
                SELECT *
                FROM events
                WHERE stream_id = ?
                ORDER BY id DESC
            """, (selected_stream["id"],)).fetchall()

            meta["label"] = "Canlı yayın"
            meta["live_stream_active"] = True
            meta["session_type"] = "stream"

        elif mode == "offstream_live":
            selected_stream = conn.execute("""
                SELECT *
                FROM streams
                WHERE session_type = 'offstream'
                  AND label_date = date('now')
                ORDER BY id DESC
                LIMIT 1
            """).fetchone()

            if not selected_stream:
                meta["label"] = "Canlı offstream"
                meta["warning"] = "Bugün için offstream verisi yok"
                meta["session_type"] = "offstream"
                return {
                    "ok": True,
                    "mode": mode,
                    "stream": None,
                    "meta": meta,
                    "summary": build_summary_from_events([])
                }

            rows = conn.execute("""
                SELECT *
                FROM events
                WHERE stream_id = ?
                ORDER BY id DESC
            """, (selected_stream["id"],)).fetchall()

            meta["label"] = "Canlı offstream"
            meta["session_type"] = "offstream"

        elif mode == "stream":
            if not stream_id:
                return {
                    "ok": False,
                    "error": "stream_id_required"
                }

            selected_stream = conn.execute("""
                SELECT *
                FROM streams
                WHERE id = ?
                LIMIT 1
            """, (stream_id,)).fetchone()

            if not selected_stream:
                return {
                    "ok": False,
                    "error": "stream_not_found"
                }

            rows = conn.execute("""
                SELECT *
                FROM events
                WHERE stream_id = ?
                ORDER BY id DESC
            """, (stream_id,)).fetchall()

            meta["label"] = build_stream_label(selected_stream)
            meta["session_type"] = selected_stream["session_type"]

        elif mode == "all":
            rows = conn.execute("""
                SELECT *
                FROM events
                ORDER BY id DESC
            """).fetchall()

            meta["label"] = "Tüm veriler"

        elif mode == "day":
            if not date:
                return {
                    "ok": False,
                    "error": "date_required"
                }

            streams = conn.execute("""
                SELECT *
                FROM streams
                WHERE label_date = ?
                  AND session_type = 'stream'
                ORDER BY id DESC
            """, (date,)).fetchall()

            stream_ids = [r["id"] for r in streams]

            if stream_ids:
                placeholders = ",".join("?" for _ in stream_ids)
                rows = conn.execute(f"""
                    SELECT *
                    FROM events
                    WHERE stream_id IN ({placeholders})
                    ORDER BY id DESC
                """, stream_ids).fetchall()
            else:
                rows = []

            meta["label"] = f"{format_human_date(date)} verisi"
            meta["stream_count"] = len(stream_ids)
            meta["stream_ids"] = stream_ids
            meta["session_type"] = "stream"

        elif mode == "offstream_day":
            if not date:
                return {
                    "ok": False,
                    "error": "date_required"
                }

            streams = conn.execute("""
                SELECT *
                FROM streams
                WHERE label_date = ?
                  AND session_type = 'offstream'
                ORDER BY id DESC
            """, (date,)).fetchall()

            stream_ids = [r["id"] for r in streams]

            if stream_ids:
                placeholders = ",".join("?" for _ in stream_ids)
                rows = conn.execute(f"""
                    SELECT *
                    FROM events
                    WHERE stream_id IN ({placeholders})
                    ORDER BY id DESC
                """, stream_ids).fetchall()
            else:
                rows = []

            meta["label"] = f"{format_human_date(date)} offstream"
            meta["stream_count"] = len(stream_ids)
            meta["stream_ids"] = stream_ids
            meta["session_type"] = "offstream"

        elif mode == "week":
            rows = conn.execute("""
                SELECT *
                FROM events
                WHERE date(timestamp) >= date('now', '-7 day')
                ORDER BY id DESC
            """).fetchall()

            meta["label"] = "Son 7 gün verisi"

        elif mode == "month":
            if not month:
                return {
                    "ok": False,
                    "error": "month_required"
                }

            rows = conn.execute("""
                SELECT *
                FROM events
                WHERE substr(timestamp, 1, 7) = ?
                ORDER BY id DESC
            """, (month,)).fetchall()

            try:
                month_dt = datetime.strptime(month, "%Y-%m")
                months = {
                    1: "Ocak",
                    2: "Şubat",
                    3: "Mart",
                    4: "Nisan",
                    5: "Mayıs",
                    6: "Haziran",
                    7: "Temmuz",
                    8: "Ağustos",
                    9: "Eylül",
                    10: "Ekim",
                    11: "Kasım",
                    12: "Aralık",
                }
                meta["label"] = f"{months[month_dt.month]} {month_dt.year} verisi"
            except Exception:
                meta["label"] = f"Ay: {month}"

        else:
            return {
                "ok": False,
                "error": "invalid_mode"
            }

        summary = build_summary_from_events(rows)

        stream_dict = row_to_dict(selected_stream)
        if stream_dict:
            stream_dict["display_label"] = build_stream_label(selected_stream)

        return {
            "ok": True,
            "mode": mode,
            "stream": stream_dict,
            "meta": meta,
            "summary": summary
        }
    finally:
        conn.close()


@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):
    await websocket.accept()
    connected_clients.add(websocket)

    try:
        await websocket.send_text(json.dumps({
            "type": "bootstrap",
            "time": now_iso(),
            "state": state,
            "recent": recent_messages
        }, ensure_ascii=False))
    except Exception:
        pass

    try:
        while True:
            await websocket.receive_text()
    except WebSocketDisconnect:
        if websocket in connected_clients:
            connected_clients.remove(websocket)
    except Exception:
        if websocket in connected_clients:
            connected_clients.remove(websocket)


async def kick_listener():
    while True:
        try:
            print(f"[KICK] Bağlanılıyor... channel_id={CHATROOM_ID}")

            async with websockets.connect(
                KICK_WS_URL,
                ping_interval=30,
                ping_timeout=30,
                close_timeout=10,
                max_size=2**20
            ) as ws:
                state["kick_connected"] = True
                state["kick_subscribed"] = False
                state["last_error"] = None

                subscribe_msg = {
                    "event": "pusher:subscribe",
                    "data": {
                        "auth": "",
                        "channel": f"chatrooms.{CHATROOM_ID}.v2"
                    }
                }

                await ws.send(json.dumps(subscribe_msg))
                await send_system_message(f"Kick socket açıldı. Kanal: chatrooms.{CHATROOM_ID}.v2")

                async for raw_message in ws:
                    try:
                        raw = json.loads(raw_message)
                    except Exception as e:
                        state["last_error"] = f"JSON parse error: {repr(e)}"
                        continue

                    event_type = raw.get("event", "")
                    state["last_event"] = event_type

                    if event_type == "pusher:ping":
                        await ws.send(json.dumps({"event": "pusher:pong", "data": {}}))
                        continue

                    if event_type in ["pusher_internal:subscription_succeeded", "pusher:subscription_succeeded"]:
                        state["kick_subscribed"] = True
                        await send_system_message("Kick kanal aboneliği başarılı.")
                        continue

                    if event_type in ["pusher:connection_established", "pusher:pong"]:
                        continue

                    data = raw.get("data")
                    if isinstance(data, str):
                        try:
                            data = json.loads(data)
                        except Exception:
                            data = {"raw_data": raw.get("data")}

                    normalized_event = event_type.split("\\")[-1] if "\\" in event_type else event_type

                    if normalized_event == "ChatMessageEvent":
                        username = (
                            data.get("sender", {}).get("username")
                            or data.get("user", {}).get("username")
                            or "Unknown"
                        )

                        content = (
                            data.get("content")
                            or data.get("message", {}).get("content")
                            or data.get("message", {}).get("message")
                            or ""
                        )

                        payload = {
                            "type": "chat",
                            "event": normalized_event,
                            "user": username,
                            "msg": str(content),
                            "time": now_iso(),
                        }
                        push_recent(payload)
                        await broadcast(payload)
                        print(f"[CHAT] {username}: {content}")
                        continue

                    payload = {
                        "type": "other",
                        "event": normalized_event,
                        "data": data,
                        "time": now_iso(),
                    }
                    push_recent(payload)
                    await broadcast(payload)

        except Exception as e:
            state["kick_connected"] = False
            state["kick_subscribed"] = False
            state["last_error"] = repr(e)
            print(f"[KICK HATA] {repr(e)}")
            await send_system_message(f"Kick bağlantı hatası: {repr(e)}")
            await asyncio.sleep(5)


app.mount("/", StaticFiles(directory="static", html=True), name="static")


@app.on_event("startup")
async def startup_event():
    init_db()
    asyncio.create_task(kick_listener())