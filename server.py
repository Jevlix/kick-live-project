import asyncio
import json
import os
from datetime import datetime
from typing import Set

import websockets
from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.responses import HTMLResponse
from fastapi.staticfiles import StaticFiles

KICK_PUSHER_KEY = "32cbd69e4b950bf97679"
KICK_WS_URL = f"wss://ws-us2.pusher.com/app/{KICK_PUSHER_KEY}?protocol=7&client=js&version=8.4.0&flash=false"

# Buraya kendi test edeceğin yayıncının chatroom ID'sini yaz
CHANNEL_ID = "25951243"

app = FastAPI(title="Kick Live Test")

connected_clients: Set[WebSocket] = set()
recent_messages = []

state = {
    "kick_connected": False,
    "kick_subscribed": False,
    "last_event": None,
    "last_error": None,
    "channel_id": CHANNEL_ID,
}


def now_iso():
    return datetime.utcnow().isoformat() + "Z"


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
            print(f"[KICK] Bağlanılıyor... channel_id={CHANNEL_ID}")

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
                        "channel": f"chatrooms.{CHANNEL_ID}.v2"
                    }
                }

                await ws.send(json.dumps(subscribe_msg))
                await send_system_message(f"Kick socket açıldı. Kanal: chatrooms.{CHANNEL_ID}.v2")

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
    asyncio.create_task(kick_listener())