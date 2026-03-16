import asyncio
import json
from datetime import datetime

import websockets

from db import (
    add_event,
    get_active_stream,
    get_or_create_today_offstream,
    LOGS_DIR,
)
from stream_manager import (
    STREAMER_NAME,
    CHATROOM_ID,
    CHANNEL_ID,
    ensure_live_stream,
    update_last_event,
    update_socket_message_time,
    set_socket_connected,
    monitor_stream_status,
)

KICK_PUSHER_KEY = "32cbd69e4b950bf97679"
KICK_WS_URL = f"wss://ws-us2.pusher.com/app/{KICK_PUSHER_KEY}?protocol=7&client=js&version=8.4.0&flash=false"

MAX_FILE_SIZE = 200 * 1024 * 1024
BATCH_SIZE = 50
RECONNECT_DELAY = 5
FORCE_LIVE_CHECK_EVERY_SECONDS = 20


class KickRecorder:
    def __init__(self):
        self.queue = asyncio.Queue()
        self.running = True
        self.file_index_map = {}
        self.last_forced_live_check_ts = 0.0

        self.stats = {
            "chat_messages": 0,
            "deleted_messages": 0,
            "timeouts": 0,
            "bans": 0,
            "unbans": 0,
            "subscriptions": 0,
            "gift_subscriptions": 0,
            "other_events": 0,
            "stream_events": 0,
            "offstream_events": 0,
        }

    def safe_get_first(self, obj, *paths, default=None):
        for path in paths:
            current = obj
            ok = True
            for key in path:
                if isinstance(current, dict) and key in current:
                    current = current[key]
                else:
                    ok = False
                    break
            if ok and current not in (None, "", {}, []):
                return current
        return default

    def extract_event_name(self, event_type):
        if not event_type:
            return "UnknownEvent"

        s = str(event_type)
        if "\\" in s:
            s = s.split("\\")[-1]
        if "." in s:
            s = s.split(".")[-1]
        return s or "UnknownEvent"

    def get_session_log_path(self, session_id: int, label_date: str, session_type: str):
        file_index = self.file_index_map.get((session_type, session_id), 1)

        if session_type == "offstream":
            filename = f"kick_logs_{STREAMER_NAME}_{label_date}_offstream{session_id}_part{file_index}.jsonl"
        else:
            filename = f"kick_logs_{STREAMER_NAME}_{label_date}_stream{session_id}_part{file_index}.jsonl"

        return LOGS_DIR / filename

    def rotate_log_if_needed(self, session_id: int, label_date: str, session_type: str):
        path = self.get_session_log_path(session_id, label_date, session_type)

        if path.exists() and path.stat().st_size >= MAX_FILE_SIZE:
            key = (session_type, session_id)
            self.file_index_map[key] = self.file_index_map.get(key, 1) + 1
            new_path = self.get_session_log_path(session_id, label_date, session_type)
            print(f"[!] Yeni log part dosyası: {new_path.name}")

    async def file_writer_worker(self):
        print("[*] DB + raw log yazıcı başlatıldı.")
        batch = []

        while self.running or not self.queue.empty():
            try:
                try:
                    item = await asyncio.wait_for(self.queue.get(), timeout=1.0)
                    batch.append(item)
                except asyncio.TimeoutError:
                    pass

                if len(batch) >= BATCH_SIZE or (batch and self.queue.empty()):
                    for entry in batch:
                        session_id = entry["stream_id"]
                        label_date = entry["stream_label_date"]
                        session_type = entry.get("session_type", "stream")

                        self.rotate_log_if_needed(session_id, label_date, session_type)
                        path = self.get_session_log_path(session_id, label_date, session_type)

                        with open(path, "a", encoding="utf-8") as f:
                            f.write(json.dumps(entry, ensure_ascii=False) + "\n")

                        add_event(
                            stream_id=session_id,
                            timestamp=entry["t"],
                            event_name=entry["e"],
                            event_type=entry.get("type"),
                            username=entry.get("user"),
                            target_username=entry.get("target_user"),
                            moderator=entry.get("mod"),
                            message=entry.get("msg"),
                            reason=entry.get("reason"),
                            duration=entry.get("duration"),
                            permanent=1 if entry.get("permanent") else 0,
                            session_type=session_type,
                            raw_json=json.dumps(entry.get("raw", {}), ensure_ascii=False)
                        )

                    for _ in range(len(batch)):
                        self.queue.task_done()

                    batch = []

            except Exception as e:
                print(f"[YAZICI HATASI] {e}")
                await asyncio.sleep(2)

        print("[*] Yazıcı kapandı.")

    async def stream_watchdog(self):
        while self.running:
            try:
                await asyncio.to_thread(monitor_stream_status)
            except Exception as e:
                print(f"[WATCHDOG HATASI] {e}")
            await asyncio.sleep(60)

    async def _resolve_session_for_entry(self):
        active = get_active_stream()
        if active:
            return {
                "stream_id": active["id"],
                "stream_label_date": active["label_date"],
                "session_type": active.get("session_type", "stream") or "stream",
            }

        now_ts = asyncio.get_running_loop().time()
        should_force = (now_ts - self.last_forced_live_check_ts) >= FORCE_LIVE_CHECK_EVERY_SECONDS

        if should_force:
            self.last_forced_live_check_ts = now_ts
            await asyncio.to_thread(ensure_live_stream, True)

        active = get_active_stream()
        if active:
            return {
                "stream_id": active["id"],
                "stream_label_date": active["label_date"],
                "session_type": active.get("session_type", "stream") or "stream",
            }

        offstream_id = await asyncio.to_thread(
            get_or_create_today_offstream,
            STREAMER_NAME,
            CHANNEL_ID
        )

        today_label = datetime.now().strftime("%Y-%m-%d")
        return {
            "stream_id": offstream_id,
            "stream_label_date": today_label,
            "session_type": "offstream",
        }

    async def _attach_session_to_entry(self, entry):
        session_info = await self._resolve_session_for_entry()
        entry["stream_id"] = session_info["stream_id"]
        entry["stream_label_date"] = session_info["stream_label_date"]
        entry["session_type"] = session_info["session_type"]

        if entry["session_type"] == "offstream":
            self.stats["offstream_events"] += 1
        else:
            self.stats["stream_events"] += 1

        await self.queue.put(entry)

    async def socket_listener(self):
        while self.running:
            try:
                print(f"[KICK] Bağlanılıyor... chatroom_id={CHATROOM_ID}")

                async with websockets.connect(
                    KICK_WS_URL,
                    ping_interval=30,
                    ping_timeout=30,
                    close_timeout=10,
                    max_size=None
                ) as ws:
                    set_socket_connected(True)

                    subscribe_msg = {
                        "event": "pusher:subscribe",
                        "data": {
                            "auth": "",
                            "channel": f"chatrooms.{CHATROOM_ID}.v2"
                        }
                    }

                    await ws.send(json.dumps(subscribe_msg))
                    print(f"[+] {STREAMER_NAME} kanalına bağlantı başarılı. İzleniyor...\n" + "-" * 60)

                    async for message in ws:
                        if not self.running:
                            break

                        update_socket_message_time()

                        try:
                            raw_data = json.loads(message)
                        except json.JSONDecodeError:
                            continue

                        event_type = raw_data.get("event")

                        if event_type == "pusher:ping":
                            await ws.send(json.dumps({"event": "pusher:pong", "data": {}}))
                            continue

                        if event_type in [
                            "pusher:pong",
                            "pusher:connection_established",
                            "pusher_internal:subscription_succeeded",
                            "pusher:subscription_succeeded"
                        ]:
                            continue

                        data = None
                        if "data" in raw_data:
                            if isinstance(raw_data["data"], str):
                                try:
                                    data = json.loads(raw_data["data"])
                                except json.JSONDecodeError:
                                    continue
                            elif isinstance(raw_data["data"], dict):
                                data = raw_data["data"]

                        if data is None:
                            continue

                        update_last_event()

                        event_name = self.extract_event_name(event_type)

                        log_entry = {
                            "t": datetime.now().isoformat(),
                            "e": event_name,
                            "type": "other"
                        }

                        if event_name == "ChatMessageEvent":
                            self.stats["chat_messages"] += 1

                            user_name = self.safe_get_first(
                                data,
                                ("sender", "username"),
                                ("user", "username"),
                                default="Unknown"
                            )

                            message_text = self.safe_get_first(
                                data,
                                ("content",),
                                ("message", "content"),
                                default=""
                            )

                            log_entry.update({
                                "user": user_name,
                                "msg": str(message_text),
                                "type": "chat",
                                "raw": data
                            })

                            print(f"[CHAT] {user_name}: {message_text}")

                        elif event_name == "MessageDeletedEvent":
                            self.stats["deleted_messages"] += 1

                            msg_id = self.safe_get_first(
                                data,
                                ("message", "id"),
                                ("message_id",),
                                ("id",),
                                default=None
                            )

                            target_user = self.safe_get_first(
                                data,
                                ("message", "sender", "username"),
                                ("user", "username"),
                                ("target_user", "username"),
                                default=None
                            )

                            deleted_msg = self.safe_get_first(
                                data,
                                ("message", "content"),
                                ("message", "body"),
                                ("content",),
                                default=None
                            )

                            mod_name = self.safe_get_first(
                                data,
                                ("moderator", "username"),
                                ("mod_user", "username"),
                                ("deleted_by", "username"),
                                ("actor", "username"),
                                default="Unknown Mod"
                            )

                            log_entry.update({
                                "mod": mod_name,
                                "target_user": target_user,
                                "msg_id": msg_id,
                                "msg": deleted_msg,
                                "type": "deleted",
                                "raw": data
                            })

                            print(f"[SİLİNDİ] Mesaj ID: {msg_id} | Mod: {mod_name}")

                        elif event_name == "UserBannedEvent":
                            user_name = self.safe_get_first(
                                data,
                                ("user", "username"),
                                ("banned_user", "username"),
                                ("target_user", "username"),
                                default="Unknown"
                            )

                            mod_name = self.safe_get_first(
                                data,
                                ("banned_by", "username"),
                                ("mod_user", "username"),
                                ("moderator", "username"),
                                ("actor", "username"),
                                default="Unknown Mod"
                            )

                            reason = self.safe_get_first(
                                data,
                                ("reason",),
                                ("ban_reason",),
                                default=None
                            )

                            duration = self.safe_get_first(
                                data,
                                ("duration",),
                                ("ban_duration",),
                                default=None
                            )

                            try:
                                duration_int = int(duration) if duration not in (None, "", "null") else None
                            except (TypeError, ValueError):
                                duration_int = None

                            permanent = 1 if duration_int in (None, 0) else 0

                            if permanent:
                                self.stats["bans"] += 1
                                action_type = "ban"
                            else:
                                self.stats["timeouts"] += 1
                                action_type = "timeout"

                            log_entry.update({
                                "user": user_name,
                                "target_user": user_name,
                                "mod": mod_name,
                                "reason": reason,
                                "duration": duration_int,
                                "permanent": permanent,
                                "type": action_type,
                                "raw": data
                            })

                            print(f"[BAN/TIMEOUT] Kullanıcı: {user_name} | Mod: {mod_name} | Süre: {duration_int}")

                        elif event_name == "UserUnbannedEvent":
                            self.stats["unbans"] += 1

                            user_name = self.safe_get_first(
                                data,
                                ("user", "username"),
                                ("unbanned_user", "username"),
                                ("target_user", "username"),
                                default="Unknown"
                            )

                            mod_name = self.safe_get_first(
                                data,
                                ("unbanned_by", "username"),
                                ("mod_user", "username"),
                                ("moderator", "username"),
                                ("actor", "username"),
                                default="Unknown Mod"
                            )

                            reason = self.safe_get_first(
                                data,
                                ("reason",),
                                default=None
                            )

                            log_entry.update({
                                "user": user_name,
                                "target_user": user_name,
                                "mod": mod_name,
                                "reason": reason,
                                "type": "unban",
                                "raw": data
                            })

                            print(f"[UNBAN] Kullanıcı: {user_name} | Mod: {mod_name}")

                        elif event_name == "SubscriptionEvent":
                            self.stats["subscriptions"] += 1

                            user_name = self.safe_get_first(
                                data,
                                ("user", "username"),
                                ("subscriber", "username"),
                                ("sender", "username"),
                                default="Unknown"
                            )

                            log_entry.update({
                                "user": user_name,
                                "type": "subscription",
                                "raw": data
                            })

                            print(f"[SUB] Kullanıcı: {user_name}")

                        elif event_name == "GiftedSubscriptionsEvent":
                            self.stats["gift_subscriptions"] += 1

                            user_name = self.safe_get_first(
                                data,
                                ("gifter", "username"),
                                ("user", "username"),
                                ("sender", "username"),
                                default="Unknown"
                            )

                            log_entry.update({
                                "user": user_name,
                                "type": "gift_sub",
                                "raw": data
                            })

                            print(f"[GIFT SUB] Gönderen: {user_name}")

                        else:
                            self.stats["other_events"] += 1
                            log_entry["raw"] = data

                        await self._attach_session_to_entry(log_entry)

            except websockets.exceptions.ConnectionClosed as e:
                set_socket_connected(False)
                print(f"[!] Bağlantı koptu ({repr(e)}). {RECONNECT_DELAY} sn içinde tekrar denenecek...")
                await asyncio.sleep(RECONNECT_DELAY)

            except Exception as e:
                set_socket_connected(False)
                print(f"[!] Hata oluştu ({repr(e)}). {RECONNECT_DELAY} sn içinde tekrar denenecek...")
                await asyncio.sleep(RECONNECT_DELAY)

        print("[*] Socket listener kapandı.")

    def print_summary(self):
        print("\n" + "=" * 40)
        print("OTURUM ÖZETİ")
        print(f"Toplam Mesaj      : {self.stats['chat_messages']}")
        print(f"Silinen Mesaj     : {self.stats['deleted_messages']}")
        print(f"Timeout           : {self.stats['timeouts']}")
        print(f"Kalıcı Ban        : {self.stats['bans']}")
        print(f"Unban             : {self.stats['unbans']}")
        print(f"Subscription      : {self.stats['subscriptions']}")
        print(f"Gift Sub          : {self.stats['gift_subscriptions']}")
        print(f"Diğer Eventler    : {self.stats['other_events']}")
        print(f"Stream Event      : {self.stats['stream_events']}")
        print(f"Offstream Event   : {self.stats['offstream_events']}")
        print("=" * 40)


async def main():
    recorder = KickRecorder()
    writer_task = asyncio.create_task(recorder.file_writer_worker())
    socket_task = asyncio.create_task(recorder.socket_listener())
    watchdog_task = asyncio.create_task(recorder.stream_watchdog())

    try:
        await asyncio.gather(writer_task, socket_task, watchdog_task)
    finally:
        recorder.running = False

        try:
            await asyncio.wait_for(recorder.queue.join(), timeout=5)
        except asyncio.TimeoutError:
            pass

        for task in (writer_task, socket_task, watchdog_task):
            task.cancel()

        recorder.print_summary()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\n[!] Program kullanıcı tarafından durduruldu.")