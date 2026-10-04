import asyncio
import json
import re
from datetime import datetime, timezone

import websockets

from config import (
    CHANNEL_SLUG, DB_BATCH_SIZE, DB_BATCH_WAIT_MS, LOGS_DIR,
    MAX_JSONL_MB, PRINT_CHAT, PUSHER_WS_URL, RECONNECT_DELAY_SECONDS,
    RAW_JSON_IN_DB, LOG_OFFSTREAM_CHAT,
)
from db import BatchWriter, connect, get_active_stream, init_db
from kick_api import KickAPI
from stream_manager import ensure_live_stream, get_channel_meta, get_chatroom_id


class KickRecorder:
    def __init__(self):
        self.running = True
        self.queue: asyncio.Queue[dict] = asyncio.Queue(maxsize=10000)
        self.recent_message_ids: dict[str, float] = {}
        self.last_user_message: dict[str, str] = {}
        self.file_index: dict[int, int] = {}
        self.stats = {'received': 0, 'chat': 0, 'stored': 0, 'duplicates': 0, 'errors': 0}
        self.stop_event = asyncio.Event()
        self.on_event = None

    @staticmethod
    def parse_data(raw):
        data = raw.get('data')
        if isinstance(data, str):
            try: return json.loads(data)
            except json.JSONDecodeError: return None
        return data if isinstance(data, dict) else None

    @staticmethod
    def event_name(event: str) -> str:
        return (event or '').split('\\')[-1].split('.')[-1] or 'UnknownEvent'

    @staticmethod
    def first(obj, *paths, default=None):
        for path in paths:
            cur = obj
            ok = True
            for key in path:
                if isinstance(cur, dict) and key in cur:
                    cur = cur[key]
                else:
                    ok = False; break
            if ok and cur not in (None, ''):
                return cur
        return default

    @staticmethod
    def words(text: str):
        # Emote codes are visual tokens, not natural-language words.
        # Keep normal words only; strip URLs and punctuation without destroying Unicode.
        out = []
        for token in re.findall(r"[^\s]+", text or ''):
            if re.fullmatch(r"\[?emote:\d+:[^\]\s]+\]?", token, flags=re.I):
                continue
            if token.startswith(('http://','https://')):
                continue
            token = token.strip('.,!?;:()[]{}<>\"\'').lower()
            if len(token) >= 2 and any(c.isalnum() for c in token):
                out.append(token[:80])
        return out

    @staticmethod
    def emotes(text: str):
        return [{'id': m.group(1), 'name': m.group(2)} for m in re.finditer(r'\[?emote:(\d+):([^\]\s]+)\]?', text or '', flags=re.I)]

    def session(self):
        active = get_active_stream()
        if active:
            return active['id'], active['label_date'], 'stream'
        return None, datetime.now(timezone.utc).strftime('%Y-%m-%d'), 'stream'

    def log_path(self, sid, label_date, session_type):
        idx = self.file_index.get(sid, 1)
        name = f'kick_{CHANNEL_SLUG}_{label_date}_{session_type}{sid}_part{idx}.jsonl'
        return LOGS_DIR / name

    def rotate_if_needed(self, sid, label_date, session_type):
        path = self.log_path(sid, label_date, session_type)
        if path.exists() and path.stat().st_size >= MAX_JSONL_MB * 1024 * 1024:
            self.file_index[sid] = self.file_index.get(sid, 1) + 1

    def normalize_event(self, data, event_name, timestamp=None):
        timestamp = timestamp or datetime.now(timezone.utc).isoformat()
        sender = data.get('sender') or data.get('user') or data.get('subscriber') or data.get('gifter') or {}
        username = self.first(data, ('sender','username'), ('user','username'), ('subscriber','username'), ('gifter','username'), ('gifter_username',))
        user_id = self.first(data, ('sender','user_id'), ('sender','id'), ('user','id'), ('user','user_id'), ('subscriber','user_id'), ('gifter','user_id'))
        profile = self.first(data, ('sender','profile_picture'), ('user','profile_picture'), ('subscriber','profile_picture'), ('gifter','profile_picture'))
        entry = {
            't': timestamp, 'e': event_name, 'type': 'other', 'user': username,
            'user_id': user_id, 'display_name': sender.get('username') if isinstance(sender, dict) else username,
            'profile_picture': profile, 'keep_raw': RAW_JSON_IN_DB, 'raw': data,
        }
        name = (event_name or '').lower()

        if 'chatmessageevent' in name or name in ('chatmessage', 'message'):
            message_obj = data.get('message') if isinstance(data.get('message'), dict) else {}
            content = data.get('content') or message_obj.get('content') or ''
            if not isinstance(content, str):
                content = str(content or '')
            mid = data.get('message_id') or data.get('chatId') or data.get('id') or message_obj.get('id')
            reply = data.get('replies_to') or {}

            # Kick currently sends emotes as {emote_id, positions}; the name is still
            # present in the [emote:id:name] text, so merge both sources.
            text_emotes = self.emotes(content)
            names_by_id = {str(x['id']): x['name'] for x in text_emotes}
            payload_emotes = data.get('emotes') if isinstance(data.get('emotes'), list) else []
            norm_ems = []
            seen = set()
            for em in payload_emotes:
                if not isinstance(em, dict):
                    continue
                eid = em.get('id') or em.get('emote_id')
                if eid is None:
                    continue
                eid = str(eid)
                ename = names_by_id.get(eid) or em.get('name') or em.get('emote_name') or f'emote{eid}'
                # positions can contain multiple occurrences of the same emote.
                positions = em.get('positions') or [None]
                for _ in positions:
                    key = (eid, str(ename), len(norm_ems))
                    norm_ems.append({'id': eid, 'name': str(ename)})
            if not norm_ems:
                norm_ems = text_emotes

            entry.update({
                'type': 'chat', 'msg': content, 'message_id': mid,
                'reply_to_message_id': reply.get('message_id') if isinstance(reply, dict) else None,
                'emotes': norm_ems, 'words': self.words(content)
            })
            if username and content:
                key = str(username).lower()
                state = self.last_user_message.get(key) or {'msg': None, 'repeat': 0}
                if state.get('msg') == content:
                    state['repeat'] = int(state.get('repeat', 1)) + 1
                else:
                    state = {'msg': content, 'repeat': 1}
                self.last_user_message[key] = state
                if state['repeat'] >= 3:
                    entry['spam_key'] = content.strip().lower()[:500]
                    entry['spam_repeat_count'] = state['repeat']
            return entry

        if 'messagedeletedevent' in name or 'chatmessagedeleted' in name or 'messagedeleted' in name:
            msg=data.get('message') or data.get('deleted_message') or {}
            mod=data.get('deleted_by') or data.get('moderator') or data.get('mod_user') or data.get('actor') or data.get('user') or {}
            target=msg.get('sender') if isinstance(msg,dict) else {}
            entry.update({'type':'deleted','msg':msg.get('content') if isinstance(msg,dict) else data.get('content'),
                          'message_id':(msg.get('id') if isinstance(msg,dict) else None) or data.get('message_id') or data.get('id'),
                          'target_user':target.get('username') if isinstance(target,dict) else self.first(data,('target_user','username')),
                          'mod':mod.get('username') if isinstance(mod,dict) else mod or 'Kick/System'})
            return entry

        if 'userbannedevent' in name or 'moderationbanned' in name or 'timeout' in name or name.endswith('banned'):
            target=data.get('banned_user') or data.get('target_user') or data.get('user') or {}
            mod=data.get('moderator') or data.get('banned_by') or data.get('banned_by_user') or data.get('mod_user') or data.get('actor') or {}
            md=data.get('metadata') or {}
            expires=md.get('expires_at') or data.get('expires_at')
            duration=data.get('duration') or data.get('duration_seconds')
            if duration is None and expires and md.get('created_at'):
                try: duration=max(0,int((datetime.fromisoformat(expires.replace('Z','+00:00'))-datetime.fromisoformat(md['created_at'].replace('Z','+00:00'))).total_seconds()))
                except Exception: pass
            permanent=(expires in (None, '', 'null') and duration in (None, '', 0, '0'))
            entry.update({'type':'ban' if permanent else 'timeout',
                          'target_user':target.get('username') if isinstance(target,dict) else target,
                          'user':target.get('username') if isinstance(target,dict) else target,
                          'mod':mod.get('username') if isinstance(mod,dict) else mod,
                          'reason':md.get('reason') or data.get('reason'), 'duration':duration,
                          'permanent':1 if permanent else 0})
            return entry

        if 'userunbannedevent' in name or 'moderationunbanned' in name or 'unban' in name or 'unmute' in name:
            target=data.get('unbanned_user') or data.get('user') or data.get('target_user') or {}
            mod=data.get('unbanned_by') or data.get('moderator') or data.get('unbanned_by_user') or data.get('mod_user') or data.get('actor') or {}
            entry.update({'type':'unban','target_user':target.get('username') if isinstance(target,dict) else target,
                          'mod':mod.get('username') if isinstance(mod,dict) else mod})
            return entry

        if ('subscriptionevent' in name or 'subscriptioncreatedevent' in name or 'channelsubscriptionevent' in name
                or name in ('subscription', 'newsubscription', 'subscriptioncreated', 'channelsubscription')):
            sub = data.get('subscriber') or data.get('user') or data.get('sender') or {}
            sub_name = sub.get('username') if isinstance(sub, dict) else sub
            entry.update({'type':'subscription','user':sub_name or username,'duration':data.get('months') or data.get('duration') or data.get('duration_months')})
            return entry
        if ('giftedsubscriptions' in name or 'subscriptionsgift' in name or 'subscriptiongift' in name
                or name in ('giftedsubscriptionsevent','giftsubscriptionsevent','channelsubscriptiongifts')):
            gifter = data.get('gifter') or data.get('user') or data.get('sender') or {}
            gifter_name = gifter.get('username') if isinstance(gifter, dict) else gifter
            giftees = data.get('giftees') or data.get('gifted_usernames') or data.get('recipients') or []
            gift_count = len(giftees) if isinstance(giftees, list) else int(data.get('gift_count') or data.get('quantity') or 1)
            entry.update({'type':'gift_sub','gift_count':max(1,gift_count), 'quantity':max(1,gift_count), 'user':gifter_name or username, 'gifter':gifter_name or username})
            return entry
        if 'followevent' in name or 'followed' in name:
            entry['type']='follow'; return entry
        if 'streamhostevent' in name:
            entry['type']='host'; return entry

        # Defensive normalization for Kick/Pusher names that may change while payload shapes stay stable.
        if data.get('giftees') is not None or data.get('gifter') is not None:
            gifter = data.get('gifter') or {}
            giftees = data.get('giftees') or []
            entry.update({'type':'gift_sub','user':gifter.get('username') if isinstance(gifter,dict) else gifter, 'gift_count':len(giftees) if isinstance(giftees,list) else 1, 'quantity':len(giftees) if isinstance(giftees,list) else 1})
            return entry
        if data.get('subscriber') is not None and ('sub' in name or 'subscription' in name):
            sub = data.get('subscriber') or {}
            entry.update({'type':'subscription','user':sub.get('username') if isinstance(sub,dict) else sub, 'duration':data.get('duration')})
            return entry
        return entry

    async def enqueue(self, entry):
        sid, date_label, session_type = self.session()
        if sid is None:
            meta = await asyncio.to_thread(get_channel_meta)
            if meta and meta.get('is_live'):
                sid = await asyncio.to_thread(ensure_live_stream, meta)
                if sid:
                    session_type = 'stream'
        if sid is None:
            # v5 stores only actual livestream sessions. Chatroom traffic while offline is ignored.
            return
        entry['stream_id'] = sid
        entry['stream_label_date'] = date_label
        entry['session_type'] = session_type
        await self.queue.put(entry)
        if self.on_event:
            try:
                result = self.on_event(entry)
                if asyncio.iscoroutine(result):
                    await result
            except Exception:
                pass

    async def writer_worker(self):
        conn = connect()
        writer = BatchWriter(conn)
        batch = []
        print(f'[DB] Batch writer hazır. batch={DB_BATCH_SIZE}')
        try:
            while self.running or not self.queue.empty():
                try:
                    item = await asyncio.wait_for(self.queue.get(), timeout=DB_BATCH_WAIT_MS / 1000)
                    batch.append(item)
                except asyncio.TimeoutError:
                    pass
                if not batch:
                    continue
                if len(batch) < DB_BATCH_SIZE and not self.queue.empty():
                    continue
                try:
                    for e in batch:
                        self.rotate_if_needed(e['stream_id'], e['stream_label_date'], e['session_type'])
                        path = self.log_path(e['stream_id'], e['stream_label_date'], e['session_type'])
                        with path.open('a', encoding='utf-8') as f:
                            f.write(json.dumps(e, ensure_ascii=False, separators=(',', ':')) + '\n')
                    writer.write_batch(batch)
                    self.stats['stored'] += len(batch)
                    for _ in batch: self.queue.task_done()
                    batch.clear()
                except Exception as e:
                    self.stats['errors'] += 1
                    print(f'[DB] batch yazma hatası: {e}')
                    await asyncio.sleep(1)
        finally:
            if batch:
                writer.write_batch(batch)
            conn.close()
            print('[DB] Writer kapandı.')


    async def _monitor_task(self):
        from stream_manager import monitor_once
        while self.running:
            try:
                await asyncio.to_thread(monitor_once)
            except Exception as e:
                print(f'[STREAM MONITOR] {e}')
            try:
                await asyncio.wait_for(self.stop_event.wait(), timeout=15)
            except asyncio.TimeoutError:
                pass

    async def socket_listener(self):
        if not CHANNEL_SLUG:
            raise RuntimeError('KICK_CHANNEL_SLUG .env içinde ayarlanmalı.')
        while self.running:
            try:
                meta = await asyncio.to_thread(get_channel_meta)
                chatroom_id = (meta or {}).get('chatroom_id') or await asyncio.to_thread(get_chatroom_id)
                if not chatroom_id:
                    print('[KICK] Chatroom ID bulunamadı. 10 sn sonra tekrar deniyorum.')
                    await asyncio.sleep(10); continue
                print(f'[KICK] Pusher bağlanıyor: {CHANNEL_SLUG} chatroom={chatroom_id}')
                async with websockets.connect(PUSHER_WS_URL, ping_interval=25, ping_timeout=25, close_timeout=10, max_size=4 * 1024 * 1024) as ws:
                    await ws.send(json.dumps({'event':'pusher:subscribe','data':{'auth':'','channel':f'chatrooms.{chatroom_id}.v2'}}))
                    print('[KICK] Chat subscription gönderildi.')
                    async for message in ws:
                        if not self.running: break
                        self.stats['received'] += 1
                        try: raw = json.loads(message)
                        except Exception: continue
                        event = raw.get('event','')
                        if event == 'pusher:ping':
                            await ws.send(json.dumps({'event':'pusher:pong','data':{}})); continue
                        if event in {'pusher:pong','pusher:connection_established','pusher_internal:subscription_succeeded','pusher:subscription_succeeded'}:
                            continue
                        data = self.parse_data(raw)
                        if data is None: continue
                        name = self.event_name(event)
                        entry = self.normalize_event(data, name)
                        if entry.get('type') == 'chat':
                            self.stats['chat'] += 1
                            if PRINT_CHAT: print(f"[CHAT] {entry.get('user')}: {entry.get('msg')}")
                        await self.enqueue(entry)
            except asyncio.CancelledError:
                raise
            except Exception as e:
                print(f'[KICK] bağlantı hatası: {e}')
                await asyncio.sleep(RECONNECT_DELAY_SECONDS)

    async def run(self):
        init_db()
        tasks = [
            asyncio.create_task(self.writer_worker()),
            asyncio.create_task(self.socket_listener()),
            asyncio.create_task(monitor_loop(self.stop_event)),
        ]
        try:
            await asyncio.gather(*tasks)
        except (KeyboardInterrupt, asyncio.CancelledError):
            pass
        finally:
            self.running = False
            self.stop_event.set()
            await self.queue.join()
            for t in tasks:
                if not t.done(): t.cancel()


async def main():
    recorder = KickRecorder()
    await recorder.run()


if __name__ == '__main__':
    asyncio.run(main())
