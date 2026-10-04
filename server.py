import asyncio
import json
from datetime import datetime, timezone
from typing import Optional, Set

from fastapi import FastAPI, Query, WebSocket, WebSocketDisconnect
from fastapi.staticfiles import StaticFiles

from config import CHANNEL_SLUG
from db import connect, get_active_stream, init_db
from recorder import KickRecorder
from stream_manager import get_channel_meta

app = FastAPI(title='Kick Live Analytics', version='2.0.0')
connected_clients: Set[WebSocket] = set()
recent_messages: list[dict] = []
recorder: KickRecorder | None = None
recorder_tasks: list[asyncio.Task] = []


def now_iso():
    return datetime.now(timezone.utc).isoformat()


def row_dict(row):
    return dict(row) if row else None


def build_stream_label(row):
    if not row: return 'Yayın'
    return f"{row['label_date']} offstream" if row['session_type'] == 'offstream' else f"{row['label_date']} yayını"


def push_recent(payload: dict):
    recent_messages.append(payload)
    if len(recent_messages) > 250:
        del recent_messages[:-250]


async def broadcast(payload: dict):
    push_recent(payload)
    if not connected_clients: return
    dead = []
    text = json.dumps(payload, ensure_ascii=False)
    for ws in list(connected_clients):
        try: await ws.send_text(text)
        except Exception: dead.append(ws)
    for ws in dead: connected_clients.discard(ws)


def _event_payload(entry):
    if entry.get('type') == 'chat':
        return {'type':'chat','event':entry.get('e'),'user':entry.get('user'),'user_id':entry.get('user_id'),'msg':entry.get('msg'),'message_id':entry.get('message_id'),'time':entry.get('t'),'stream_id':entry.get('stream_id')}
    return {'type':'other','event':entry.get('e'),'user':entry.get('user'),'data':entry.get('raw',{}),'time':entry.get('t'),'stream_id':entry.get('stream_id')}


async def recorder_event(entry):
    await broadcast(_event_payload(entry))


def stream_ids_for_mode(conn, mode, date=None, month=None, stream_id=None):
    if mode == 'live':
        row = conn.execute("SELECT * FROM streams WHERE status='live' AND session_type='stream' ORDER BY id DESC LIMIT 1").fetchone()
        return [row['id']] if row else [], row
    if mode == 'stream':
        row = conn.execute("SELECT * FROM streams WHERE id=? AND session_type='stream'", (stream_id,)).fetchone() if stream_id else None
        return [row['id']] if row else [], row
    if mode == 'day':
        rows = conn.execute("SELECT * FROM streams WHERE label_date=? AND session_type='stream' ORDER BY id DESC", (date,)).fetchall()
        return [r['id'] for r in rows], rows[0] if rows else None
    if mode == 'offstream_day':
        rows = conn.execute("SELECT * FROM streams WHERE label_date=? AND session_type='offstream' ORDER BY id DESC", (date,)).fetchall()
        return [r['id'] for r in rows], rows[0] if rows else None
    if mode == 'week':
        rows = conn.execute("SELECT * FROM streams WHERE label_date >= date('now','-7 day') ORDER BY id DESC").fetchall()
        return [r['id'] for r in rows], rows[0] if rows else None
    if mode == 'month':
        rows = conn.execute("SELECT * FROM streams WHERE substr(label_date,1,7)=? ORDER BY id DESC", (month,)).fetchall()
        return [r['id'] for r in rows], rows[0] if rows else None
    rows = conn.execute("SELECT * FROM streams ORDER BY id DESC").fetchall()
    return [r['id'] for r in rows], rows[0] if rows else None


def summary_for_stream_ids(conn, ids: list[int], event_limit=1000):
    if not ids:
        empty_stats = {'total_messages':0,'total_msgs':0,'unique_users':0,'total_users':0,'deleted_messages':0,
                       'timeouts':0,'bans':0,'unbans':0,'subscriptions':0,'gift_subscriptions':0,
                       'other_events':0,'peak_chat_per_minute':0,'peak_chat_per_second':0}
        return {'stats': empty_stats, 'users':[], 'words':[], 'emotes':[], 'spam':[],
                'moderation':{'summary':{},'mods':[],'recent_actions':[]}, 'events':[],
                'game_special':{'all_pool':[],'top_10_users':[],'top_10_words':[],'top_10_emotes':[]}}
    ph=','.join('?'*len(ids))
    stats=dict(conn.execute(f"""SELECT COALESCE(SUM(total_messages),0) total_messages,
        COALESCE(SUM(deleted_messages),0) deleted_messages,COALESCE(SUM(timeouts),0) timeouts,
        COALESCE(SUM(bans),0) bans,COALESCE(SUM(unbans),0) unbans,COALESCE(SUM(subscriptions),0) subscriptions,
        COALESCE(SUM(gift_subscriptions),0) gift_subscriptions,COALESCE(SUM(other_events),0) other_events,
        COALESCE(MAX(peak_chat_per_minute),0) peak_chat_per_minute,COALESCE(MAX(peak_chat_per_second),0) peak_chat_per_second
        FROM stream_stats WHERE stream_id IN ({ph})""",ids).fetchone())
    unique=conn.execute(f'SELECT COUNT(DISTINCT user_id) c FROM user_stream_stats WHERE stream_id IN ({ph})',ids).fetchone()['c']
    stats.update({'unique_users':unique,'total_users':unique,'total_msgs':stats.get('total_messages',0)})
    users=[]
    user_rows=conn.execute(f"""SELECT us.user_id id,MAX(us.username) n,SUM(us.messages) mc,
        MIN(us.first_message_at) first_message_at,MAX(us.last_message_at) last_message_at
        FROM user_stream_stats us WHERE us.stream_id IN ({ph}) GROUP BY us.user_id ORDER BY mc DESC LIMIT 500""",ids).fetchall()
    for r in user_rows:
        uid=r['id']; uname=r['n']
        wc=conn.execute(f'SELECT COALESCE(SUM(count),0) c FROM user_word_stats WHERE user_id=? AND stream_id IN ({ph})',[uid]+ids).fetchone()['c']
        ec=conn.execute(f'SELECT COALESCE(SUM(count),0) c FROM user_emote_stats WHERE user_id=? AND stream_id IN ({ph})',[uid]+ids).fetchone()['c']
        received=conn.execute(f"""SELECT SUM(CASE WHEN event_type='timeout' THEN 1 ELSE 0 END) timeouts,
            SUM(CASE WHEN event_type='ban' THEN 1 ELSE 0 END) bans,SUM(CASE WHEN event_type='unban' THEN 1 ELSE 0 END) unbans,
            SUM(CASE WHEN event_type='deleted' THEN 1 ELSE 0 END) deleted_messages FROM events
            WHERE stream_id IN ({ph}) AND LOWER(COALESCE(target_username,''))=LOWER(?)""",ids+[uname]).fetchone()
        users.append({'n':uname,'mc':r['mc'],'id':uid,'first_message_at':r['first_message_at'],'last_message_at':r['last_message_at'],
                      'wc':wc or 0,'ec':ec or 0,'mod_received':{'timeouts':received['timeouts'] or 0,'bans':received['bans'] or 0,'unbans':received['unbans'] or 0,'deleted_messages':received['deleted_messages'] or 0}})
    word_rows=conn.execute(f"""SELECT word w,SUM(count) c FROM word_stats WHERE stream_id IN ({ph})
        AND word NOT LIKE 'emote:%' COLLATE NOCASE AND word NOT LIKE '[emote:%' COLLATE NOCASE
        GROUP BY word ORDER BY c DESC""",ids).fetchall()
    words=[{'w':r['w'],'c':r['c'],'top':[]} for r in word_rows]
    emote_rows=conn.execute(f"SELECT emote_id id,MAX(emote_name) n,SUM(count) c FROM emote_stats WHERE stream_id IN ({ph}) GROUP BY emote_id ORDER BY c DESC",ids).fetchall()
    emotes=[]
    for r in emote_rows:
        ur=conn.execute(f'SELECT COUNT(DISTINCT user_id) c FROM user_emote_stats WHERE stream_id IN ({ph}) AND emote_id=?',ids+[r['id']]).fetchone()['c']
        top_rows=conn.execute(f"""SELECT ue.user_id id,MAX(u.username) n,SUM(ue.count) c FROM user_emote_stats ue LEFT JOIN users u ON u.id=ue.user_id
            WHERE ue.stream_id IN ({ph}) AND ue.emote_id=? GROUP BY ue.user_id ORDER BY c DESC LIMIT 5""",ids+[r['id']]).fetchall()
        last=conn.execute(f"""SELECT MAX(timestamp) t FROM chat_messages WHERE stream_id IN ({ph}) AND (message LIKE ? OR message LIKE ?)""",ids+[f'%[emote:{r["id"]}:%',f'%emote:{r["id"]}:%']).fetchone()['t']
        emotes.append({'id':str(r['id']),'n':r['n'],'c':r['c'],'unique_users':ur or 0,'top':[{'u':x['n'],'c':x['c']} for x in top_rows],'lastUsed':last})
    spam_rows=conn.execute(f"""SELECT message_key key,MAX(message) m,SUM(count) c,MAX(unique_users) unique_users,MAX(last_username) username,
        MAX(last_timestamp) last_t,MAX(usernames_json) usernames_json FROM spam_stats WHERE stream_id IN ({ph}) GROUP BY message_key ORDER BY c DESC LIMIT 500""",ids).fetchall()
    spam=[]
    for r in spam_rows:
        try: names=json.loads(r['usernames_json'] or '[]')
        except Exception: names=[]
        spam.append({'key':r['key'],'m':r['m'],'c':r['c'],'unique_users':r['unique_users'] or 0,'last_t':r['last_t'],'top':[{'u':n,'c':None} for n in names[:10]]})
    event_rows=conn.execute(f"""SELECT id,stream_id,timestamp,event_name,event_type,username,target_username,moderator,message,reason,duration,permanent,session_type,message_id,quantity
        FROM events WHERE stream_id IN ({ph}) ORDER BY id DESC LIMIT ?""",ids+[event_limit]).fetchall()
    events=[dict(r) for r in reversed(event_rows)]
    mod_rows=conn.execute(f"""SELECT COALESCE(NULLIF(moderator,''),'Kick/System') n,
        SUM(CASE WHEN event_type='timeout' THEN 1 ELSE 0 END) timeouts,SUM(CASE WHEN event_type='ban' THEN 1 ELSE 0 END) bans,
        SUM(CASE WHEN event_type='unban' THEN 1 ELSE 0 END) unbans,SUM(CASE WHEN event_type='deleted' THEN 1 ELSE 0 END) deleted_messages,
        COUNT(*) total_actions,MAX(timestamp) last_action_at FROM events WHERE stream_id IN ({ph})
        AND event_type IN ('timeout','ban','unban','deleted') GROUP BY COALESCE(NULLIF(moderator,''),'Kick/System') ORDER BY total_actions DESC""",ids).fetchall()
    mod_list=[]
    for mr in mod_rows:
        mod={'n':mr['n'],'total_actions':mr['total_actions'],'timeouts':mr['timeouts'] or 0,'bans':mr['bans'] or 0,'unbans':mr['unbans'] or 0,'deleted_messages':mr['deleted_messages'] or 0,'last_action_at':mr['last_action_at'],'top_targets':[],'top_reasons':[],'logs':[]}
        target_rows=conn.execute(f"""SELECT COALESCE(target_username,'Bilinmiyor') n,COUNT(*) c FROM events WHERE stream_id IN ({ph})
            AND event_type IN ('timeout','ban','unban','deleted') AND COALESCE(NULLIF(moderator,''),'Kick/System')=? GROUP BY target_username ORDER BY c DESC LIMIT 10""",ids+[mr['n']]).fetchall()
        reason_rows=conn.execute(f"""SELECT COALESCE(reason,'Sebep belirtilmemiş') r,COUNT(*) c FROM events WHERE stream_id IN ({ph})
            AND event_type IN ('timeout','ban','unban','deleted') AND COALESCE(NULLIF(moderator,''),'Kick/System')=? GROUP BY reason ORDER BY c DESC LIMIT 10""",ids+[mr['n']]).fetchall()
        logs=conn.execute(f"""SELECT timestamp t,event_type action,target_username target,reason,duration,message msg,permanent,quantity FROM events WHERE stream_id IN ({ph})
            AND event_type IN ('timeout','ban','unban','deleted') AND COALESCE(NULLIF(moderator,''),'Kick/System')=? ORDER BY id DESC LIMIT 200""",ids+[mr['n']]).fetchall()
        mod['top_targets']=[dict(x) for x in target_rows]; mod['top_reasons']=[dict(x) for x in reason_rows]; mod['logs']=[dict(x) for x in reversed(logs)]
        mod_list.append(mod)
    moderation_summary={'total_actions':sum(m['total_actions'] for m in mod_list),'timeouts':sum(m['timeouts'] for m in mod_list),'bans':sum(m['bans'] for m in mod_list),'unbans':sum(m['unbans'] for m in mod_list),'deleted_messages':sum(m['deleted_messages'] for m in mod_list)}
    recent_mod=[dict(r) for r in conn.execute(f"""SELECT id,stream_id,timestamp,event_name,event_type,username,target_username,moderator,message,reason,duration,permanent,session_type,message_id,quantity
        FROM events WHERE stream_id IN ({ph}) AND event_type IN ('ban','timeout','unban','deleted') ORDER BY id DESC LIMIT 300""",ids).fetchall()][::-1]
    return {'stats':stats,'users':users,'words':words,'emotes':emotes,'spam':spam,'moderation':{'summary':moderation_summary,'mods':mod_list,'recent_actions':recent_mod},'events':events,
            'game_special':{'all_pool':words,'top_10_users':users[:10],'top_10_words':words[:10],'top_10_emotes':emotes[:10]}}


@app.get('/health')
async def health():
    active = get_active_stream()
    return {'ok':True,'channel':CHANNEL_SLUG,'live':bool(active),'db':'ok','recorder_running':bool(recorder and recorder.running)}


@app.get('/api/status')
async def api_status():
    active = get_active_stream()
    meta = await asyncio.to_thread(get_channel_meta)
    return {'ok':True,'channel':meta or {'slug':CHANNEL_SLUG},'live':bool(meta and meta.get('is_live')),'active_stream':row_dict(active),'recorder_stats':recorder.stats if recorder else {}}


@app.get('/api/streams')
async def api_streams(limit: int = Query(30, ge=1, le=200)):
    conn = connect()
    try:
        rows = conn.execute("SELECT * FROM streams WHERE session_type='stream' AND status='ended' ORDER BY id DESC LIMIT ?", (limit,)).fetchall()
        return {'ok':True,'streams':[row_dict(r) for r in rows]}
    finally: conn.close()


@app.get('/api/streams/active')
async def api_active_stream():
    row = get_active_stream()
    return {'ok':True,'active':row_dict(row)}


@app.get('/api/streams/{stream_id}/events')
async def api_stream_events(stream_id:int, limit:int=Query(500,ge=1,le=5000), before_id:Optional[int]=None):
    conn=connect()
    try:
        if before_id:
            rows=conn.execute('SELECT * FROM events WHERE stream_id=? AND id<? ORDER BY id DESC LIMIT ?', (stream_id,before_id,limit)).fetchall()
        else:
            rows=conn.execute('SELECT * FROM events WHERE stream_id=? ORDER BY id DESC LIMIT ?', (stream_id,limit)).fetchall()
        return {'ok':True,'events':[dict(r) for r in reversed(rows)]}
    finally: conn.close()


@app.get('/api/user_messages')
async def api_user_messages(username:str, stream_id:Optional[int]=None, limit:int=Query(100,ge=1,le=1000)):
    conn=connect()
    try:
        if stream_id:
            rows=conn.execute('SELECT * FROM chat_messages WHERE username=? AND stream_id=? ORDER BY id DESC LIMIT ?', (username,stream_id,limit)).fetchall()
        else:
            rows=conn.execute('SELECT * FROM chat_messages WHERE username=? ORDER BY id DESC LIMIT ?', (username,limit)).fetchall()
        return {'ok':True,'messages':[dict(r) for r in reversed(rows)]}
    finally: conn.close()


@app.get('/api/user_detail')
async def api_user_detail(username: str, stream_id: Optional[int] = None, limit: int = Query(100, ge=1, le=500)):
    conn = connect()
    try:
        params=[username]
        where='cm.username=?'
        if stream_id:
            where += ' AND cm.stream_id=?'; params.append(stream_id)
        user = conn.execute('SELECT * FROM users WHERE username=? COLLATE NOCASE LIMIT 1',(username,)).fetchone()
        messages = conn.execute(f'''SELECT cm.timestamp,cm.message,cm.stream_id,cm.message_id FROM chat_messages cm WHERE {where} ORDER BY cm.id DESC LIMIT ?''', params+[limit]).fetchall()
        uid = user['id'] if user else None
        words=[]; emotes=[]
        if uid:
            wwhere='stream_id IN (SELECT id FROM streams)'
            wparams=[]
            if stream_id:
                wwhere='stream_id=?'; wparams=[stream_id]
            words=[dict(r) for r in conn.execute(f'''SELECT word,count FROM user_word_stats WHERE user_id=? AND {wwhere} AND word NOT LIKE 'emote:%' COLLATE NOCASE AND word NOT LIKE '[emote:%' COLLATE NOCASE ORDER BY count DESC LIMIT 20''',[uid]+wparams).fetchall()]
            emotes=[dict(r) for r in conn.execute(f'''SELECT emote_id id,emote_name n,count c FROM user_emote_stats WHERE user_id=? AND {wwhere} ORDER BY count DESC LIMIT 20''',[uid]+wparams).fetchall()]
        history=[]
        mod_received={'timeouts':0,'bans':0,'unbans':0,'deleted_messages':0}
        if user:
            uname=user['username']
            rows=conn.execute('''SELECT timestamp,event_type,moderator,target_username,reason,duration,permanent,message,quantity FROM events WHERE LOWER(COALESCE(target_username,''))=LOWER(?) ORDER BY id DESC LIMIT 100''',(uname,)).fetchall()
            for r in rows:
                d=dict(r); history.append(d)
                if d['event_type'] in mod_received: mod_received[d['event_type']]+=1
                elif d['event_type']=='deleted': mod_received['deleted_messages']+=1
        return {'ok':True,'user':dict(user) if user else None,'words':words,'emotes':emotes,'messages':[dict(r) for r in reversed(messages)],'mod_received':mod_received,'mod_history_received':list(reversed(history))}
    finally:
        conn.close()


@app.get('/api/word_detail')
async def api_word_detail(word: str, stream_id: Optional[int] = None, limit: int = Query(10, ge=1, le=50)):
    conn=connect()
    try:
        ids=[stream_id] if stream_id else ([get_active_stream()['id']] if get_active_stream() else [])
        if not ids: return {'ok':True,'word':word,'count':0,'unique_users':0,'top_users':[]}
        ph=','.join('?'*len(ids))
        row=conn.execute(f'SELECT COALESCE(SUM(count),0) c FROM word_stats WHERE stream_id IN ({ph}) AND word=? COLLATE NOCASE',ids+[word]).fetchone()
        top=conn.execute(f"""SELECT uw.user_id id,MAX(u.username) name,SUM(uw.count) count FROM user_word_stats uw LEFT JOIN users u ON u.id=uw.user_id
            WHERE uw.stream_id IN ({ph}) AND uw.word=? COLLATE NOCASE GROUP BY uw.user_id ORDER BY count DESC LIMIT ?""",ids+[word,limit]).fetchall()
        unique=conn.execute(f'SELECT COUNT(DISTINCT user_id) c FROM user_word_stats WHERE stream_id IN ({ph}) AND word=? COLLATE NOCASE',ids+[word]).fetchone()['c']
        return {'ok':True,'word':word,'count':row['c'] if row else 0,'unique_users':unique or 0,'top_users':[{'id':r['id'],'name':r['name'] or 'Unknown','count':r['count']} for r in top]}
    finally: conn.close()

@app.get('/api/emotes')
async def api_emotes(stream_id: Optional[int] = None):
    conn=connect()
    try:
        ids=[stream_id] if stream_id else ([get_active_stream()['id']] if get_active_stream() else [])
        if not ids: return {'ok':True,'emotes':[]}
        ph=','.join('?'*len(ids)); rows=conn.execute(f'SELECT emote_id id,MAX(emote_name) n,SUM(count) c FROM emote_stats WHERE stream_id IN ({ph}) GROUP BY emote_id ORDER BY c DESC',ids).fetchall()
        out=[]
        for r in rows:
            unique=conn.execute(f'SELECT COUNT(DISTINCT user_id) c FROM user_emote_stats WHERE stream_id IN ({ph}) AND emote_id=?',ids+[r['id']]).fetchone()['c']
            top=conn.execute(f'SELECT MAX(u.username) u,SUM(ue.count) c FROM user_emote_stats ue LEFT JOIN users u ON u.id=ue.user_id WHERE ue.stream_id IN ({ph}) AND ue.emote_id=? GROUP BY ue.user_id ORDER BY c DESC LIMIT 5',ids+[r['id']]).fetchall()
            out.append({'id':str(r['id']),'n':r['n'],'c':r['c'],'unique_users':unique or 0,'top':[{'u':x['u'],'c':x['c']} for x in top]})
        return {'ok':True,'emotes':out}
    finally: conn.close()


@app.get('/api/data')
async def api_data(mode:str='live', date:Optional[str]=None, month:Optional[str]=None, stream_id:Optional[int]=None):
    conn=connect()
    try:
        ids, selected = stream_ids_for_mode(conn, mode, date, month, stream_id)
        meta={'label':'Canlı yayın' if mode=='live' else mode,'live_stream_active':False,'session_type':'stream'}
        if selected: meta.update({'stream_count':len(ids),'stream_ids':ids,'session_type':selected['session_type']})
        if mode=='live' and not selected:
            meta['warning']='Canlı yayın kapalı'
            empty=summary_for_stream_ids(conn,[])
            return {'ok':True,'mode':mode,'stream':None,'meta':meta,'summary':empty}
        summary=summary_for_stream_ids(conn,ids)
        if mode=='live': meta['live_stream_active']=True
        if mode=='stream' and not selected: return {'ok':False,'error':'stream_not_found'}
        stream=row_dict(selected)
        if stream: stream['display_label']=build_stream_label(selected)
        # viewer timeline only for selected stream, bounded.
        if selected:
            snaps=conn.execute('SELECT captured_at,viewer_count FROM stream_snapshots WHERE stream_id=? ORDER BY id DESC LIMIT 360', (selected['id'],)).fetchall()
            meta['viewer_history']=[dict(r) for r in reversed(snaps)]
        return {'ok':True,'mode':mode,'stream':stream,'meta':meta,'summary':summary}
    finally: conn.close()


@app.websocket('/ws')
async def websocket_endpoint(websocket:WebSocket):
    await websocket.accept(); connected_clients.add(websocket)
    try:
        await websocket.send_text(json.dumps({'type':'bootstrap','time':now_iso(),'state':{'channel':CHANNEL_SLUG},'recent':recent_messages},ensure_ascii=False))
        while True: await websocket.receive_text()
    except (WebSocketDisconnect, Exception):
        connected_clients.discard(websocket)


@app.on_event('startup')
async def startup_event():
    global recorder, recorder_tasks
    init_db()
    recorder=KickRecorder()
    recorder.on_event = recorder_event
    # Recorder's socket + DB writer + live monitor all live in the same process as FastAPI.
    recorder_tasks=[asyncio.create_task(recorder.writer_worker()), asyncio.create_task(recorder.socket_listener()), asyncio.create_task(recorder._monitor_task())]


@app.on_event('shutdown')
async def shutdown_event():
    if recorder:
        recorder.running=False
        recorder.stop_event.set()
        await recorder.queue.join()
    for task in recorder_tasks:
        if not task.done(): task.cancel()


app.mount('/', StaticFiles(directory='static', html=True), name='static')
