import sqlite3
import json
import re
import sys
from pathlib import Path
from datetime import datetime

# Ayarlar
DB_PATH = "data/kick_live.db"
SUMMARIES_DIR = Path("static/summaries")
SUMMARIES_DIR.mkdir(parents=True, exist_ok=True)

def normalize_word(word: str) -> str:
    return "".join(ch for ch in (word or "").lower().strip() if ch.isalnum())

def process_day(date_str):
    print(f"[{date_str}] için arka plan analizi başlatılıyor...")
    
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    
    # O güne ait stream ID'lerini bul
    streams = conn.execute("SELECT id FROM streams WHERE label_date = ? AND session_type = 'stream'", (date_str,)).fetchall()
    stream_ids = [r["id"] for r in streams]
    
    if not stream_ids:
        print(f"[{date_str}] için kayıtlı yayın bulunamadı.")
        conn.close()
        return
        
    placeholders = ",".join("?" for _ in stream_ids)
    events = conn.execute(f"""
        SELECT * FROM events 
        WHERE stream_id IN ({placeholders}) 
        ORDER BY id ASC
    """, stream_ids).fetchall()

    print(f"[{date_str}] Toplam {len(events)} olay işleniyor...")

    stats = {
        "total_messages": 0, "unique_users": 0, "deleted_messages": 0,
        "timeouts": 0, "bans": 0, "unbans": 0, "subscriptions": 0,
        "gift_subscriptions": 0, "other_events": 0,
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

        if username and username not in users_map:
            users_map[username] = {
                "n": username, "mc": 0,
                "mod_received": {"timeouts": 0, "bans": 0, "unbans": 0, "deleted_messages": 0},
                "mod_history_received": []
            }

        if event_type == "chat":
            stats["total_messages"] += 1
            if username:
                users_map[username]["mc"] += 1

            if message:
                # Kelimeler
                for raw_word in str(message).split():
                    word = normalize_word(raw_word)
                    if not word: continue
                    if word not in words_map:
                        words_map[word] = {"w": word, "c": 0, "top_user_map": {}}
                    words_map[word]["c"] += 1
                    if username:
                        words_map[word]["top_user_map"][username] = words_map[word]["top_user_map"].get(username, 0) + 1

                # Emotelar
                emote_matches = re.findall(r"\[emote:(\d+):([^\]]+)\]", str(message))
                for emote_id, emote_name in emote_matches:
                    key = f"{emote_id}:{emote_name}"
                    if key not in emotes_map:
                        emotes_map[key] = {"id": emote_id, "n": emote_name, "c": 0, "unique_users_set": set(), "top_user_map": {}}
                    emotes_map[key]["c"] += 1
                    if username:
                        emotes_map[key]["unique_users_set"].add(username)
                        emotes_map[key]["top_user_map"][username] = emotes_map[key]["top_user_map"].get(username, 0) + 1

                # Spam
                if username:
                    prev = user_message_cache.get(username)
                    if prev and prev == message:
                        key = str(message).strip().lower()
                        if key:
                            if key not in spam_map:
                                spam_map[key] = {"key": key, "m": message, "c": 0, "top_user_map": {}}
                            spam_map[key]["c"] += 1
                            spam_map[key]["top_user_map"][username] = spam_map[key]["top_user_map"].get(username, 0) + 1
                    user_message_cache[username] = message

        elif event_type == "deleted":
            stats["deleted_messages"] += 1
            if target_username and target_username not in users_map:
                users_map[target_username] = {
                    "n": target_username, "mc": 0,
                    "mod_received": {"timeouts": 0, "bans": 0, "unbans": 0, "deleted_messages": 0},
                    "mod_history_received": []
                }
            if target_username:
                users_map[target_username]["mod_received"]["deleted_messages"] += 1
                users_map[target_username]["mod_history_received"].append({
                    "action": "deleted", "mod": moderator, "reason": reason, "duration": duration, "msg": message, "t": timestamp
                })
            moderation_actions.append({
                "action": "deleted", "mod": moderator, "target": target_username, "reason": reason, "duration": duration, "msg": message, "t": timestamp
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
                    "n": target_username, "mc": 0,
                    "mod_received": {"timeouts": 0, "bans": 0, "unbans": 0, "deleted_messages": 0},
                    "mod_history_received": []
                }
            if target_username:
                if mod_action == "ban": users_map[target_username]["mod_received"]["bans"] += 1
                else: users_map[target_username]["mod_received"]["timeouts"] += 1
                users_map[target_username]["mod_history_received"].append({
                    "action": mod_action, "mod": moderator, "reason": reason, "duration": duration, "msg": message, "t": timestamp
                })
            moderation_actions.append({
                "action": mod_action, "mod": moderator, "target": target_username, "reason": reason, "duration": duration, "msg": message, "t": timestamp
            })

        elif event_type == "unban":
            stats["unbans"] += 1
            if target_username and target_username not in users_map:
                users_map[target_username] = {
                    "n": target_username, "mc": 0,
                    "mod_received": {"timeouts": 0, "bans": 0, "unbans": 0, "deleted_messages": 0},
                    "mod_history_received": []
                }
            if target_username:
                users_map[target_username]["mod_received"]["unbans"] += 1
                users_map[target_username]["mod_history_received"].append({
                    "action": "unban", "mod": moderator, "reason": reason, "duration": duration, "msg": message, "t": timestamp
                })
            moderation_actions.append({
                "action": "unban", "mod": moderator, "target": target_username, "reason": reason, "duration": duration, "msg": message, "t": timestamp
            })

        elif event_type == "subscription":
            stats["subscriptions"] += 1
        elif event_type == "gift_sub":
            stats["gift_subscriptions"] += 1
        else:
            stats["other_events"] += 1

        events_out.append({
            "id": row["id"], "stream_id": row["stream_id"], "timestamp": row["timestamp"], "event_name": row["event_name"],
            "event_type": row["event_type"], "username": row["username"], "target_username": row["target_username"],
            "moderator": row["moderator"], "message": row["message"], "reason": row["reason"], "duration": row["duration"],
            "permanent": row["permanent"], "session_type": session_type,
        })

    stats["unique_users"] = len(users_map)
    users = sorted(users_map.values(), key=lambda x: x["mc"], reverse=True)

    words = []
    for word_data in words_map.values():
        top = sorted([{"u": u, "c": c} for u, c in word_data["top_user_map"].items()], key=lambda x: x["c"], reverse=True)[:5]
        words.append({"w": word_data["w"], "c": word_data["c"], "top": top})
    words.sort(key=lambda x: x["c"], reverse=True)

    emotes = []
    for emote_data in emotes_map.values():
        top = sorted([{"u": u, "c": c} for u, c in emote_data["top_user_map"].items()], key=lambda x: x["c"], reverse=True)[:5]
        emotes.append({"id": emote_data["id"], "n": emote_data["n"], "c": emote_data["c"], "unique_users": len(emote_data["unique_users_set"]), "top": top})
    emotes.sort(key=lambda x: x["c"], reverse=True)

    spam = []
    for spam_data in spam_map.values():
        if spam_data["c"] < 2: continue
        top = sorted([{"u": u, "c": c} for u, c in spam_data["top_user_map"].items()], key=lambda x: x["c"], reverse=True)[:10]
        spam.append({"key": spam_data["key"], "m": spam_data["m"], "c": spam_data["c"], "top": top})
    spam.sort(key=lambda x: x["c"], reverse=True)

    mods_map = {}
    for action in moderation_actions:
        mod_name = action["mod"] or "Unknown Mod"
        if mod_name not in mods_map:
            mods_map[mod_name] = {"n": mod_name, "total_actions": 0, "timeouts": 0, "bans": 0, "unbans": 0, "deleted_messages": 0, "logs": [], "top_targets_map": {}, "top_reasons_map": {}, "last_action_at": None}
        mod = mods_map[mod_name]
        mod["total_actions"] += 1

        if action["action"] == "timeout": mod["timeouts"] += 1
        elif action["action"] == "ban": mod["bans"] += 1
        elif action["action"] == "unban": mod["unbans"] += 1
        elif action["action"] == "deleted": mod["deleted_messages"] += 1

        if action["target"]: mod["top_targets_map"][action["target"]] = mod["top_targets_map"].get(action["target"], 0) + 1
        if action["reason"]: mod["top_reasons_map"][action["reason"]] = mod["top_reasons_map"].get(action["reason"], 0) + 1

        mod["logs"].append(action)
        if not mod["last_action_at"] or action["t"] > mod["last_action_at"]:
            mod["last_action_at"] = action["t"]

    mods = []
    for mod in mods_map.values():
        mod["top_targets"] = sorted([{"n": k, "c": v} for k, v in mod["top_targets_map"].items()], key=lambda x: x["c"], reverse=True)[:10]
        mod["top_reasons"] = sorted([{"r": k, "c": v} for k, v in mod["top_reasons_map"].items()], key=lambda x: x["c"], reverse=True)[:10]
        mod["logs"] = sorted(mod["logs"], key=lambda x: x["t"], reverse=True)[:120]
        del mod["top_targets_map"]
        del mod["top_reasons_map"]
        mods.append(mod)
    mods.sort(key=lambda x: x["total_actions"], reverse=True)

    moderation = {
        "summary": {
            "total_actions": len(moderation_actions), "timeouts": stats["timeouts"], "bans": stats["bans"],
            "unbans": stats["unbans"], "deleted_messages": stats["deleted_messages"],
        },
        "mods": mods,
        "recent_actions": sorted(moderation_actions, key=lambda x: x["t"], reverse=True)
    }

    game_special = {
        "all_pool": words[:500], "top_10_users": users[:10], "top_10_words": words[:10], "top_10_emotes": emotes[:10]
    }

    summary = {
        "stats": stats, "users": users, "words": words, "emotes": emotes,
        "spam": spam, "moderation": moderation, "events": events_out, "game_special": game_special
    }

    out_path = SUMMARIES_DIR / f"{date_str}.json"
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(summary, f, ensure_ascii=False)
        
    conn.close()
    print(f"[{date_str}] Analiz tamamlandı. JSON oluşturuldu: {out_path}")

if __name__ == "__main__":
    if len(sys.argv) > 1:
        process_day(sys.argv[1])
    else:
        print("Kullanım: python analyzer.py YYYY-MM-DD")