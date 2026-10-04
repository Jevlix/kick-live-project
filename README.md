# Kick Live Analytics v8

Performans odaklı Kick canlı yayın arşivleyici ve analiz paneli.

## v5 değişiklikleri
- Sadece gerçek livestream sessionları tutulur; offline chat ayrı session olarak kaydedilmez.
- Veri modu UI: **Canlı Yayın** + **Önceki Yayınlar**.
- Chat mesajları persistent queue + batch SQLite WAL ile yazılır.
- Kelime analizi için yapay 1000 kelime sınırı kaldırıldı; tüm benzersiz kelimeler döndürülür.
- Kelime detayında top kullanıcılar doğrudan indeksli DB sorgusundan gelir.
- Emote parser hem `[emote:ID:NAME]` hem `emote:ID:NAME` formatını destekler.
- Emoji sıralaması doğrudan aggregate DB endpointinden gelir.
- Kullanıcı modalı canlı yenilemede kapanmaz.
- Subscription ve Gift Subscription eventleri normalize edilir; gift sayısı `giftees` üzerinden tutulur.
- Moderasyon: mod, hedef, timeout/ban/unban, sebep, süre ve mesaj silme istatistikleri.
- Raw JSONL logları canlı yayın sessionı ile sınırlıdır.

## Kurulum
1. `.env.example` -> `.env`
2. `KICK_CHANNEL_SLUG` doldur.
3. `python -m venv venv`
4. `venv\Scripts\python.exe -m pip install -r requirements.txt`
5. `venv\Scripts\python.exe -m uvicorn server:app --host 127.0.0.1 --port 8000`
6. `http://127.0.0.1:8000`

## Resmi API notu
Kick Public API canlı kanal/livestream metadata sağlayabilir. `events:subscribe` ise yetkilendirilmiş kanal eventleri içindir. Başkasının kanalını sadece izleyen collector için chat tarafında mevcut Pusher reader korunur.


## v6 UI / history fixes
- User detail panels stay open during live refreshes.
- Home Top-15 user cards fetch the same full details as the Users page.
- User chat history is cursor-paginated at `/api/user_messages` and loads older messages while scrolling to the top.
- Long detail headers no longer push the message history out of the viewport.
- A global non-blocking `Veriler yükleniyor…` indicator appears for API loads across pages.
- Word detail stays stable during auto-refresh instead of being closed/replaced.


## v7 UI/state fixes
- Ana sayfa TOP 15 kullanıcı kartları artık gerçek kullanıcı detayını açar.
- 5 saniyelik dashboard yenilemesi açık kullanıcı modalındaki kelime/emote/mesaj detaylarını silemez.
- Hafif kullanıcı özeti ile detay modalı state'i bilinçli olarak ayrıdır.


## v8 runtime hardening
- Kick socket ingestion is isolated from slow dashboard websocket clients.
- Batch DB writes fall back to per-event writes if a batch contains one problematic event.
- Recorder components are supervised and automatically restarted after unexpected task failure.
- Runtime paths are absolute to the project directory.
- See `VPS_KURULUM.md` for a clean Windows VPS deployment.
