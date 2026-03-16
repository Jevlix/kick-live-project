function parseMessage(msg) {
    return String(msg || '').replace(
        /\[emote:(\d+):([^\]]+)\]/g,
        (match, id, name) =>
            `<img src="https://files.kick.com/emotes/${id}/fullsize" class="chat-emote" title="${name}">`
    );
}

const RR_FILTER_KEY = 'rr_shared_filter_state_v1';

function getDefaultFilterState() {
    const now = new Date();
    const yyyy = now.getFullYear();
    const mm = String(now.getMonth() + 1).padStart(2, '0');
    const dd = String(now.getDate()).padStart(2, '0');

    return {
        mode: 'live',
        stream_id: '',
        date: `${yyyy}-${mm}-${dd}`,
        month: `${yyyy}-${mm}`
    };
}

function getFilterState() {
    try {
        const raw = localStorage.getItem(RR_FILTER_KEY);
        if (!raw) return getDefaultFilterState();

        const parsed = JSON.parse(raw);
        return {
            ...getDefaultFilterState(),
            ...parsed
        };
    } catch (e) {
        return getDefaultFilterState();
    }
}

function setFilterState(nextState) {
    const merged = {
        ...getDefaultFilterState(),
        ...nextState
    };
    localStorage.setItem(RR_FILTER_KEY, JSON.stringify(merged));
    return merged;
}

async function fetchStreams(limit = 80) {
    const res = await fetch(`/api/streams?limit=${limit}`);

    if (!res.ok) {
        throw new Error('streams alınamadı');
    }

    const data = await res.json();

    if (Array.isArray(data)) {
        return data;
    }

    if (data?.ok === false) {
        throw new Error(data?.error || 'streams alınamadı');
    }

    if (Array.isArray(data?.streams)) {
        return data.streams;
    }

    if (Array.isArray(data?.data)) {
        return data.data;
    }

    return [];
}

function normalizeWord(word) {
    return String(word || '')
        .toLowerCase()
        .trim()
        .replace(/[^\p{L}\p{N}_]/gu, '');
}

function formatHumanDate(dateStr) {
    if (!dateStr) return '-';

    try {
        const [y, m, d] = String(dateStr).split('-').map(Number);
        const months = {
            1: 'Ocak',
            2: 'Şubat',
            3: 'Mart',
            4: 'Nisan',
            5: 'Mayıs',
            6: 'Haziran',
            7: 'Temmuz',
            8: 'Ağustos',
            9: 'Eylül',
            10: 'Ekim',
            11: 'Kasım',
            12: 'Aralık'
        };
        return `${d} ${months[m] || m}`;
    } catch (e) {
        return String(dateStr);
    }
}

function getReadableFilterLabel(filter, apiMeta = null, selectedStream = null) {
    if (apiMeta?.label) return apiMeta.label;

    const f = filter || getFilterState();
    const mode = String(f?.mode || 'live').toLowerCase();

    if (mode === 'live') return 'Canlı yayın';
    if (mode === 'offstream_live') return 'Canlı offstream';
    if (mode === 'stream') {
        if (selectedStream?.display_label) return selectedStream.display_label;
        if (selectedStream?.label_date) {
            return `${formatHumanDate(selectedStream.label_date)} ${selectedStream.session_type === 'offstream' ? 'offstream' : 'yayını'}`;
        }
        return 'Yayın seç';
    }
    if (mode === 'day') return f?.date ? `${formatHumanDate(f.date)} verisi` : 'Gün seç';
    if (mode === 'offstream_day') return f?.date ? `${formatHumanDate(f.date)} offstream` : 'Offstream gün seç';
    if (mode === 'week') return 'Son 7 gün verisi';
    if (mode === 'month') {
        if (!f?.month) return 'Ay seç';
        const [y, m] = String(f.month).split('-');
        const months = {
            '01': 'Ocak',
            '02': 'Şubat',
            '03': 'Mart',
            '04': 'Nisan',
            '05': 'Mayıs',
            '06': 'Haziran',
            '07': 'Temmuz',
            '08': 'Ağustos',
            '09': 'Eylül',
            '10': 'Ekim',
            '11': 'Kasım',
            '12': 'Aralık'
        };
        return `${months[m] || m} ${y} verisi`;
    }
    if (mode === 'all') return 'Tüm veriler';

    return 'Seçili veri';
}

function buildModerationFromEvents(events) {
    const modsMap = {};
    const recentActions = [];
    const summary = {
        total_actions: 0,
        timeouts: 0,
        bans: 0,
        unbans: 0,
        deleted_messages: 0
    };

    for (const ev of events || []) {
        const type = ev.event_type;
        const modName = ev.moderator || 'Unknown Mod';

        if (!['deleted', 'ban', 'unban'].includes(type)) continue;

        if (!modsMap[modName]) {
            modsMap[modName] = {
                n: modName,
                total_actions: 0,
                timeouts: 0,
                bans: 0,
                unbans: 0,
                deleted_messages: 0
            };
        }

        modsMap[modName].total_actions += 1;
        summary.total_actions += 1;

        let action = type;

        if (type === 'deleted') {
            modsMap[modName].deleted_messages += 1;
            summary.deleted_messages += 1;
            action = 'deleted';
        } else if (type === 'unban') {
            modsMap[modName].unbans += 1;
            summary.unbans += 1;
            action = 'unban';
        } else if (type === 'ban') {
            if (Number(ev.permanent) === 1) {
                modsMap[modName].bans += 1;
                summary.bans += 1;
                action = 'ban';
            } else {
                modsMap[modName].timeouts += 1;
                summary.timeouts += 1;
                action = 'timeout';
            }
        }

        recentActions.push({
            action,
            mod: modName,
            target: ev.target_username || ev.username || 'Bilinmiyor',
            reason: ev.reason || '',
            duration: ev.duration || '',
            msg: ev.message || '',
            t: ev.timestamp || ''
        });
    }

    const mods = Object.values(modsMap).sort((a, b) => b.total_actions - a.total_actions);

    return {
        summary,
        mods,
        recent_actions: recentActions.slice(-100)
    };
}

function buildEmotesFromEvents(events) {
    const emoteMap = {};

    for (const ev of events || []) {
        if (ev.event_type !== 'chat') continue;

        const msg = String(ev.message || '');
        const matches = [...msg.matchAll(/\[emote:(\d+):([^\]]+)\]/g)];

        for (const m of matches) {
            const id = m[1];
            const name = m[2];
            const key = `${id}:${name}`;

            if (!emoteMap[key]) {
                emoteMap[key] = { id, n: name, c: 0 };
            }
            emoteMap[key].c += 1;
        }
    }

    return Object.values(emoteMap).sort((a, b) => b.c - a.c);
}

function buildSpamFromEvents(events) {
    const spamList = [];
    const userLastMessages = {};

    for (const ev of events || []) {
        if (ev.event_type !== 'chat' || !ev.username) continue;

        const msg = String(ev.message || '').trim();
        const key = ev.username.toLowerCase();

        if (!userLastMessages[key]) {
            userLastMessages[key] = { msg, count: 1, t: ev.timestamp };
            continue;
        }

        if (userLastMessages[key].msg === msg && msg.length > 0) {
            userLastMessages[key].count += 1;

            if (userLastMessages[key].count === 3) {
                spamList.push({
                    u: ev.username,
                    m: msg,
                    t: ev.timestamp
                });
            }
        } else {
            userLastMessages[key] = { msg, count: 1, t: ev.timestamp };
        }
    }

    return spamList.slice(0, 200);
}

function buildUsersFromSummary(summaryUsers, events) {
    const usersMap = {};

    for (const u of summaryUsers || []) {
        const username = u.username || u.n;
        usersMap[username] = {
            n: username,
            mc: u.messages || u.mc || 0,
            wc: 0,
            ec: 0,
            tw: [],
            te: [],
            logs: [],
            mod_received: u.mod_received || {
                timeouts: 0,
                bans: 0,
                unbans: 0,
                deleted_messages: 0
            },
            mod_history_received: u.mod_history_received || []
        };
    }

    for (const ev of events || []) {
        if (ev.event_type !== 'chat' || !ev.username) continue;

        if (!usersMap[ev.username]) {
            usersMap[ev.username] = {
                n: ev.username,
                mc: 0,
                wc: 0,
                ec: 0,
                tw: [],
                te: [],
                logs: [],
                mod_received: {
                    timeouts: 0,
                    bans: 0,
                    unbans: 0,
                    deleted_messages: 0
                },
                mod_history_received: []
            };
        }

        const user = usersMap[ev.username];
        const msg = String(ev.message || '');

        user.logs.unshift({
            t: ev.timestamp || '',
            m: msg
        });

        if (user.logs.length > 20) user.logs.pop();

        const localWords = {};
        const tokens = msg.split(/\s+/).filter(Boolean);

        for (const token of tokens) {
            const cleaned = normalizeWord(token);
            if (cleaned.length >= 2) {
                user.wc += 1;
                localWords[cleaned] = (localWords[cleaned] || 0) + 1;
            }

            const emojiMatches = token.match(/\[emote:(\d+):([^\]]+)\]/g);
            if (emojiMatches) {
                for (const raw of emojiMatches) {
                    const m = raw.match(/\[emote:(\d+):([^\]]+)\]/);
                    if (m) {
                        user.ec += 1;
                    }
                }
            }
        }

        for (const [k, v] of Object.entries(localWords)) {
            if (!user.__wordMap) user.__wordMap = {};
            user.__wordMap[k] = (user.__wordMap[k] || 0) + v;
        }
    }

    const result = Object.values(usersMap).map((u) => {
        const wordMap = u.__wordMap || {};
        u.tw = Object.entries(wordMap)
            .sort((a, b) => b[1] - a[1])
            .slice(0, 8);

        delete u.__wordMap;
        return u;
    });

    result.sort((a, b) => b.mc - a.mc);
    result.forEach((u, i) => u.rank = i + 1);

    return result;
}

function transformApiData(apiData) {
    const summary = apiData?.summary || {};
    const stats = summary?.stats || {};
    const events = summary?.events || [];
    const apiUsers = summary?.users || [];
    const apiWords = summary?.words || [];
    const apiEmotes = summary?.emotes || [];
    const apiSpam = summary?.spam || [];
    const apiModeration = summary?.moderation || null;
    const apiGameSpecial = summary?.game_special || {};

    const users = buildUsersFromSummary(apiUsers, events);
    const emotes = apiEmotes.length ? apiEmotes : buildEmotesFromEvents(events);
    const spam = apiSpam.length ? apiSpam : buildSpamFromEvents(events);
    const moderation = apiModeration || buildModerationFromEvents(events);

    return {
        stats: {
            total_users: stats.unique_users || 0,
            total_msgs: stats.total_messages || 0,
            deleted_messages: stats.deleted_messages || 0,
            timeouts: stats.timeouts || 0,
            bans: stats.bans || 0,
            unbans: stats.unbans || 0,
            subscriptions: stats.subscriptions || 0,
            gift_subscriptions: stats.gift_subscriptions || 0,
            other_events: stats.other_events || 0
        },
        users,
        words: (apiWords || []).map(w => ({
            w: w.word || w.w,
            c: w.count ?? w.c,
            top: w.top || []
        })),
        emotes,
        spam,
        moderation,
        game_special: apiGameSpecial,
        rawEvents: events
    };
}

async function loadDashboardData(customFilter = null) {
    const filter = customFilter || getFilterState();

    let url = '/api/data?mode=all';

    if (filter.mode === 'live') {
        url = '/api/data?mode=live';
    } else if (filter.mode === 'offstream_live') {
        url = '/api/data?mode=offstream_live';
    } else if (filter.mode === 'stream') {
        if (!filter.stream_id) throw new Error('stream_id gerekli');
        url = `/api/data?mode=stream&stream_id=${encodeURIComponent(filter.stream_id)}`;
    } else if (filter.mode === 'day') {
        if (!filter.date) throw new Error('date gerekli');
        url = `/api/data?mode=day&date=${encodeURIComponent(filter.date)}`;
    } else if (filter.mode === 'offstream_day') {
        if (!filter.date) throw new Error('date gerekli');
        url = `/api/data?mode=offstream_day&date=${encodeURIComponent(filter.date)}`;
    } else if (filter.mode === 'week') {
        url = '/api/data?mode=week';
    } else if (filter.mode === 'month') {
        if (!filter.month) throw new Error('month gerekli');
        url = `/api/data?mode=month&month=${encodeURIComponent(filter.month)}`;
    } else if (filter.mode === 'all') {
        url = '/api/data?mode=all';
    }

    const res = await fetch(url);
    const data = await res.json();

    if (!data.ok) {
        throw new Error(data.error || 'api_data_failed');
    }

    return {
        filter,
        apiData: data,
        transformed: transformApiData(data)
    };
}

function sidebarHtml(activePage) {
    return `
        <aside class="w-64 glass flex flex-col p-6 space-y-6 shadow-2xl z-20">
            <div class="flex items-center gap-3">
                <div class="w-10 h-10 bg-pink-600 rounded-lg flex items-center justify-center font-black text-xl text-white">RR</div>
                <h1 class="text-2xl font-black text-pink-500 tracking-tighter uppercase">RRaenee</h1>
            </div>

            <nav class="flex flex-col space-y-1">
                <a href="/index.html" class="p-3 rounded-xl text-left transition ${activePage === 'index' ? 'active-link' : 'hover:bg-white/5'}">
                    <i class="fa fa-chart-pie mr-3"></i> Chat Özeti
                </a>

                <a href="/users.html" class="p-3 rounded-xl text-left transition ${activePage === 'users' ? 'active-link' : 'hover:bg-white/5'}">
                    <i class="fa fa-users mr-3"></i> Chat Sıralaması
                </a>

                <a href="/words.html" class="p-3 rounded-xl text-left transition ${activePage === 'words' ? 'active-link' : 'hover:bg-white/5'}">
                    <i class="fa fa-font mr-3"></i> Kelime Sıralaması
                </a>

                <a href="/emojis.html" class="p-3 rounded-xl text-left transition ${activePage === 'emojis' ? 'active-link text-yellow-400' : 'hover:bg-yellow-900/20 text-yellow-400'}">
                    <i class="fa fa-face-smile mr-3"></i> Emoji Sıralaması
                </a>

                <a href="/spam.html" class="p-3 rounded-xl text-left transition ${activePage === 'spam' ? 'active-link text-red-400' : 'hover:bg-red-900/20 text-red-400'}">
                    <i class="fa fa-ghost mr-3"></i> Spam Raporu
                </a>

                <a href="/moderation.html" class="p-3 rounded-xl text-left transition ${activePage === 'moderation' ? 'active-link text-cyan-400' : 'hover:bg-cyan-900/20 text-cyan-400'}">
                    <i class="fa fa-shield-halved mr-3"></i> Moderasyon
                </a>

                <a href="/cezalar.html" class="p-3 rounded-xl text-left transition ${activePage === 'cezalar' ? 'active-link text-orange-400' : 'hover:bg-orange-900/20 text-orange-400'}">
                    <i class="fa fa-gavel mr-3"></i> Ceza Sıralaması
                </a>

                <div class="pt-3 mt-3 border-t border-white/10 text-[10px] uppercase tracking-widest opacity-40 px-3 font-black">
                    Oyunlar
                </div>

                <a href="/arena.html" class="p-3 rounded-xl text-left transition ${activePage === 'arena' ? 'active-link' : 'hover:bg-pink-900/20 text-pink-300'}">
                    <i class="fa fa-trophy mr-3"></i> Gagara Arena
                </a>

                <a href="/kelime-tahmin.html" class="p-3 rounded-xl text-left transition ${activePage === 'kelimeTahmin' ? 'active-link text-green-400' : 'hover:bg-green-900/20 text-green-400'}">
                    <i class="fa fa-font mr-3"></i> Kelime Tahmin
                </a>

                <a href="/emoji-tahmin.html" class="p-3 rounded-xl text-left transition ${activePage === 'emojiTahmin' ? 'active-link text-yellow-400' : 'hover:bg-yellow-900/20 text-yellow-400'}">
                    <i class="fa fa-face-smile mr-3"></i> Emoji Tahmin
                </a>

                <a href="/karsilastirma.html" class="p-3 rounded-xl text-left transition ${activePage === 'karsilastirma' ? 'active-link text-violet-400' : 'hover:bg-violet-900/20 text-violet-400'}">
                    <i class="fa fa-scale-balanced mr-3"></i> Karşılaştırma
                </a>

                <a href="/dogru-yanlis.html" class="p-3 rounded-xl text-left transition ${activePage === 'dogruYanlis' ? 'active-link text-sky-400' : 'hover:bg-sky-900/20 text-sky-400'}">
                    <i class="fa fa-check-double mr-3"></i> Doğru / Yanlış
                </a>
            </nav>
        </aside>
    `;
}

function enrichUsers(users) {
    return (users || []).map((u, index) => ({
        ...u,
        rank: index + 1
    }));
}

function getNestedUserList(item) {
    if (!item) return [];
    if (Array.isArray(item.users)) return item.users;
    if (Array.isArray(item.top)) return item.top;
    return [];
}

function getEntryName(item) {
    if (Array.isArray(item)) return item[0] ?? '-';
    return item?.n || item?.u || item?.name || item?.user || '-';
}

function getEntryCount(item) {
    if (Array.isArray(item)) return item[1] ?? 0;
    return item?.c || item?.count || item?.value || 0;
}

function getActionColor(action) {
    switch (String(action || '').toLowerCase()) {
        case 'ban': return 'text-red-400';
        case 'timeout': return 'text-yellow-400';
        case 'unban': return 'text-green-400';
        case 'delete':
        case 'deleted': return 'text-pink-400';
        default: return 'text-cyan-400';
    }
}

function formatChatTime(value) {
    if (!value) return '-';

    try {
        const d = new Date(value);
        if (!isNaN(d.getTime())) {
            return d.toLocaleTimeString('tr-TR', {
                hour: '2-digit',
                minute: '2-digit',
                second: '2-digit'
            });
        }
    } catch (e) {}

    if (typeof value === 'string' && value.includes('T')) {
        const m = value.match(/T(\d{2}):(\d{2}):(\d{2})/);
        if (m) return `${m[1]}:${m[2]}:${m[3]}`;
    }

    return String(value);
}