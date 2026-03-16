window.RRGameFactory = {
    create(config) {
        const { createApp } = Vue;

        const extraData = typeof config.data === 'function' ? config.data : null;
        const extraComputed = config.computed || {};
        const extraMethods = config.methods || {};

        return createApp({
            data() {
                const sharedFilter =
                    typeof getFilterState === 'function'
                        ? getFilterState()
                        : {
                              mode: 'live',
                              stream_id: '',
                              date: '',
                              month: ''
                          };

                const baseData = {
                    stats: {},
                    users: [],
                    words: [],
                    emotes: [],
                    spam: [],
                    moderation: {
                        summary: {},
                        mods: [],
                        recent_actions: []
                    },
                    gameSpecial: {
                        all_pool: [],
                        top_5_users: [],
                        top_5_words: [],
                        top_5_emotes: []
                    },

                    rawEvents: [],
                    dashboardMeta: {},

                    playerName: '',
                    selectedRounds: config.defaultRounds || 10,

                    arenaSettings: {
                        classicQuiz: true,
                        guessGames: true,
                        compareGames: true,
                        trueFalseGames: true,
                        punishmentGames: true,
                        modGames: true
                    },

                    questionMetric: 'auto',
                    customQuestionPrefix: '',
                    includeDatePrefix: true,
                    includeModePrefix: true,

                    selectedDataMode: sharedFilter.mode || 'live',
                    selectedDataDate: sharedFilter.date || '',
                    selectedDataMonth: sharedFilter.month || '',
                    selectedDataStreamId: sharedFilter.stream_id || '',

                    currentFilterState: {
                        mode: sharedFilter.mode || 'live',
                        stream_id: sharedFilter.stream_id || '',
                        date: sharedFilter.date || '',
                        month: sharedFilter.month || ''
                    },
                    filterContextLabel: '',
                    activeMetricLabel: 'Genel',

                    dataReady: false,
                    loadingData: true,
                    dataError: '',

                    gamePhase: 'lobby',
                    currentQuestionIndex: 0,
                    timeLeft: 20,
                    transitionTime: 4,

                    votes: { 1: 0, 2: 0, 3: 0, 4: 0, 5: 0 },
                    mySelection: null,
                    myGuess: null,
                    lastChatGuess: null,

                    roundWinners: [],
                    globalScores: {},
                    showAnswers: false,
                    timer: null,
                    transitionTimer: null,
                    liveUsersData: {},
                    activeQuestion: {
                        type: 'guess',
                        title: '',
                        options: [],
                        target: '',
                        targetId: '',
                        correct: 0,
                        correctValue: '',
                        infoText: '',
                        isEmoji: false,
                        modeLabel: '',
                        questionPrefix: '',
                        datasetLabel: '',
                        metricLabel: ''
                    },

                    isConnected: false,
                    socketRef: null,
                    liveMessages: [],

                    dashboardRefreshTimer: null
                };

                const customData = extraData ? extraData.call(this) : {};
                return {
                    ...baseData,
                    ...customData
                };
            },

            computed: {
                sidebar() {
                    return sidebarHtml(config.sidebarKey);
                },

                pageTitle() {
                    return config.pageTitle;
                },

                displayedRoundWinners() {
                    return [...this.roundWinners].sort((a, b) => b.p - a.p).slice(0, 10);
                },

                displayedGlobalWinners() {
                    const sorted = Object.entries(this.globalScores)
                        .map(([u, total]) => ({ u, total }))
                        .sort((a, b) => b.total - a.total);

                    sorted.forEach((w, i) => {
                        w.rank = i + 1;
                    });

                    return sorted.slice(0, 10);
                },

                totalRounds() {
                    return Number(this.selectedRounds) || 10;
                },

                availableArenaQuestionCount() {
                    if (!config.isArena || !config.countArenaPool) return 0;
                    return config.countArenaPool.call(this);
                },

                currentModeLabel() {
                    return this.getFilterLabel(this.currentFilterState, this.dashboardMeta);
                },

                gameContextBadge() {
                    const parts = [];

                    if (this.includeModePrefix && this.currentModeLabel) {
                        parts.push(this.currentModeLabel);
                    }

                    if (this.activeMetricLabel && this.questionMetric !== 'auto') {
                        parts.push(this.activeMetricLabel);
                    }

                    return parts.join(' • ');
                },

                ...extraComputed
            },

            methods: {
                parseMessage,
                formatChatTime,

                shuffle(arr) {
                    return [...arr].sort(() => Math.random() - 0.5);
                },

                formatTopList(arr, label = 'kez') {
                    if (!Array.isArray(arr) || !arr.length) return '';
                    return arr
                        .slice(0, 3)
                        .map(x => `${x.u || x.n} ${x.c} ${label}`)
                        .join(' • ');
                },

                renderTextWithEmotes(text) {
                    return parseMessage(text || '');
                },

                getSafeArray(value) {
                    return Array.isArray(value) ? value : [];
                },

                normalizeText(value) {
                    return String(value || '').trim().toLowerCase();
                },

                isSystemUser(username) {
                    const u = this.normalizeText(username);
                    return u === 'system' || u === 'kick system' || u === 'bot';
                },

                isBotrixUser(username) {
                    return this.normalizeText(username).includes('botrix');
                },

                getFilterStateSafe() {
                    try {
                        if (typeof getFilterState === 'function') {
                            return getFilterState();
                        }
                    } catch (e) {
                        console.warn('getFilterState okunamadı:', e);
                    }

                    return {
                        mode: 'live',
                        stream_id: '',
                        date: '',
                        month: ''
                    };
                },

                getSelectedStreamForLabel(filter) {
                    const source = this.streamOptions || this.streams || [];
                    if (!Array.isArray(source)) return null;

                    return source.find(s => String(s.id) === String(filter?.stream_id || '')) || null;
                },

                getFilterLabel(filter, meta = null) {
                    try {
                        if (typeof getReadableFilterLabel === 'function') {
                            return getReadableFilterLabel(
                                filter || this.currentFilterState || this.getSelectedGameFilter(),
                                meta || this.dashboardMeta || {},
                                this.getSelectedStreamForLabel(filter || this.currentFilterState || this.getSelectedGameFilter())
                            );
                        }
                    } catch (e) {
                        console.warn('getReadableFilterLabel kullanılamadı:', e);
                    }

                    const mode = String(filter?.mode || 'live').toLowerCase();

                    if (mode === 'live') return 'Canlı yayın';
                    if (mode === 'offstream_live') return 'Canlı offstream';
                    if (mode === 'all') return 'Tüm veriler';
                    if (mode === 'week') return 'Son 7 gün verisi';
                    if (mode === 'day') return filter?.date || 'Seçili gün';
                    if (mode === 'offstream_day') return filter?.date ? `${filter.date} offstream` : 'Seçili offstream gün';
                    if (mode === 'month') return filter?.month || 'Seçili ay';
                    if (mode === 'stream') return filter?.stream_id ? `Yayın ${filter.stream_id}` : 'Seçili yayın';

                    return 'Veri seç';
                },

                syncSelectedDataWithSharedFilter() {
                    const shared = this.getFilterStateSafe();

                    this.selectedDataMode = shared.mode || 'live';
                    this.selectedDataDate = shared.date || '';
                    this.selectedDataMonth = shared.month || '';
                    this.selectedDataStreamId = shared.stream_id || '';

                    this.currentFilterState = {
                        mode: this.selectedDataMode,
                        date: this.selectedDataDate,
                        month: this.selectedDataMonth,
                        stream_id: this.selectedDataStreamId
                    };

                    this.filterContextLabel = this.getFilterLabel(this.currentFilterState, this.dashboardMeta);
                },

                syncSelectedDataFromFilter(filter) {
                    const safeFilter = {
                        mode: filter?.mode || 'live',
                        date: filter?.date || '',
                        month: filter?.month || '',
                        stream_id: filter?.stream_id || ''
                    };

                    this.selectedDataMode = safeFilter.mode;
                    this.selectedDataDate = safeFilter.date;
                    this.selectedDataMonth = safeFilter.month;
                    this.selectedDataStreamId = safeFilter.stream_id;
                    this.currentFilterState = { ...safeFilter };
                    this.filterContextLabel = this.getFilterLabel(this.currentFilterState, this.dashboardMeta);

                    try {
                        if (typeof setFilterState === 'function') {
                            setFilterState(safeFilter);
                        }
                    } catch (e) {
                        console.warn('setFilterState yazılamadı:', e);
                    }
                },

                getSelectedGameFilter() {
                    return {
                        mode: this.selectedDataMode || 'live',
                        date: this.selectedDataDate || '',
                        month: this.selectedDataMonth || '',
                        stream_id: this.selectedDataStreamId || ''
                    };
                },

                buildFilterContextLabel(filter, meta = null) {
                    return this.getFilterLabel(filter, meta);
                },

                getMetricLabel(metric = 'auto') {
                    const map = {
                        auto: 'Genel',
                        users: 'Kullanıcı',
                        words: 'Kelime',
                        emotes: 'Emoji',
                        spam: 'Spam',
                        moderation: 'Moderasyon',
                        punishments: 'Ceza',
                        mixed: 'Karışık',
                        timeouts: 'Timeout',
                        bans: 'Ban'
                    };

                    return map[metric] || 'Genel';
                },

                getQuestionPrefix(metric = 'auto') {
                    const base = this.getFilterLabel(this.currentFilterState, this.dashboardMeta);

                    if (!this.includeModePrefix) return '';
                    if (!base) return '';

                    return base;
                },

                applyQuestionContext(question, metric = 'auto') {
                    if (!question || typeof question !== 'object') return question;

                    const prefix = this.getQuestionPrefix(metric);
                    const datasetLabel = this.getFilterLabel(this.currentFilterState, this.dashboardMeta);
                    const metricLabel = this.getMetricLabel(metric);

                    question.questionPrefix = prefix;
                    question.datasetLabel = datasetLabel;
                    question.metricLabel = metricLabel;
                    question.modeLabel = question.modeLabel || datasetLabel;

                    if (question.title && prefix) {
                        const rawTitle = String(question.title).trim();
                        const rawLower = rawTitle.toLowerCase();
                        const prefixLower = prefix.toLowerCase();

                        if (!rawLower.startsWith(prefixLower)) {
                            question.title = `${prefix} verisine göre ${rawTitle}`;
                        }
                    }

                    return question;
                },

                setActiveMetricFromQuestion(question) {
                    const metric =
                        question?.metric ||
                        question?.sourceType ||
                        question?.category ||
                        'auto';

                    this.activeMetricLabel = this.getMetricLabel(metric);
                },

                normalizeLoadedData(data) {
                    return {
                        stats: data?.stats || {},
                        users: this.getSafeArray(data?.users),
                        words: this.getSafeArray(data?.words),
                        emotes: this.getSafeArray(data?.emotes),
                        spam: this.getSafeArray(data?.spam),
                        moderation: data?.moderation || {
                            summary: {},
                            mods: [],
                            recent_actions: []
                        },
                        gameSpecial: data?.game_special || data?.gameSpecial || {
                            all_pool: [],
                            top_5_users: [],
                            top_5_words: [],
                            top_5_emotes: []
                        },
                        rawEvents: this.getSafeArray(data?.rawEvents || data?.raw_events)
                    };
                },

                async loadGameData(showAlertOnError = false) {
                    this.loadingData = true;
                    this.dataError = '';

                    try {
                        const filter = this.getSelectedGameFilter();
                        const result = await loadDashboardData(filter);

                        const normalized = this.normalizeLoadedData({
                            ...result?.transformed,
                            rawEvents: result?.transformed?.rawEvents || result?.apiData?.summary?.events || []
                        });

                        this.stats = normalized.stats;
                        this.users = normalized.users;
                        this.words = normalized.words;
                        this.emotes = normalized.emotes;
                        this.spam = normalized.spam;
                        this.moderation = normalized.moderation;
                        this.gameSpecial = normalized.gameSpecial;
                        this.rawEvents = normalized.rawEvents;

                        this.dashboardMeta = result?.apiData?.meta || {};
                        this.currentFilterState = result?.filter || filter;

                        this.selectedDataMode = this.currentFilterState.mode || 'live';
                        this.selectedDataDate = this.currentFilterState.date || '';
                        this.selectedDataMonth = this.currentFilterState.month || '';
                        this.selectedDataStreamId = this.currentFilterState.stream_id || '';

                        this.filterContextLabel = this.getFilterLabel(this.currentFilterState, this.dashboardMeta);

                        this.dataReady = true;
                    } catch (err) {
                        console.error('Oyun verisi yüklenemedi:', err);
                        this.dataError = err?.message || 'Oyun verisi alınamadı.';
                        this.dataReady = false;

                        if (showAlertOnError) {
                            alert(this.dataError);
                        }
                    } finally {
                        this.loadingData = false;
                    }
                },

                async reloadGameDataForFilter(filter, showAlertOnError = false) {
                    this.syncSelectedDataFromFilter(filter);
                    await this.loadGameData(showAlertOnError);
                },

                scheduleDashboardRefresh() {
                    if (this.dashboardRefreshTimer) {
                        clearTimeout(this.dashboardRefreshTimer);
                    }

                    this.dashboardRefreshTimer = setTimeout(() => {
                        const mode = String(this.selectedDataMode || 'live').toLowerCase();
                        if ((mode === 'live' || mode === 'offstream_live') && this.gamePhase === 'lobby') {
                            this.loadGameData(false);
                        }
                    }, 1200);
                },

                resetGame() {
                    if (this.timer) {
                        clearInterval(this.timer);
                        this.timer = null;
                    }

                    if (this.transitionTimer) {
                        clearInterval(this.transitionTimer);
                        this.transitionTimer = null;
                    }

                    this.gamePhase = 'lobby';
                    this.currentQuestionIndex = 0;
                    this.globalScores = {};
                    this.timeLeft = 20;
                    this.transitionTime = 4;
                    this.showAnswers = false;
                    this.roundWinners = [];
                    this.mySelection = null;
                    this.myGuess = null;
                    this.lastChatGuess = null;
                    this.votes = { 1: 0, 2: 0, 3: 0, 4: 0, 5: 0 };
                    this.liveUsersData = {};
                    this.activeQuestion = {
                        type: 'guess',
                        title: '',
                        options: [],
                        target: '',
                        targetId: '',
                        correct: 0,
                        correctValue: '',
                        infoText: '',
                        isEmoji: false,
                        modeLabel: '',
                        questionPrefix: '',
                        datasetLabel: '',
                        metricLabel: ''
                    };
                },

                startTransition() {
                    if (!this.playerName || !this.isConnected) return;

                    if (!this.dataReady) {
                        alert('Veri henüz hazır değil.');
                        return;
                    }

                    if (this.transitionTimer) {
                        clearInterval(this.transitionTimer);
                        this.transitionTimer = null;
                    }

                    try {
                        this.setupActiveQuestion();
                    } catch (e) {
                        console.error(e);
                        alert('Soru oluşturulamadı.');
                        return;
                    }

                    this.gamePhase = 'countdown';
                    this.transitionTime = 4;

                    this.transitionTimer = setInterval(() => {
                        if (this.transitionTime > 1) {
                            this.transitionTime--;
                        } else {
                            clearInterval(this.transitionTimer);
                            this.transitionTimer = null;
                            this.startQuestion();
                        }
                    }, 1000);
                },

                startQuestion() {
                    if (this.timer) {
                        clearInterval(this.timer);
                        this.timer = null;
                    }

                    this.gamePhase = 'playing';
                    this.timeLeft = 20;
                    this.showAnswers = false;
                    this.mySelection = null;
                    this.myGuess = null;
                    this.lastChatGuess = null;
                    this.votes = { 1: 0, 2: 0, 3: 0, 4: 0, 5: 0 };
                    this.liveUsersData = {};

                    this.timer = setInterval(() => {
                        if (this.gamePhase !== 'playing') {
                            clearInterval(this.timer);
                            this.timer = null;
                            return;
                        }

                        this.timeLeft -= 1;

                        if (this.timeLeft <= 0) {
                            this.timeLeft = 0;
                            clearInterval(this.timer);
                            this.timer = null;
                            this.endRound();
                        }
                    }, 1000);
                },

                selectAnswer(i) {
                    if (this.gamePhase === 'playing' && !this.showAnswers) {
                        this.mySelection = i;
                    }
                },

                endRound() {
                    if (this.timer) {
                        clearInterval(this.timer);
                        this.timer = null;
                    }

                    this.showAnswers = true;

                    setTimeout(() => {
                        this.gamePhase = 'leaderboard';
                        const results = [];
                        const correct = this.activeQuestion.correct;

                        Object.entries(this.liveUsersData).forEach(([u, data]) => {
                            let p = 50;
                            let diff = 0;

                            if (this.activeQuestion.type === 'quiz') {
                                if (data.choice === this.activeQuestion.correctIndex) {
                                    p += Math.max(100, 450 - (data.time * 12));
                                }
                            } else {
                                diff = Math.abs((data.guess ?? 0) - correct);
                                p += Math.max(0, 450 - (diff * 2));
                            }

                            results.push({ u, p: Math.floor(p), diff });
                        });

                        let myP = 50;
                        let myDiff = 0;

                        if (this.activeQuestion.type === 'quiz') {
                            if (this.mySelection === this.activeQuestion.correctIndex) {
                                myP += 450;
                            }
                        } else {
                            myDiff = Math.abs((Number(this.myGuess) || 0) - correct);
                            myP += Math.max(0, 450 - (myDiff * 2));
                        }

                        results.push({
                            u: `${this.playerName} (SEN)`,
                            p: Math.floor(myP),
                            diff: myDiff
                        });

                        this.roundWinners = results.sort((a, b) => b.p - a.p);

                        this.roundWinners.forEach(w => {
                            this.globalScores[w.u] = (this.globalScores[w.u] || 0) + w.p;
                        });
                    }, 1200);
                },

                nextStep() {
                    if (this.currentQuestionIndex < this.totalRounds - 1) {
                        this.currentQuestionIndex++;
                        this.startTransition();
                    } else {
                        alert('OYUN BİTTİ!');
                        this.resetGame();
                    }
                },

                initLiveKickChat() {
                    const protocol = location.protocol === 'https:' ? 'wss:' : 'ws:';
                    const socket = new WebSocket(`${protocol}//${location.host}/ws`);
                    this.socketRef = socket;

                    socket.onopen = () => {
                        this.isConnected = true;
                    };

                    socket.onclose = () => {
                        this.isConnected = false;
                        setTimeout(() => this.initLiveKickChat(), 5000);
                    };

                    socket.onerror = () => {
                        this.isConnected = false;
                    };

                    socket.onmessage = (event) => {
                        try {
                            const data = JSON.parse(event.data);

                            if (data.type === 'chat') {
                                const user = data.user || 'Bilinmeyen Kullanıcı';
                                const msg = String(data.msg || '');
                                const cleanMsg = msg.replace(/\[emote:\d+:[^\]]+\]/g, '').trim();

                                this.liveMessages.unshift({
                                    id: `${Date.now()}-${Math.random()}`,
                                    u: user,
                                    m: msg,
                                    t: data.t || data.time || new Date().toISOString()
                                });

                                if (this.liveMessages.length > 40) this.liveMessages.pop();

                                if (this.gamePhase === 'playing') {
                                    if (this.activeQuestion.type === 'quiz') {
                                        const v = parseInt(cleanMsg, 10);

                                        if (v >= 1 && v <= (this.activeQuestion.options?.length || 5)) {
                                            const previous = this.liveUsersData[user];

                                            if (previous && previous.choice >= 1 && previous.choice <= 5) {
                                                this.votes[previous.choice] =
                                                    Math.max(0, (this.votes[previous.choice] || 0) - 1);
                                            }

                                            this.votes[v] = (this.votes[v] || 0) + 1;

                                            this.liveUsersData[user] = {
                                                choice: v,
                                                time: previous?.time ?? (20 - this.timeLeft),
                                                lastChangeTime: 20 - this.timeLeft
                                            };
                                        }
                                    } else {
                                        const g = parseInt(cleanMsg.replace(/[^0-9]/g, ''), 10);

                                        if (!isNaN(g)) {
                                            this.lastChatGuess = g;
                                            this.liveUsersData[user] = {
                                                guess: g,
                                                time: 20 - this.timeLeft
                                            };
                                        }
                                    }
                                }
                            }

                            const mode = String(this.selectedDataMode || 'live').toLowerCase();
                            if ((mode === 'live' || mode === 'offstream_live') && this.gamePhase === 'lobby') {
                                if (data.type === 'chat' || data.type === 'moderation' || data.type === 'bootstrap') {
                                    this.scheduleDashboardRefresh();
                                }
                            }
                        } catch (e) {
                            console.error('WS mesaj parse hatası:', e);
                        }
                    };
                },

                setupActiveQuestion() {
                    let question = config.buildQuestion.call(this, this.currentQuestionIndex);

                    if (!question || !question.type) {
                        throw new Error('Soru oluşturulamadı');
                    }

                    const metric =
                        question?.metric ||
                        question?.sourceType ||
                        question?.category ||
                        this.questionMetric ||
                        'auto';

                    question = this.applyQuestionContext(question, metric);
                    this.setActiveMetricFromQuestion(question);

                    this.activeQuestion = question;

                    if (this.activeQuestion.type === 'quiz') {
                        if (!this.activeQuestion.correctIndex) {
                            this.activeQuestion.correctIndex =
                                this.activeQuestion.options.findIndex(x =>
                                    (x.n || x.w || x.label) === this.activeQuestion.correctValue
                                ) + 1;
                        }
                    }
                },

                getContextTitle(text, metric = 'auto') {
                    const prefix = this.getQuestionPrefix(metric);
                    if (!prefix) return text;
                    return `${prefix} verisine göre ${text}`;
                },

                getDatasetLabel() {
                    return this.getFilterLabel(this.currentFilterState, this.dashboardMeta);
                },

                getCurrentGameDataSnapshot() {
                    return {
                        stats: this.stats,
                        users: this.users,
                        words: this.words,
                        emotes: this.emotes,
                        spam: this.spam,
                        moderation: this.moderation,
                        gameSpecial: this.gameSpecial,
                        rawEvents: this.rawEvents,
                        filter: this.currentFilterState,
                        filterLabel: this.filterContextLabel,
                        metricLabel: this.activeMetricLabel
                    };
                },

                async reloadGameDataFromUI(filter = null) {
                    if (filter) {
                        await this.reloadGameDataForFilter(filter, true);
                    } else {
                        await this.loadGameData(true);
                    }
                },

                ...extraMethods
            },

            async mounted() {
                this.syncSelectedDataWithSharedFilter();
                await this.loadGameData(false);
                this.initLiveKickChat();

                window.addEventListener('storage', async (event) => {
                    if (event.key === 'rr_shared_filter_state_v1') {
                        this.syncSelectedDataWithSharedFilter();
                        await this.loadGameData(false);
                    }
                });
            },

            beforeUnmount() {
                try {
                    if (this.timer) clearInterval(this.timer);
                    if (this.transitionTimer) clearInterval(this.transitionTimer);
                    if (this.dashboardRefreshTimer) clearTimeout(this.dashboardRefreshTimer);
                    if (this.socketRef) this.socketRef.close();
                } catch (e) {}
            }
        });
    }
};