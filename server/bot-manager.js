/**
 * BotManager - 多用户 Bot 实例管理器
 *
 * 职责:
 *   - 维护 Map<userId, BotInstance> 内存映射
 *   - 与 SQLite 持久层交互
 *   - 通过 Socket.io 广播实时事件
 *   - QR 扫码登录流程管理
 */

const EventEmitter = require('events');
const { BotInstance } = require('./bot-instance');
const db = require('./database');
const { requestQrLogin, getQrCodeBase64 } = require('./qr-service');
const { CONFIG } = require('../src/config');

function parseFeatureToggles(raw) {
    if (!raw) return null;
    if (typeof raw === 'object') return raw;
    try {
        const parsed = JSON.parse(raw);
        return parsed && typeof parsed === 'object' ? parsed : null;
    } catch {
        return null;
    }
}

function parseFriendActionConfig(raw) {
    if (!raw) return {};
    if (typeof raw === 'object') return raw;
    try {
        const parsed = JSON.parse(raw);
        return parsed && typeof parsed === 'object' ? parsed : {};
    } catch {
        return {};
    }
}

function parseDailyStats(raw) {
    if (!raw) return null;
    if (typeof raw === 'object') return raw;
    try {
        const parsed = JSON.parse(raw);
        return parsed && typeof parsed === 'object' ? parsed : null;
    } catch {
        return null;
    }
}

function normalizeNotifyCooldownSec(val, fallback = 60) {
    const n = Number(val);
    if (!Number.isFinite(n)) return fallback;
    return Math.max(0, Math.min(86400, Math.trunc(n)));
}

function resolveNapcatNotifyFlags(raw = {}) {
    const legacyEnabled = !!raw.napcat_notify_enabled;
    const hasMatureFlag = raw.napcat_notify_mature_enabled !== undefined && raw.napcat_notify_mature_enabled !== null;
    const hasHelpFlag = raw.napcat_notify_help_enabled !== undefined && raw.napcat_notify_help_enabled !== null;
    return {
        matureEnabled: hasMatureFlag ? !!raw.napcat_notify_mature_enabled : legacyEnabled,
        helpEnabled: hasHelpFlag ? !!raw.napcat_notify_help_enabled : legacyEnabled,
    };
}

class BotManager extends EventEmitter {
    constructor() {
        super();
        /** @type {Map<string, BotInstance>} userId (uin) → bot 实例 */
        this.bots = new Map();
        /** @type {Map<string, object>} userId → 进行中的 QR 登录会话 */
        this.qrSessions = new Map();
    }

    // ============================================================
    //  实例管理
    // ============================================================

    /**
     * 获取所有账号状态列表 (合并 DB + 内存)
     */
    listAccounts() {
        const users = db.getAllUsers();
        return users.map(u => {
            const notifyFlags = resolveNapcatNotifyFlags(u);
            const bot = this.bots.get(u.uin);
            if (bot) {
                const snap = bot.getSnapshot();
                return {
                    uin: u.uin,
                    nickname: snap.userState.name || u.nickname,
                    gid: snap.userState.gid || u.gid,
                    level: snap.userState.level || u.level,
                    gold: snap.userState.gold || u.gold,
                    exp: snap.userState.exp || u.exp,
                    status: snap.status,
                    errorMessage: snap.errorMessage,
                    platform: u.platform,
                    farmInterval: u.farm_interval,
                    friendInterval: u.friend_interval,
                    friendWhitelist: u.friend_whitelist || '',
                    napcatNotifyEnabled: notifyFlags.matureEnabled || notifyFlags.helpEnabled,
                    napcatNotifyMatureEnabled: notifyFlags.matureEnabled,
                    napcatNotifyHelpEnabled: notifyFlags.helpEnabled,
                    napcatBaseUrl: u.napcat_base_url || '',
                    napcatGroupId: u.napcat_group_id || '',
                    autoStart: !!u.auto_start,
                    startedAt: snap.startedAt,
                    uptime: snap.uptime,
                    createdAt: u.created_at,
                };
            }
            return {
                uin: u.uin,
                nickname: u.nickname,
                gid: u.gid,
                level: u.level,
                gold: u.gold,
                exp: u.exp,
                status: u.status === 'running' ? 'stopped' : u.status, // DB 说 running 但没有内存实例，纠正
                errorMessage: '',
                platform: u.platform,
                farmInterval: u.farm_interval,
                friendInterval: u.friend_interval,
                friendWhitelist: u.friend_whitelist || '',
                napcatNotifyEnabled: notifyFlags.matureEnabled || notifyFlags.helpEnabled,
                napcatNotifyMatureEnabled: notifyFlags.matureEnabled,
                napcatNotifyHelpEnabled: notifyFlags.helpEnabled,
                napcatBaseUrl: u.napcat_base_url || '',
                napcatGroupId: u.napcat_group_id || '',
                autoStart: !!u.auto_start,
                startedAt: null,
                uptime: 0,
                createdAt: u.created_at,
            };
        });
    }

    /**
     * 获取单个账号状态
     */
    getAccount(uin) {
        const accounts = this.listAccounts();
        return accounts.find(a => a.uin === uin) || null;
    }

    // ============================================================
    //  QR 扫码登录流程
    // ============================================================

    /**
     * 发起 QR 登录流程
     * @param {string} uin - 用户标识 (QQ号)
     * @param {object} opts - { platform: 'qq'|'wx', farmInterval, friendInterval }
     * @returns {{ qrBase64: string, qrUrl: string }} 二维码数据
     */
    async startQrLogin(uin, opts = {}) {
        if (this.qrSessions.has(uin)) {
            throw new Error('该账号已有扫码会话进行中');
        }
        if (this.bots.has(uin) && this.bots.get(uin).status === 'running') {
            throw new Error('该账号已在运行中');
        }

        const platform = opts.platform || 'qq';

        // 确保 DB 中有记录
        let user = db.getUserByUin(uin);
        if (!user) {
            user = db.createUser({
                uin,
                platform,
                farmInterval: opts.farmInterval || CONFIG.farmCheckInterval,
                friendInterval: opts.friendInterval || CONFIG.friendCheckInterval,
            });
        }
        const notifyFlags = resolveNapcatNotifyFlags(user);

        // 请求二维码
        const { loginCode, url } = await requestQrLogin();
        const qrBase64 = await getQrCodeBase64(url);

        // 保存会话
        const session = {
            uin, loginCode, url, platform,
            farmInterval: opts.farmInterval || user.farm_interval || CONFIG.farmCheckInterval,
            friendInterval: opts.friendInterval || user.friend_interval || CONFIG.friendCheckInterval,
            friendNotifyCooldownSec: normalizeNotifyCooldownSec(user.friend_notify_cooldown_sec, 60),
            friendWhitelist: opts.friendWhitelist !== undefined ? String(opts.friendWhitelist || '') : (user.friend_whitelist || ''),
            friendActionConfig: parseFriendActionConfig(user.friend_action_config),
            dailyStats: parseDailyStats(user.daily_stats),
            featureToggles: parseFeatureToggles(user.feature_toggles),
            napcatNotifyMatureEnabled: notifyFlags.matureEnabled,
            napcatNotifyHelpEnabled: notifyFlags.helpEnabled,
            napcatBaseUrl: user.napcat_base_url || '',
            napcatGroupId: user.napcat_group_id || '',
            napcatAccessToken: user.napcat_access_token || '',
            createdAt: Date.now(),
        };
        this.qrSessions.set(uin, session);

        // 开始轮询扫码状态
        this._pollQrLogin(uin);

        return { qrBase64, qrUrl: url };
    }

    /**
     * 轮询扫码状态
     */
    async _pollQrLogin(uin) {
        const { queryScanStatus, getAuthCode } = require('./qr-service');
        const session = this.qrSessions.get(uin);
        if (!session) return;

        const POLL_INTERVAL = 2000;
        const TIMEOUT = 180000;
        const start = Date.now();

        const poll = async () => {
            if (!this.qrSessions.has(uin)) return; // 已取消

            if (Date.now() - start > TIMEOUT) {
                this.qrSessions.delete(uin);
                this.emit('qrExpired', { uin });
                db.updateUserStatus(uin, 'stopped');
                return;
            }

            try {
                const result = await queryScanStatus(session.loginCode);
                if (result.status === 'OK') {
                    const code = await getAuthCode(result.ticket);
                    this.qrSessions.delete(uin);
                    this.emit('qrScanned', { uin });
                    // 保存 session 并启动 bot
                    db.saveSession(uin, code);
                    db.updateUser(uin, { last_login_at: new Date().toISOString() });
                    await this._startBot(uin, code, session);
                    return;
                }
                if (result.status === 'Used') {
                    this.qrSessions.delete(uin);
                    this.emit('qrExpired', { uin, reason: '二维码已失效' });
                    return;
                }
                if (result.status === 'Error') {
                    this.qrSessions.delete(uin);
                    this.emit('qrError', { uin, reason: '扫码查询错误' });
                    return;
                }
                // Wait: 继续轮询
                setTimeout(poll, POLL_INTERVAL);
            } catch (err) {
                this.qrSessions.delete(uin);
                this.emit('qrError', { uin, reason: err.message });
            }
        };

        // 延迟第一次轮询
        setTimeout(poll, POLL_INTERVAL);
    }

    /**
     * 取消 QR 登录
     */
    cancelQrLogin(uin) {
        this.qrSessions.delete(uin);
        this.emit('qrCancelled', { uin });
    }

    // ============================================================
    //  Bot 启停
    // ============================================================

    /**
     * 用已有 code 启动 Bot 实例
     */
    async _startBot(uin, code, opts = {}) {
        // 清理旧实例
        if (this.bots.has(uin)) {
            const old = this.bots.get(uin);
            old.destroy();
            this.bots.delete(uin);
        }

        const bot = new BotInstance(uin, {
            platform: opts.platform || 'qq',
            farmInterval: opts.farmInterval || CONFIG.farmCheckInterval,
            friendInterval: opts.friendInterval || CONFIG.friendCheckInterval,
            friendNotifyCooldownSec: normalizeNotifyCooldownSec(opts.friendNotifyCooldownSec, 60),
            friendWhitelist: opts.friendWhitelist || '',
            friendActionConfig: parseFriendActionConfig(opts.friendActionConfig),
            dailyStats: parseDailyStats(opts.dailyStats),
            preferredSeedId: opts.preferredSeedId || 0,
            featureToggles: parseFeatureToggles(opts.featureToggles),
            napcatNotifyMatureEnabled: opts.napcatNotifyMatureEnabled !== undefined
                ? !!opts.napcatNotifyMatureEnabled
                : !!opts.napcatNotifyEnabled,
            napcatNotifyHelpEnabled: opts.napcatNotifyHelpEnabled !== undefined
                ? !!opts.napcatNotifyHelpEnabled
                : !!opts.napcatNotifyEnabled,
            napcatBaseUrl: opts.napcatBaseUrl || '',
            napcatGroupId: opts.napcatGroupId || '',
            napcatAccessToken: opts.napcatAccessToken || '',
        });

        // 监听事件并转发给 BotManager 的事件总线
        bot.on('log', (entry) => {
            this.emit('botLog', entry);
            // 可选: 持久化到 DB
            // db.addLog(uin, entry.tag, entry.msg, entry.level);
        });

        bot.on('statusChange', (data) => {
            db.updateUserStatus(uin, data.newStatus);
            if (data.newStatus !== 'running') {
                db.updateUser(uin, { daily_stats: JSON.stringify(bot.dailyStats || {}) });
            }
            this.emit('botStatusChange', data);
        });

        bot.on('stateUpdate', (data) => {
            // 更新 DB 中的游戏状态 + 今日统计
            db.updateUser(uin, {
                nickname: data.userState.name,
                gid: data.userState.gid,
                level: data.userState.level,
                gold: data.userState.gold,
                exp: data.userState.exp,
                daily_stats: JSON.stringify(bot.dailyStats || {}),
            });
            this.emit('botStateUpdate', data);
        });

        this.bots.set(uin, bot);
        db.updateUserStatus(uin, 'connecting');

        try {
            await bot.start(code);
        } catch (err) {
            db.updateUserStatus(uin, 'error');
            this.emit('botError', { uin, error: err.message });
        }
    }

    /**
     * 停止指定 Bot
     */
    async stopBot(uin) {
        const bot = this.bots.get(uin);
        if (!bot) throw new Error('未找到运行中的 Bot 实例');
        bot.stop();
        db.updateUserStatus(uin, 'stopped');
    }

    /**
     * 使用已保存的 session 重新启动 Bot
     */
    async restartBot(uin) {
        const code = db.getSession(uin);
        if (!code) throw new Error('没有保存的登录凭证，请重新扫码');
        const user = db.getUserByUin(uin);
        const notifyFlags = resolveNapcatNotifyFlags(user || {});
        await this._startBot(uin, code, {
            platform: user?.platform || 'qq',
            farmInterval: user?.farm_interval || 10000,
            friendInterval: user?.friend_interval || 10000,
            friendNotifyCooldownSec: normalizeNotifyCooldownSec(user?.friend_notify_cooldown_sec, 60),
            friendWhitelist: user?.friend_whitelist || '',
            friendActionConfig: parseFriendActionConfig(user?.friend_action_config),
            dailyStats: parseDailyStats(user?.daily_stats),
            preferredSeedId: user?.preferred_seed_id || 0,
            featureToggles: parseFeatureToggles(user?.feature_toggles),
            napcatNotifyMatureEnabled: notifyFlags.matureEnabled,
            napcatNotifyHelpEnabled: notifyFlags.helpEnabled,
            napcatBaseUrl: user?.napcat_base_url || '',
            napcatGroupId: user?.napcat_group_id || '',
            napcatAccessToken: user?.napcat_access_token || '',
        });
    }

    /**
     * 删除账号 (停止运行 + 删除 DB 记录)
     */
    async removeAccount(uin) {
        if (this.bots.has(uin)) {
            this.bots.get(uin).destroy();
            this.bots.delete(uin);
        }
        this.qrSessions.delete(uin);
        db.deleteUser(uin);
    }

    /**
     * 获取某 Bot 的最近日志
     */
    getBotLogs(uin, limit = 100) {
        const bot = this.bots.get(uin);
        if (bot) return bot.getRecentLogs(limit);
        return db.getRecentLogs(uin, limit);
    }

    /**
     * 修改账号配置
     */
    updateAccountConfig(uin, {
        farmInterval, friendInterval, friendNotifyCooldownSec, friendWhitelist, friendActionConfig, autoStart, platform, preferredSeedId,
        napcatNotifyEnabled, napcatNotifyMatureEnabled, napcatNotifyHelpEnabled,
        napcatBaseUrl, napcatGroupId, napcatAccessToken
    }) {
        const updates = {};
        if (farmInterval !== undefined) updates.farm_interval = farmInterval;
        if (friendInterval !== undefined) updates.friend_interval = friendInterval;
        if (friendNotifyCooldownSec !== undefined) updates.friend_notify_cooldown_sec = normalizeNotifyCooldownSec(friendNotifyCooldownSec, 60);
        if (friendWhitelist !== undefined) updates.friend_whitelist = String(friendWhitelist || '');
        if (friendActionConfig !== undefined) updates.friend_action_config = JSON.stringify(parseFriendActionConfig(friendActionConfig));
        if (autoStart !== undefined) updates.auto_start = autoStart ? 1 : 0;
        if (platform !== undefined) updates.platform = platform;
        if (preferredSeedId !== undefined) updates.preferred_seed_id = preferredSeedId;
        if (napcatNotifyEnabled !== undefined) {
            const legacyEnabled = napcatNotifyEnabled ? 1 : 0;
            updates.napcat_notify_enabled = legacyEnabled;
            if (napcatNotifyMatureEnabled === undefined) updates.napcat_notify_mature_enabled = legacyEnabled;
            if (napcatNotifyHelpEnabled === undefined) updates.napcat_notify_help_enabled = legacyEnabled;
        }
        if (napcatNotifyMatureEnabled !== undefined) updates.napcat_notify_mature_enabled = napcatNotifyMatureEnabled ? 1 : 0;
        if (napcatNotifyHelpEnabled !== undefined) updates.napcat_notify_help_enabled = napcatNotifyHelpEnabled ? 1 : 0;
        if (napcatBaseUrl !== undefined) updates.napcat_base_url = napcatBaseUrl;
        if (napcatGroupId !== undefined) updates.napcat_group_id = napcatGroupId;
        if (napcatAccessToken !== undefined) updates.napcat_access_token = napcatAccessToken;
        db.updateUser(uin, updates);

        // 如果 Bot 正在运行，更新运行时配置
        const bot = this.bots.get(uin);
        if (bot) {
            if (farmInterval !== undefined) bot.farmInterval = farmInterval;
            if (friendInterval !== undefined) bot.friendInterval = friendInterval;
            if (friendNotifyCooldownSec !== undefined) bot.friendNotifyCooldownSec = normalizeNotifyCooldownSec(friendNotifyCooldownSec, 60);
            if (friendWhitelist !== undefined) bot.setFriendWhitelist(friendWhitelist);
            if (friendActionConfig !== undefined) bot.setFriendActionConfig(friendActionConfig);
            if (preferredSeedId !== undefined) bot.setPreferredSeedId(preferredSeedId);
            if (
                napcatNotifyEnabled !== undefined ||
                napcatNotifyMatureEnabled !== undefined ||
                napcatNotifyHelpEnabled !== undefined ||
                napcatBaseUrl !== undefined ||
                napcatGroupId !== undefined ||
                napcatAccessToken !== undefined
            ) {
                bot.setNapcatNotifyConfig({
                    friendMatureEnabled: napcatNotifyMatureEnabled !== undefined
                        ? napcatNotifyMatureEnabled
                        : napcatNotifyEnabled,
                    friendHelpEnabled: napcatNotifyHelpEnabled !== undefined
                        ? napcatNotifyHelpEnabled
                        : napcatNotifyEnabled,
                    baseUrl: napcatBaseUrl,
                    groupId: napcatGroupId,
                    accessToken: napcatAccessToken,
                });
            }
        }
    }

    updateAccountToggles(uin, toggles = {}) {
        const safeToggles = toggles && typeof toggles === 'object' ? toggles : {};
        db.updateUser(uin, { feature_toggles: JSON.stringify(safeToggles) });

        const bot = this.bots.get(uin);
        if (bot) {
            bot.setFeatureToggles(safeToggles);
            return bot.featureToggles;
        }
        return safeToggles;
    }

    updateAccountFriendConfig(uin, gid, config = {}) {
        const gidNum = Number(gid);
        if (!Number.isFinite(gidNum) || gidNum <= 0) {
            throw new Error('无效的好友 GID');
        }
        const user = db.getUserByUin(uin);
        if (!user) throw new Error('账号不存在');

        const allConfig = parseFriendActionConfig(user.friend_action_config);
        const key = String(Math.trunc(gidNum));
        const prev = allConfig[key] && typeof allConfig[key] === 'object' ? allConfig[key] : {};
        const next = { ...prev };
        if (config.allowSteal !== undefined) next.allowSteal = !!config.allowSteal;
        if (config.allowHelp !== undefined) next.allowHelp = !!config.allowHelp;
        if (config.allowNotify !== undefined) {
            const enabled = !!config.allowNotify;
            next.allowNotifySteal = enabled;
            next.allowNotifyHelp = enabled;
        }
        if (config.allowNotifySteal !== undefined) next.allowNotifySteal = !!config.allowNotifySteal;
        if (config.allowNotifyHelp !== undefined) next.allowNotifyHelp = !!config.allowNotifyHelp;
        allConfig[key] = next;

        db.updateUser(uin, { friend_action_config: JSON.stringify(allConfig) });
        const bot = this.bots.get(uin);
        if (bot) bot.updateFriendActionPermission(key, next);
        return next;
    }

    // ============================================================
    //  服务器启动时自动恢复
    // ============================================================

    async autoStartBots() {
        const users = db.getAutoStartUsers();
        if (users.length === 0) return;
        console.log(`[BotManager] 自动启动 ${users.length} 个账号...`);
        for (const user of users) {
            try {
                const code = db.getSession(user.uin);
                if (code) {
                    const notifyFlags = resolveNapcatNotifyFlags(user);
                    await this._startBot(user.uin, code, {
                        platform: user.platform,
                        farmInterval: user.farm_interval,
                        friendInterval: user.friend_interval,
                        friendNotifyCooldownSec: normalizeNotifyCooldownSec(user.friend_notify_cooldown_sec, 60),
                        friendWhitelist: user.friend_whitelist || '',
                        friendActionConfig: parseFriendActionConfig(user.friend_action_config),
                        dailyStats: parseDailyStats(user.daily_stats),
                        preferredSeedId: user.preferred_seed_id || 0,
                        featureToggles: parseFeatureToggles(user.feature_toggles),
                        napcatNotifyMatureEnabled: notifyFlags.matureEnabled,
                        napcatNotifyHelpEnabled: notifyFlags.helpEnabled,
                        napcatBaseUrl: user.napcat_base_url || '',
                        napcatGroupId: user.napcat_group_id || '',
                        napcatAccessToken: user.napcat_access_token || '',
                    });
                    console.log(`[BotManager] 已启动: ${user.uin} (${user.nickname || '未知'})`);
                }
            } catch (err) {
                console.error(`[BotManager] 自动启动失败 ${user.uin}: ${err.message}`);
            }
        }
    }

    // ============================================================
    //  清理
    // ============================================================

    shutdown() {
        console.log('[BotManager] 关闭所有 Bot...');
        for (const [uin, bot] of this.bots) {
            bot.destroy();
        }
        this.bots.clear();
        this.qrSessions.clear();
    }
}

// 单例
const botManager = new BotManager();

module.exports = { botManager, BotManager };
