/**
 * SQLite 数据库层 - 用户会话与状态持久化
 * 使用 sql.js (纯 JS SQLite, 无需 native 编译)
 */

const initSqlJs = require('sql.js');
const crypto = require('crypto');
const path = require('path');
const fs = require('fs');

// 加密密钥 (生产环境应从环境变量读取)
const ENCRYPTION_KEY = process.env.BOT_ENCRYPT_KEY || 'qq-farm-bot-default-key-32bytes!';
const IV_LENGTH = 16;

// ============ 加密/解密工具 ============

function encrypt(text) {
    if (!text) return '';
    const iv = crypto.randomBytes(IV_LENGTH);
    const key = crypto.scryptSync(ENCRYPTION_KEY, 'salt', 32);
    const cipher = crypto.createCipheriv('aes-256-cbc', key, iv);
    let encrypted = cipher.update(text, 'utf8', 'hex');
    encrypted += cipher.final('hex');
    return iv.toString('hex') + ':' + encrypted;
}

function decrypt(text) {
    if (!text) return '';
    try {
        const parts = text.split(':');
        const iv = Buffer.from(parts.shift(), 'hex');
        const key = crypto.scryptSync(ENCRYPTION_KEY, 'salt', 32);
        const encryptedText = parts.join(':');
        const decipher = crypto.createDecipheriv('aes-256-cbc', key, iv);
        let decrypted = decipher.update(encryptedText, 'hex', 'utf8');
        decrypted += decipher.final('utf8');
        return decrypted;
    } catch (e) {
        return '';
    }
}

// ============ 数据库核心 ============

const DB_PATH = path.join(__dirname, '..', 'data', 'farm-bot.db');
let db = null;

/** 将 sql.js 查询结果转为对象数组 */
function queryAll(sql, params = []) {
    const stmt = db.prepare(sql);
    if (params.length) stmt.bind(params);
    const rows = [];
    while (stmt.step()) {
        rows.push(stmt.getAsObject());
    }
    stmt.free();
    return rows;
}

/** 查询单行 */
function queryOne(sql, params = []) {
    const rows = queryAll(sql, params);
    return rows.length > 0 ? rows[0] : null;
}

/** 执行写操作 */
function run(sql, params = []) {
    db.run(sql, params);
}

/** 持久化到磁盘 */
function saveToFile() {
    if (!db) return;
    const dir = path.dirname(DB_PATH);
    if (!fs.existsSync(dir)) fs.mkdirSync(dir, { recursive: true });
    const data = db.export();
    fs.writeFileSync(DB_PATH, Buffer.from(data));
}

// 定期自动保存 (每 30 秒)
let saveTimer = null;

async function initDatabase() {
    const SQL = await initSqlJs();

    // 如果数据库文件已存在则加载
    if (fs.existsSync(DB_PATH)) {
        const fileBuffer = fs.readFileSync(DB_PATH);
        db = new SQL.Database(fileBuffer);
    } else {
        const dir = path.dirname(DB_PATH);
        if (!fs.existsSync(dir)) fs.mkdirSync(dir, { recursive: true });
        db = new SQL.Database();
    }

    // 创建用户表
    db.run(`
        CREATE TABLE IF NOT EXISTS users (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            uin TEXT UNIQUE NOT NULL,
            nickname TEXT DEFAULT '',
            gid INTEGER DEFAULT 0,
            level INTEGER DEFAULT 0,
            gold INTEGER DEFAULT 0,
            exp INTEGER DEFAULT 0,
            status TEXT DEFAULT 'stopped',
            session_data TEXT DEFAULT '',
            platform TEXT DEFAULT 'qq',
            farm_interval INTEGER DEFAULT 10000,
            friend_interval INTEGER DEFAULT 10000,
            friend_notify_cooldown_sec INTEGER DEFAULT 60,
            friend_whitelist TEXT DEFAULT '',
            friend_action_config TEXT DEFAULT '',
            napcat_notify_enabled INTEGER DEFAULT 0,
            napcat_notify_mature_enabled INTEGER DEFAULT 0,
            napcat_notify_help_enabled INTEGER DEFAULT 0,
            napcat_base_url TEXT DEFAULT '',
            napcat_group_id TEXT DEFAULT '',
            napcat_access_token TEXT DEFAULT '',
            feature_toggles TEXT DEFAULT '',
            daily_stats TEXT DEFAULT '',
            auto_start INTEGER DEFAULT 0,
            last_login_at TEXT,
            created_at TEXT DEFAULT (datetime('now','localtime')),
            updated_at TEXT DEFAULT (datetime('now','localtime'))
        )
    `);

    // 创建日志表
    db.run(`
        CREATE TABLE IF NOT EXISTS bot_logs (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_uin TEXT NOT NULL,
            tag TEXT DEFAULT '',
            message TEXT DEFAULT '',
            level TEXT DEFAULT 'info',
            created_at TEXT DEFAULT (datetime('now','localtime'))
        )
    `);

    // 创建管理用户表 (Web 登录用)
    db.run(`
        CREATE TABLE IF NOT EXISTS admin_users (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            username TEXT UNIQUE NOT NULL,
            password_hash TEXT NOT NULL,
            role TEXT DEFAULT 'user',
            allowed_uins TEXT DEFAULT '',
            created_at TEXT DEFAULT (datetime('now','localtime'))
        )
    `);

    // 索引
    db.run(`CREATE INDEX IF NOT EXISTS idx_users_uin ON users(uin)`);
    db.run(`CREATE INDEX IF NOT EXISTS idx_logs_uin ON bot_logs(user_uin)`);
    db.run(`CREATE INDEX IF NOT EXISTS idx_logs_created ON bot_logs(created_at)`);

    // 创建公告表
    db.run(`
        CREATE TABLE IF NOT EXISTS announcements (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            title TEXT DEFAULT '',
            content TEXT DEFAULT '',
            enabled INTEGER DEFAULT 1,
            updated_at TEXT DEFAULT (datetime('now','localtime')),
            created_at TEXT DEFAULT (datetime('now','localtime'))
        )
    `);

    // 迁移: 添加 preferred_seed_id 列
    try { db.run(`ALTER TABLE users ADD COLUMN preferred_seed_id INTEGER DEFAULT 0`); } catch (e) { /* 列已存在 */ }
    try { db.run(`ALTER TABLE users ADD COLUMN friend_whitelist TEXT DEFAULT ''`); } catch (e) { /* 列已存在 */ }
    try { db.run(`ALTER TABLE users ADD COLUMN friend_notify_cooldown_sec INTEGER DEFAULT 60`); } catch (e) { /* 列已存在 */ }
    try { db.run(`ALTER TABLE users ADD COLUMN friend_action_config TEXT DEFAULT ''`); } catch (e) { /* 列已存在 */ }
    try { db.run(`ALTER TABLE users ADD COLUMN napcat_notify_enabled INTEGER DEFAULT 0`); } catch (e) { /* 列已存在 */ }
    try { db.run(`ALTER TABLE users ADD COLUMN napcat_notify_mature_enabled INTEGER DEFAULT 0`); } catch (e) { /* 列已存在 */ }
    try { db.run(`ALTER TABLE users ADD COLUMN napcat_notify_help_enabled INTEGER DEFAULT 0`); } catch (e) { /* 列已存在 */ }
    try { db.run(`ALTER TABLE users ADD COLUMN napcat_base_url TEXT DEFAULT ''`); } catch (e) { /* 列已存在 */ }
    try { db.run(`ALTER TABLE users ADD COLUMN napcat_group_id TEXT DEFAULT ''`); } catch (e) { /* 列已存在 */ }
    try { db.run(`ALTER TABLE users ADD COLUMN napcat_access_token TEXT DEFAULT ''`); } catch (e) { /* 列已存在 */ }
    try { db.run(`ALTER TABLE users ADD COLUMN feature_toggles TEXT DEFAULT ''`); } catch (e) { /* 列已存在 */ }
    try { db.run(`ALTER TABLE users ADD COLUMN daily_stats TEXT DEFAULT ''`); } catch (e) { /* 列已存在 */ }

    // 迁移: 添加 feature_toggles 列
    try { db.run(`ALTER TABLE users ADD COLUMN feature_toggles TEXT DEFAULT ''`); } catch (e) { /* 列已存在 */ }

    saveToFile();

    // 自动保存定时器
    saveTimer = setInterval(saveToFile, 30000);

    console.log('[DB] SQLite 数据库已初始化');
    return db;
}

// ============ 用户 CRUD ============

function getAllUsers() {
    return queryAll('SELECT * FROM users ORDER BY created_at DESC');
}

function getUserByUin(uin) {
    return queryOne('SELECT * FROM users WHERE uin = ?', [uin]);
}

function getUserById(id) {
    return queryOne('SELECT * FROM users WHERE id = ?', [id]);
}

function createUser({ uin, nickname = '', platform = 'qq', farmInterval = 10000, friendInterval = 10000 }) {
    run(`INSERT INTO users (uin, nickname, platform, farm_interval, friend_interval) VALUES (?, ?, ?, ?, ?)`,
        [uin, nickname, platform, farmInterval, friendInterval]);
    saveToFile();
    return getUserByUin(uin);
}

function updateUser(uin, updates) {
    const fields = [];
    const values = [];
    for (const [key, value] of Object.entries(updates)) {
        if (key === 'session_data') {
            fields.push(`${key} = ?`);
            values.push(encrypt(value));
        } else {
            fields.push(`${key} = ?`);
            values.push(value);
        }
    }
    fields.push("updated_at = datetime('now','localtime')");
    values.push(uin);
    run(`UPDATE users SET ${fields.join(', ')} WHERE uin = ?`, values);
    saveToFile();
    return getUserByUin(uin);
}

function updateUserStatus(uin, status) {
    return updateUser(uin, { status });
}

function updateUserGameState(uin, { nickname, gid, level, gold, exp }) {
    const updates = {};
    if (nickname !== undefined) updates.nickname = nickname;
    if (gid !== undefined) updates.gid = gid;
    if (level !== undefined) updates.level = level;
    if (gold !== undefined) updates.gold = gold;
    if (exp !== undefined) updates.exp = exp;
    if (Object.keys(updates).length > 0) {
        return updateUser(uin, updates);
    }
}

function saveSession(uin, sessionData) {
    return updateUser(uin, { session_data: sessionData });
}

function getSession(uin) {
    const user = getUserByUin(uin);
    if (!user || !user.session_data) return null;
    return decrypt(user.session_data);
}

function deleteUser(uin) {
    run('DELETE FROM users WHERE uin = ?', [uin]);
    run('DELETE FROM bot_logs WHERE user_uin = ?', [uin]);
    saveToFile();
}

// ============ 日志持久化 ============

function addLog(uin, tag, message, level = 'info') {
    run(`INSERT INTO bot_logs (user_uin, tag, message, level) VALUES (?, ?, ?, ?)`,
        [uin, tag, message, level]);
}

function getRecentLogs(uin, limit = 100) {
    return queryAll(
        `SELECT * FROM bot_logs WHERE user_uin = ? ORDER BY created_at DESC LIMIT ?`,
        [uin, limit]
    ).reverse();
}

function cleanOldLogs(daysToKeep = 7) {
    run(`DELETE FROM bot_logs WHERE created_at < datetime('now', '-${daysToKeep} days', 'localtime')`);
    saveToFile();
}

// ============ 自动启动用户列表 ============

function getAutoStartUsers() {
    return queryAll('SELECT * FROM users WHERE auto_start = 1 AND session_data != ""');
}

// ============ 管理用户 (Web 登录) ============

function getAdminUser(username) {
    return queryOne('SELECT * FROM admin_users WHERE username = ?', [username]);
}

function getAdminUserById(id) {
    return queryOne('SELECT * FROM admin_users WHERE id = ?', [id]);
}

function getAllAdminUsers() {
    return queryAll('SELECT id, username, role, allowed_uins, created_at FROM admin_users ORDER BY created_at');
}

function createAdminUser({ username, passwordHash, role = 'user', allowedUins = '' }) {
    run(`INSERT INTO admin_users (username, password_hash, role, allowed_uins) VALUES (?, ?, ?, ?)`,
        [username, passwordHash, role, allowedUins]);
    saveToFile();
    return getAdminUser(username);
}

function updateAdminUser(id, updates) {
    const fields = [];
    const values = [];
    for (const [key, value] of Object.entries(updates)) {
        fields.push(`${key} = ?`);
        values.push(value);
    }
    if (fields.length === 0) return;
    values.push(id);
    run(`UPDATE admin_users SET ${fields.join(', ')} WHERE id = ?`, values);
    saveToFile();
}

function deleteAdminUser(id) {
    run('DELETE FROM admin_users WHERE id = ?', [id]);
    saveToFile();
}

// ============ 公告 ============

function getAnnouncement() {
    return queryOne('SELECT * FROM announcements WHERE enabled = 1 ORDER BY updated_at DESC LIMIT 1');
}

function saveAnnouncement({ title, content }) {
    const existing = queryOne('SELECT * FROM announcements LIMIT 1');
    if (existing) {
        run(`UPDATE announcements SET title = ?, content = ?, enabled = 1, updated_at = datetime('now','localtime') WHERE id = ?`,
            [title, content, existing.id]);
    } else {
        run(`INSERT INTO announcements (title, content) VALUES (?, ?)`, [title, content]);
    }
    saveToFile();
    return getAnnouncement();
}

/** 确保存在默认管理员 (首次运行时) */
function ensureDefaultAdmin() {
    const admin = getAdminUser('admin');
    if (!admin) {
        // 默认密码: admin123 (sha256)
        const hash = crypto.createHash('sha256').update('admin123').digest('hex');
        createAdminUser({ username: 'admin', passwordHash: hash, role: 'admin' });
        console.log('[DB] 已创建默认管理员 admin / admin123');
    }
}

function closeDatabase() {
    if (saveTimer) { clearInterval(saveTimer); saveTimer = null; }
    if (db) {
        saveToFile();
        db.close();
        db = null;
    }
}

module.exports = {
    initDatabase,
    closeDatabase,
    getAllUsers,
    getUserByUin,
    getUserById,
    createUser,
    updateUser,
    updateUserStatus,
    updateUserGameState,
    saveSession,
    getSession,
    deleteUser,
    addLog,
    getRecentLogs,
    cleanOldLogs,
    getAutoStartUsers,
    encrypt,
    decrypt,
    // 管理用户
    getAdminUser,
    getAdminUserById,
    getAllAdminUsers,
    createAdminUser,
    updateAdminUser,
    deleteAdminUser,
    ensureDefaultAdmin,
    // 公告
    getAnnouncement,
    saveAnnouncement,
};
