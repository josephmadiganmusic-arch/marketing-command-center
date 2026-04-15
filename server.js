const express = require('express');
const fs = require('fs');
const path = require('path');
const crypto = require('crypto');
const session = require('express-session');
const FileStore = require('session-file-store')(session);
const bcrypt = require('bcryptjs');
const initSqlJs = require('sql.js');
const Stripe = require('stripe');
const { Resend } = require('resend');
// ExcelJS is lazy-loaded inside the soundexchange-xlsx endpoint to avoid
// adding ~76MB RSS at startup (would OOM small Railway containers).

const app = express();
const PORT = process.env.PORT || 3000;

// --- Database Setup (sql.js — pure JS, no native build needed) ---
const DATA_DIR = process.env.DATA_DIR || path.join(__dirname, 'data');
if (!fs.existsSync(DATA_DIR)) fs.mkdirSync(DATA_DIR, { recursive: true });

const DB_PATH = path.join(DATA_DIR, 'users.db');
let db; // initialized in startServer()

// sql.js helper: wraps db methods to match better-sqlite3-style API
const dbHelpers = {
  exec(sql) { db.run(sql); },
  prepare(sql) {
    return {
      get(...params) {
        const stmt = db.prepare(sql);
        stmt.bind(params);
        if (stmt.step()) {
          const row = stmt.getAsObject();
          stmt.free();
          return row;
        }
        stmt.free();
        return undefined;
      },
      all(...params) {
        const results = [];
        const stmt = db.prepare(sql);
        stmt.bind(params);
        while (stmt.step()) results.push(stmt.getAsObject());
        stmt.free();
        return results;
      },
      run(...params) {
        db.run(sql, params);
        const lastId = db.exec("SELECT last_insert_rowid() as id")[0]?.values[0]?.[0];
        const changes = db.getRowsModified();
        saveDb();
        return { lastInsertRowid: lastId, changes };
      }
    };
  },
  transaction(fn) {
    return (...args) => {
      db.run("BEGIN");
      try { fn(...args); db.run("COMMIT"); saveDb(); }
      catch(e) { db.run("ROLLBACK"); throw e; }
    };
  }
};

let _saveTimer = null;
function flushDbNow() {
  if (!db) return;
  try {
    const data = db.export();
    const buf = Buffer.from(data);
    const tmpPath = DB_PATH + '.tmp';
    fs.writeFileSync(tmpPath, buf);
    fs.renameSync(tmpPath, DB_PATH);

    // --- Data Safety: post-write integrity verification ---
    // Catches silent filesystem corruption, disk-full truncation, and rename failures.
    const stat = fs.statSync(DB_PATH);
    if (stat.size === 0) {
      console.error('[FLUSH] CRITICAL: DB file is 0 bytes after write — attempting recovery write');
      fs.writeFileSync(DB_PATH, buf);
    } else if (stat.size !== buf.length) {
      console.error(`[FLUSH] CRITICAL: DB size mismatch — expected ${buf.length}, got ${stat.size} — attempting recovery write`);
      fs.writeFileSync(tmpPath, buf);
      fs.renameSync(tmpPath, DB_PATH);
    }
  } catch(e) { console.error('DB save error:', e.message); }
}
function saveDb() {
  clearTimeout(_saveTimer);
  _saveTimer = setTimeout(flushDbNow, 100);
}
// Flush pending writes on graceful shutdown so the 100ms debounce window
// can never silently drop a batch of saves on SIGTERM (Railway redeploy). (H9)
function shutdown(signal) {
  console.log('[SHUTDOWN] Received', signal, '— flushing DB');
  clearTimeout(_saveTimer);
  flushDbNow();
  releaseInstanceLock();
  process.exit(0);
}
process.on('SIGTERM', () => shutdown('SIGTERM'));
process.on('SIGINT', () => shutdown('SIGINT'));

// --- Data Safety: operation journal helper ---
// Append-only audit trail for critical mutations. Never call DELETE/TRUNCATE
// on this table. The journal survives soft deletes, backups, and restores.
function logOperation(req, action, entityType, entityId, detail) {
  try {
    const actorId = req && req.session ? req.session.userId : null;
    const actorEmail = req && req.user ? req.user.email : null;
    const ip = req ? (req.headers['x-forwarded-for'] || req.socket.remoteAddress || '') : '';
    dbHelpers.prepare(`
      INSERT INTO operation_journal (actor_id, actor_email, action, entity_type, entity_id, detail, ip)
      VALUES (?, ?, ?, ?, ?, ?, ?)
    `).run(
      actorId || null,
      actorEmail || null,
      action,
      entityType || null,
      entityId != null ? String(entityId) : null,
      typeof detail === 'object' ? JSON.stringify(detail) : (detail || null),
      ip.split(',')[0].trim() || null
    );
  } catch (e) {
    // Journal failures must NEVER break the primary operation. Log and continue.
    console.error('[JOURNAL] logOperation failed:', e.message);
  }
}

// --- Single-instance boot lock (C4) ---
// sql.js + session-file-store + in-memory rate limits all assume a single
// Node process. A second replica would silently clobber the other's writes
// on every debounced flush (last-writer-wins on the whole DB file). This
// lock fails boot loudly if another instance is already running against
// the same DATA_DIR. On crash the lock is stale — delete it manually with
// a clear operator message before restarting.
const INSTANCE_LOCK_PATH = path.join(DATA_DIR, '.instance.lock');
const LOCK_STALE_MS = 90 * 1000;          // lock older than this is considered stale
const LOCK_RETRY_TOTAL_MS = 120 * 1000;   // total time we'll wait for an overlapping instance to release
const LOCK_RETRY_INTERVAL_MS = 3 * 1000;
let _instanceLockHeld = false;
let _instanceLockHeartbeat = null;
function _writeLockFile() {
  const payload = JSON.stringify({
    pid: process.pid,
    started: new Date().toISOString(),
    heartbeat: new Date().toISOString(),
    host: process.env.RAILWAY_REPLICA_ID || process.env.HOSTNAME || 'unknown'
  });
  const fd = fs.openSync(INSTANCE_LOCK_PATH, 'wx'); // O_EXCL
  fs.writeSync(fd, payload);
  fs.closeSync(fd);
}
function _lockIsStale() {
  try {
    const raw = fs.readFileSync(INSTANCE_LOCK_PATH, 'utf8');
    let parsed = null;
    try { parsed = JSON.parse(raw); } catch(_) {}

    // Fast path: if the lock was written by a different container (different
    // RAILWAY_REPLICA_ID or HOSTNAME), the old process is guaranteed dead —
    // Railway kills the old container before the new one mounts the volume.
    // No need to wait 90 seconds.
    if (parsed && parsed.host) {
      const myHost = process.env.RAILWAY_REPLICA_ID || process.env.HOSTNAME || 'unknown';
      if (parsed.host !== myHost && parsed.host !== 'unknown' && myHost !== 'unknown') {
        console.log('[BOOT] Lock belongs to different container (' + parsed.host + ' vs ' + myHost + '), treating as stale');
        return true;
      }
    }

    // Same host — check if the PID is still alive (handles crash/OOM on same container)
    if (parsed && parsed.pid) {
      try {
        process.kill(parsed.pid, 0); // signal 0 = existence check, doesn't kill
      } catch (e) {
        if (e.code === 'ESRCH') {
          console.log('[BOOT] Lock holder PID ' + parsed.pid + ' is not running, treating as stale');
          return true;
        }
      }
    }

    // Fallback: time-based staleness
    const heartbeatTs = parsed && parsed.heartbeat ? Date.parse(parsed.heartbeat) : NaN;
    const mtimeMs = fs.statSync(INSTANCE_LOCK_PATH).mtimeMs;
    const freshest = Math.max(
      isNaN(heartbeatTs) ? 0 : heartbeatTs,
      mtimeMs || 0
    );
    return (Date.now() - freshest) > LOCK_STALE_MS;
  } catch (_) { return false; }
}
function acquireInstanceLock() {
  // Retry loop handles Railway zero-downtime deploys: the new instance boots
  // while the old one still holds the lock. Old instance gets SIGTERM around
  // the same time, releases the lock in its shutdown handler, and we acquire
  // on the next retry. Stale-lock detection handles kill -9 / OOM / host reap
  // where the shutdown handler never ran.
  const deadline = Date.now() + LOCK_RETRY_TOTAL_MS;
  let attempt = 0;
  while (true) {
    attempt++;
    try {
      _writeLockFile();
      _instanceLockHeld = true;
      console.log('[BOOT] Instance lock acquired at', INSTANCE_LOCK_PATH, '(attempt ' + attempt + ')');
      // Heartbeat so a future zero-downtime boot can see we're still alive
      // vs. stale lock. Cheap — one stat + one write every 30s.
      _instanceLockHeartbeat = setInterval(() => {
        try {
          const payload = JSON.stringify({
            pid: process.pid,
            started: new Date().toISOString(),
            heartbeat: new Date().toISOString(),
            host: process.env.RAILWAY_REPLICA_ID || process.env.HOSTNAME || 'unknown'
          });
          fs.writeFileSync(INSTANCE_LOCK_PATH, payload);
        } catch(_) {}
      }, 30 * 1000);
      _instanceLockHeartbeat.unref();
      return;
    } catch (e) {
      if (e.code !== 'EEXIST') throw e;
      // Lock file exists — is it stale?
      if (_lockIsStale()) {
        let holder = '(unreadable)';
        try { holder = fs.readFileSync(INSTANCE_LOCK_PATH, 'utf8'); } catch(_) {}
        console.warn('[BOOT] Stale lock detected (age > ' + LOCK_STALE_MS + 'ms), force-releasing. Prior holder:', holder);
        try { fs.unlinkSync(INSTANCE_LOCK_PATH); } catch(_) {}
        continue; // retry immediately
      }
      // Fresh lock — another instance is genuinely running (likely the
      // outgoing zero-downtime instance). Wait and retry.
      if (Date.now() >= deadline) {
        let holder = '(unreadable)';
        try { holder = fs.readFileSync(INSTANCE_LOCK_PATH, 'utf8'); } catch(_) {}
        console.error('[BOOT] FATAL: could not acquire instance lock after', LOCK_RETRY_TOTAL_MS, 'ms');
        console.error('[BOOT] Lock holder:', holder);
        console.error('[BOOT] sql.js + file sessions require single-instance operation.');
        console.error('[BOOT] If you are scaling up: DO NOT. Set Railway replicas = 1.');
        console.error('[BOOT] If the previous instance is hung: `rm ' + INSTANCE_LOCK_PATH + '` on the volume and redeploy.');
        process.exit(1);
      }
      console.log('[BOOT] Lock held by another instance, retrying in ' + (LOCK_RETRY_INTERVAL_MS/1000) + 's (attempt ' + attempt + ')');
      // Synchronous sleep — we're single-threaded and not accepting traffic yet.
      const waitUntil = Date.now() + LOCK_RETRY_INTERVAL_MS;
      while (Date.now() < waitUntil) { /* busy-wait — boot only, not hot path */ }
    }
  }
}
function releaseInstanceLock() {
  if (!_instanceLockHeld) return;
  if (_instanceLockHeartbeat) { clearInterval(_instanceLockHeartbeat); _instanceLockHeartbeat = null; }
  try { fs.unlinkSync(INSTANCE_LOCK_PATH); } catch(_) {}
  _instanceLockHeld = false;
}
process.on('exit', () => releaseInstanceLock());

// --- DB backup strategy (H2) ---
// sql.js writes the full DB on every debounced flush, so the hot file is
// always a complete snapshot — cp is a safe backup. Strategy:
//   hourly snapshots for the last 48h (rolling)
//   daily snapshots for the last 30d (one per day, kept longest-running)
// Restore is best-effort on corrupt-DB boot: pick newest backup that
// loads without throwing a sanity check.
const BACKUP_DIR = path.join(DATA_DIR, 'backups');
const BACKUP_HOURLY_KEEP = 48;
const BACKUP_DAILY_KEEP = 30;
function ensureBackupDir() {
  try { if (!fs.existsSync(BACKUP_DIR)) fs.mkdirSync(BACKUP_DIR, { recursive: true }); } catch(_) {}
}
function snapshotDbNow(label) {
  if (!fs.existsSync(DB_PATH)) return null;
  ensureBackupDir();
  const now = new Date();
  const stamp = now.toISOString().replace(/[:.]/g, '-').replace('T', '_').slice(0, 19);
  const kind = label || 'hourly';
  const dest = path.join(BACKUP_DIR, `users.db.${kind}.${stamp}.bak`);
  try {
    // Flush any in-memory state first so the snapshot is up-to-date.
    flushDbNow();
    fs.copyFileSync(DB_PATH, dest);
    return dest;
  } catch (e) {
    console.error('[BACKUP] snapshot failed:', e.message);
    return null;
  }
}
function pruneBackups() {
  try {
    ensureBackupDir();
    const files = fs.readdirSync(BACKUP_DIR)
      .filter(f => f.startsWith('users.db.') && f.endsWith('.bak'))
      .map(f => ({ name: f, full: path.join(BACKUP_DIR, f), mtime: fs.statSync(path.join(BACKUP_DIR, f)).mtime }))
      .sort((a, b) => b.mtime - a.mtime);

    const hourly = files.filter(f => f.name.includes('.hourly.'));
    const daily = files.filter(f => f.name.includes('.daily.'));

    // Keep the N newest hourly backups, delete the rest
    for (const stale of hourly.slice(BACKUP_HOURLY_KEEP)) {
      try { fs.unlinkSync(stale.full); } catch(_) {}
    }
    for (const stale of daily.slice(BACKUP_DAILY_KEEP)) {
      try { fs.unlinkSync(stale.full); } catch(_) {}
    }
  } catch (e) {
    console.error('[BACKUP] prune failed:', e.message);
  }
}
function startBackupTimers() {
  // Hourly snapshot + prune. First snapshot runs 10 minutes after boot
  // (not immediately, to avoid piling up on every deploy).
  setTimeout(() => {
    snapshotDbNow('hourly');
    pruneBackups();
  }, 10 * 60 * 1000);
  setInterval(() => {
    snapshotDbNow('hourly');
    pruneBackups();
  }, 60 * 60 * 1000).unref();

  // Daily snapshot at next UTC midnight, then every 24h.
  const now = new Date();
  const nextMidnight = new Date(Date.UTC(now.getUTCFullYear(), now.getUTCMonth(), now.getUTCDate() + 1, 0, 5, 0));
  const msUntilMidnight = nextMidnight.getTime() - now.getTime();
  setTimeout(() => {
    snapshotDbNow('daily');
    pruneBackups();
    setInterval(() => {
      snapshotDbNow('daily');
      pruneBackups();
    }, 24 * 60 * 60 * 1000).unref();
  }, msUntilMidnight).unref();
}
// Attempt to restore from the most recent backup that actually loads.
// Called from the corrupt-DB recovery path in startServer() before the
// fresh-DB fallback fires. Returns a loaded sql.js db on success, or null.
function tryRestoreFromBackup(SQL) {
  try {
    ensureBackupDir();
    const files = fs.readdirSync(BACKUP_DIR)
      .filter(f => f.startsWith('users.db.') && f.endsWith('.bak'))
      .map(f => ({ full: path.join(BACKUP_DIR, f), mtime: fs.statSync(path.join(BACKUP_DIR, f)).mtime }))
      .sort((a, b) => b.mtime - a.mtime);
    for (const candidate of files) {
      try {
        const buf = fs.readFileSync(candidate.full);
        const restored = new SQL.Database(buf);
        restored.exec("SELECT count(*) FROM sqlite_master"); // sanity check
        console.error('[RESTORE] Restored from backup:', candidate.full);
        // Write restored DB into DB_PATH so the app continues from this state.
        fs.copyFileSync(candidate.full, DB_PATH);
        return restored;
      } catch (e) {
        console.error('[RESTORE] Backup unusable:', candidate.full, '-', e.message);
      }
    }
  } catch (e) {
    console.error('[RESTORE] Scan failed:', e.message);
  }
  return null;
}

function initDb() {
  db.run(`
    CREATE TABLE IF NOT EXISTS users (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      email TEXT UNIQUE NOT NULL,
      password TEXT NOT NULL,
      role TEXT DEFAULT 'user',
      stripe_customer_id TEXT,
      stripe_subscription_id TEXT,
      subscription_status TEXT DEFAULT 'none',
      trial_ends_at TEXT,
      email_verified INTEGER DEFAULT 0,
      verification_token TEXT,
      verification_expires TEXT,
      created_at TEXT DEFAULT (datetime('now'))
    )
  `);

  // Add email verification columns if they don't exist (migration for existing DB).
  // Only swallow "duplicate column" errors — surface anything else so a real
  // schema problem doesn't get silently masked.
  const isDupColErr = e => e && /duplicate column/i.test(String(e.message || e));
  try { db.run("ALTER TABLE users ADD COLUMN email_verified INTEGER DEFAULT 0"); } catch(e) { if (!isDupColErr(e)) throw e; }
  try { db.run("ALTER TABLE users ADD COLUMN verification_token TEXT"); } catch(e) { if (!isDupColErr(e)) throw e; }
  try { db.run("ALTER TABLE users ADD COLUMN verification_expires TEXT"); } catch(e) { if (!isDupColErr(e)) throw e; }
  // Elite tier columns. subscription_tier is orthogonal to subscription_status —
  // 'pro' for monthly, 'elite' / 'elite_plus' for annual concierge tiers.
  // onboarding_completed gates the manual-work flow: paid Elite users get app
  // access immediately but Joseph can't start distribution/registration/outreach
  // until they submit the credential form.
  try { db.run("ALTER TABLE users ADD COLUMN subscription_tier TEXT DEFAULT 'pro'"); } catch(e) { if (!isDupColErr(e)) throw e; }
  try { db.run("ALTER TABLE users ADD COLUMN onboarding_completed INTEGER DEFAULT 0"); } catch(e) { if (!isDupColErr(e)) throw e; }

  // Mark existing admin accounts as verified
  db.run("UPDATE users SET email_verified = 1 WHERE role = 'admin'");

  db.run(`
    CREATE TABLE IF NOT EXISTS support_tickets (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      user_id INTEGER NOT NULL,
      user_email TEXT NOT NULL,
      subject TEXT NOT NULL,
      message TEXT NOT NULL,
      status TEXT DEFAULT 'open',
      ai_response TEXT,
      admin_notes TEXT,
      escalated INTEGER DEFAULT 0,
      created_at TEXT DEFAULT (datetime('now')),
      updated_at TEXT DEFAULT (datetime('now')),
      FOREIGN KEY (user_id) REFERENCES users(id)
    )
  `);

  db.run(`
    CREATE TABLE IF NOT EXISTS user_data (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      user_id INTEGER NOT NULL,
      key TEXT NOT NULL,
      value TEXT,
      updated_at TEXT DEFAULT (datetime('now')),
      UNIQUE(user_id, key)
    )
  `);

  // --- Gamification Tables ---
  db.run(`
    CREATE TABLE IF NOT EXISTS user_xp (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      user_id INTEGER NOT NULL,
      total_xp INTEGER DEFAULT 0,
      level INTEGER DEFAULT 1,
      current_streak INTEGER DEFAULT 0,
      longest_streak INTEGER DEFAULT 0,
      last_active_date TEXT,
      releases_completed INTEGER DEFAULT 0,
      tasks_completed INTEGER DEFAULT 0,
      emails_generated INTEGER DEFAULT 0,
      research_runs INTEGER DEFAULT 0,
      playlists_submitted INTEGER DEFAULT 0,
      campaigns_generated INTEGER DEFAULT 0,
      content_copied INTEGER DEFAULT 0,
      logins_total INTEGER DEFAULT 0,
      created_at TEXT DEFAULT (datetime('now')),
      UNIQUE(user_id)
    )
  `);

  db.run(`
    CREATE TABLE IF NOT EXISTS user_achievements (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      user_id INTEGER NOT NULL,
      achievement_id TEXT NOT NULL,
      unlocked_at TEXT DEFAULT (datetime('now')),
      UNIQUE(user_id, achievement_id)
    )
  `);

  db.run(`
    CREATE TABLE IF NOT EXISTS xp_log (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      user_id INTEGER NOT NULL,
      action TEXT NOT NULL,
      xp_amount INTEGER NOT NULL,
      description TEXT,
      created_at TEXT DEFAULT (datetime('now'))
    )
  `);

  // Per-user, per-day API usage tracking. Prevents the Claude proxy from
  // becoming a free Anthropic-credit faucet for trial users (or compromised
  // paying ones). One row per (user, UTC day, provider).
  // Indexes for hot paths — keep query plans cheap as the tables grow. (M6)
  db.run('CREATE INDEX IF NOT EXISTS idx_xp_log_user ON xp_log(user_id, created_at DESC)');
  db.run('CREATE INDEX IF NOT EXISTS idx_user_data_user ON user_data(user_id)');
  db.run('CREATE INDEX IF NOT EXISTS idx_tickets_user ON support_tickets(user_id)');
  db.run('CREATE INDEX IF NOT EXISTS idx_user_xp_user ON user_xp(user_id)');
  db.run('CREATE INDEX IF NOT EXISTS idx_user_ach_user ON user_achievements(user_id)');

  // Stripe webhook idempotency: Stripe retries webhooks on transient errors,
  // so we record every processed event ID and bail early on duplicates. (H5)
  db.run(`
    CREATE TABLE IF NOT EXISTS stripe_events (
      id TEXT PRIMARY KEY,
      type TEXT,
      processed_at TEXT DEFAULT (datetime('now'))
    )
  `);

  db.run(`
    CREATE TABLE IF NOT EXISTS api_usage (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      user_id INTEGER NOT NULL,
      provider TEXT NOT NULL,
      day TEXT NOT NULL,
      input_tokens INTEGER DEFAULT 0,
      output_tokens INTEGER DEFAULT 0,
      requests INTEGER DEFAULT 0,
      UNIQUE(user_id, provider, day)
    )
  `);

  // Encrypted credential storage for Elite/Elite Plus customers. Joseph needs
  // distribution-account logins (DistroKid/TuneCore/etc) and social-account
  // creds to do the manual concierge work. Stored as AES-256-GCM blobs;
  // decryption only happens via the admin-only route. data_encrypted, iv, and
  // auth_tag are all hex-encoded.
  db.run(`
    CREATE TABLE IF NOT EXISTS elite_onboarding (
      user_id INTEGER PRIMARY KEY,
      tier TEXT NOT NULL,
      data_encrypted TEXT NOT NULL,
      iv TEXT NOT NULL,
      auth_tag TEXT NOT NULL,
      submitted_at TEXT DEFAULT (datetime('now')),
      updated_at TEXT DEFAULT (datetime('now')),
      FOREIGN KEY (user_id) REFERENCES users(id)
    )
  `);

  // Audit log for credential decrypts. Every admin view of the plaintext
  // creds is recorded so we have a paper trail if anything is ever disputed.
  db.run(`
    CREATE TABLE IF NOT EXISTS elite_onboarding_access_log (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      user_id INTEGER NOT NULL,
      admin_id INTEGER NOT NULL,
      admin_email TEXT,
      accessed_at TEXT DEFAULT (datetime('now'))
    )
  `);

  // Redemption Release one-time service. Artists pay $49.99 to have Joseph
  // manually clean up an old release (PRO registration, SoundExchange, metadata,
  // re-positioning). Row is created on form submit (status='pending'), updated
  // to 'paid' by the Stripe webhook, and marked 'completed' by admin once work
  // is done. All fields are public-ish release metadata — nothing sensitive,
  // so no encryption.
  db.run(`
    CREATE TABLE IF NOT EXISTS redemption_requests (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      user_id INTEGER NOT NULL,
      release_title TEXT NOT NULL,
      artist_name TEXT,
      dsp_link TEXT,
      original_distributor TEXT,
      original_release_date TEXT,
      still_live TEXT,
      what_went_wrong TEXT,
      extra_notes TEXT,
      status TEXT DEFAULT 'pending',
      stripe_session_id TEXT,
      stripe_payment_intent_id TEXT,
      admin_notes TEXT,
      created_at TEXT DEFAULT (datetime('now')),
      paid_at TEXT,
      completed_at TEXT,
      FOREIGN KEY (user_id) REFERENCES users(id)
    )
  `);
  db.run('CREATE INDEX IF NOT EXISTS idx_redemption_user ON redemption_requests(user_id)');
  db.run('CREATE INDEX IF NOT EXISTS idx_redemption_status ON redemption_requests(status)');
  // Registration-focused fields added after the initial table ship. The
  // service is mainly about cleaning up releases that were never registered
  // (or only partially registered) with royalty collection services, so we
  // need to capture which collection services are missing and the songwriter
  // metadata required to register them. Tolerant of duplicate-column errors
  // for re-runs / fresh DBs that already have these from CREATE TABLE.
  try { db.run("ALTER TABLE redemption_requests ADD COLUMN isrc TEXT"); } catch(e) { if (!isDupColErr(e)) throw e; }
  try { db.run("ALTER TABLE redemption_requests ADD COLUMN songwriter_names TEXT"); } catch(e) { if (!isDupColErr(e)) throw e; }
  try { db.run("ALTER TABLE redemption_requests ADD COLUMN pro_affiliation TEXT"); } catch(e) { if (!isDupColErr(e)) throw e; }
  try { db.run("ALTER TABLE redemption_requests ADD COLUMN registrations_missing TEXT"); } catch(e) { if (!isDupColErr(e)) throw e; }

  // --- Outreach List Add-On tables ---
  // One-time-purchase product ($250 trial / $100 Pro) that unlocks the curated
  // outreach contact list, per-category Google Contacts CSV exports, the
  // Submission Tracker, and AI-tweaked category intros in the Email Generator.
  // The contact list itself is versioned: admin uploads CSVs which bump the
  // version number and record a per-category diff so customers see an
  // "updated" banner the next time they log in.
  db.run(`
    CREATE TABLE IF NOT EXISTS outreach_contacts (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      version INTEGER NOT NULL,
      category TEXT NOT NULL,
      name TEXT NOT NULL,
      submission_type TEXT,
      submission_value TEXT,
      website TEXT,
      phone TEXT,
      notes TEXT,
      created_at TEXT DEFAULT (datetime('now'))
    )
  `);
  db.run('CREATE INDEX IF NOT EXISTS idx_outreach_contacts_version ON outreach_contacts(version)');
  db.run('CREATE INDEX IF NOT EXISTS idx_outreach_contacts_category ON outreach_contacts(category)');

  // Current published version + per-version change summary. Only one row
  // with singleton_key='current' is ever updated — history lives in the
  // change_summary JSON snapshots keyed by version (admin upload flow writes
  // {version, added_total, removed_total, by_category} on publish).
  db.run(`
    CREATE TABLE IF NOT EXISTS outreach_list_version (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      singleton_key TEXT UNIQUE NOT NULL,
      current_version INTEGER NOT NULL DEFAULT 0,
      change_summary TEXT,
      updated_at TEXT DEFAULT (datetime('now'))
    )
  `);
  try {
    dbHelpers.prepare("INSERT OR IGNORE INTO outreach_list_version (singleton_key, current_version, change_summary) VALUES ('current', 0, '{}')").run();
  } catch (e) { /* first boot before helpers ready — ignore */ }

  // One row per user that has purchased the Outreach List. Price paid is
  // locked at purchase time (trial buyers pay $250, Pro buyers pay $100)
  // so we keep the amount for support/refund lookups. No refunds per
  // product decision — once purchased, unlocked forever for that user.
  db.run(`
    CREATE TABLE IF NOT EXISTS outreach_purchases (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      user_id INTEGER NOT NULL,
      stripe_session_id TEXT,
      stripe_payment_intent_id TEXT,
      amount_cents INTEGER,
      price_id TEXT,
      purchased_at TEXT DEFAULT (datetime('now')),
      banner_dismissed_version INTEGER DEFAULT 0,
      FOREIGN KEY (user_id) REFERENCES users(id)
    )
  `);
  db.run('CREATE UNIQUE INDEX IF NOT EXISTS idx_outreach_purchases_user ON outreach_purchases(user_id)');

  // Per-user submission state for the Submission Tracker list. Keyed by
  // (user_id, contact_id) so if the admin removes a contact in a new
  // version, the row just stops showing up in the UI but the history is
  // preserved. XP is granted inline at mark time (+25) — no row here means
  // "not yet submitted".
  db.run(`
    CREATE TABLE IF NOT EXISTS submission_progress (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      user_id INTEGER NOT NULL,
      contact_id INTEGER NOT NULL,
      status TEXT NOT NULL DEFAULT 'submitted',
      release_id TEXT,
      submitted_at TEXT DEFAULT (datetime('now')),
      notes TEXT,
      FOREIGN KEY (user_id) REFERENCES users(id),
      FOREIGN KEY (contact_id) REFERENCES outreach_contacts(id)
    )
  `);
  db.run('CREATE INDEX IF NOT EXISTS idx_submission_progress_user ON submission_progress(user_id)');
  db.run('CREATE UNIQUE INDEX IF NOT EXISTS idx_submission_progress_user_contact ON submission_progress(user_id, contact_id)');

  // --- Data Safety: soft-delete columns (additive, tolerant) ---
  // deleted_at enables soft delete: rows are marked, not destroyed. Queries
  // that return user-facing data must include WHERE deleted_at IS NULL.
  try { db.run("ALTER TABLE users ADD COLUMN deleted_at TEXT"); } catch(e) { if (!isDupColErr(e)) throw e; }
  try { db.run("ALTER TABLE user_data ADD COLUMN deleted_at TEXT"); } catch(e) { if (!isDupColErr(e)) throw e; }
  try { db.run("ALTER TABLE user_xp ADD COLUMN deleted_at TEXT"); } catch(e) { if (!isDupColErr(e)) throw e; }
  try { db.run("ALTER TABLE user_achievements ADD COLUMN deleted_at TEXT"); } catch(e) { if (!isDupColErr(e)) throw e; }
  try { db.run("ALTER TABLE xp_log ADD COLUMN deleted_at TEXT"); } catch(e) { if (!isDupColErr(e)) throw e; }
  try { db.run("ALTER TABLE support_tickets ADD COLUMN deleted_at TEXT"); } catch(e) { if (!isDupColErr(e)) throw e; }
  try { db.run("ALTER TABLE api_usage ADD COLUMN deleted_at TEXT"); } catch(e) { if (!isDupColErr(e)) throw e; }
  try { db.run("ALTER TABLE submission_progress ADD COLUMN deleted_at TEXT"); } catch(e) { if (!isDupColErr(e)) throw e; }
  try { db.run("ALTER TABLE outreach_purchases ADD COLUMN deleted_at TEXT"); } catch(e) { if (!isDupColErr(e)) throw e; }
  try { db.run("ALTER TABLE redemption_requests ADD COLUMN deleted_at TEXT"); } catch(e) { if (!isDupColErr(e)) throw e; }

  // --- Data Safety: updated_at + version columns for change tracking ---
  try { db.run("ALTER TABLE users ADD COLUMN updated_at TEXT"); } catch(e) { if (!isDupColErr(e)) throw e; }
  try { db.run("ALTER TABLE user_xp ADD COLUMN updated_at TEXT"); } catch(e) { if (!isDupColErr(e)) throw e; }
  try { db.run("ALTER TABLE redemption_requests ADD COLUMN updated_at TEXT"); } catch(e) { if (!isDupColErr(e)) throw e; }
  try { db.run("ALTER TABLE submission_progress ADD COLUMN updated_at TEXT"); } catch(e) { if (!isDupColErr(e)) throw e; }
  try { db.run("ALTER TABLE user_data ADD COLUMN version INTEGER DEFAULT 1"); } catch(e) { if (!isDupColErr(e)) throw e; }

  // --- Data Safety: deleted user archive (append-only, never truncated) ---
  // When an admin soft-deletes a user, a full JSON snapshot of all their data
  // is preserved here so the deletion can be reversed if needed.
  db.run(`
    CREATE TABLE IF NOT EXISTS deleted_users_archive (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      user_id INTEGER NOT NULL,
      user_email TEXT NOT NULL,
      snapshot_json TEXT NOT NULL,
      deleted_by INTEGER NOT NULL,
      deleted_at TEXT DEFAULT (datetime('now'))
    )
  `);

  // --- Data Safety: append-only operation journal ---
  // Every critical mutation (signup, subscription change, admin delete, payment,
  // credential access) is logged here. Rows are NEVER deleted or truncated.
  db.run(`
    CREATE TABLE IF NOT EXISTS operation_journal (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      timestamp TEXT DEFAULT (datetime('now')),
      actor_id INTEGER,
      actor_email TEXT,
      action TEXT NOT NULL,
      entity_type TEXT,
      entity_id TEXT,
      detail TEXT,
      ip TEXT
    )
  `);
  db.run('CREATE INDEX IF NOT EXISTS idx_opjournal_timestamp ON operation_journal(timestamp DESC)');
  db.run('CREATE INDEX IF NOT EXISTS idx_opjournal_action ON operation_journal(action)');

  // --- Referral / Commission System ---
  // Each referrer gets a unique code. When a referred user subscribes via
  // Stripe, a commission row is created. Payouts are tracked manually.
  db.run(`
    CREATE TABLE IF NOT EXISTS referral_codes (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      user_id INTEGER NOT NULL,
      code TEXT NOT NULL UNIQUE,
      commission_rate REAL NOT NULL DEFAULT 0.10,
      active INTEGER NOT NULL DEFAULT 1,
      created_at TEXT DEFAULT (datetime('now')),
      FOREIGN KEY (user_id) REFERENCES users(id)
    )
  `);
  db.run('CREATE UNIQUE INDEX IF NOT EXISTS idx_referral_codes_code ON referral_codes(code)');
  db.run('CREATE INDEX IF NOT EXISTS idx_referral_codes_user ON referral_codes(user_id)');

  db.run(`
    CREATE TABLE IF NOT EXISTS referral_commissions (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      referrer_id INTEGER NOT NULL,
      referred_user_id INTEGER NOT NULL,
      referral_code TEXT NOT NULL,
      subscription_tier TEXT NOT NULL,
      amount_cents INTEGER NOT NULL,
      commission_cents INTEGER NOT NULL,
      status TEXT NOT NULL DEFAULT 'pending',
      stripe_subscription_id TEXT,
      paid_at TEXT,
      created_at TEXT DEFAULT (datetime('now')),
      FOREIGN KEY (referrer_id) REFERENCES users(id),
      FOREIGN KEY (referred_user_id) REFERENCES users(id)
    )
  `);
  db.run('CREATE INDEX IF NOT EXISTS idx_referral_commissions_referrer ON referral_commissions(referrer_id)');
  db.run('CREATE INDEX IF NOT EXISTS idx_referral_commissions_referred ON referral_commissions(referred_user_id)');

  // Add referred_by column to users (stores referral code used at signup)
  try { db.run("ALTER TABLE users ADD COLUMN referred_by TEXT"); } catch(e) { if (!isDupColErr(e)) throw e; }
  try { db.run("ALTER TABLE users ADD COLUMN slack_user_id TEXT"); } catch(e) { if (!isDupColErr(e)) throw e; }

  // Stripe Connect columns (added after CREATE TABLEs above)
  try { db.run("ALTER TABLE referral_codes ADD COLUMN stripe_connect_id TEXT"); } catch(e) { if (!isDupColErr(e)) throw e; }
  try { db.run("ALTER TABLE referral_codes ADD COLUMN stripe_onboarding_complete INTEGER DEFAULT 0"); } catch(e) { if (!isDupColErr(e)) throw e; }
  try { db.run("ALTER TABLE referral_commissions ADD COLUMN stripe_transfer_id TEXT"); } catch(e) { if (!isDupColErr(e)) throw e; }

  // --- Public Submission Forms (music + playlist) ---
  db.run(`
    CREATE TABLE IF NOT EXISTS music_submissions (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      artist_name TEXT NOT NULL,
      track_name TEXT NOT NULL,
      email TEXT NOT NULL,
      spotify_link TEXT,
      facebook_link TEXT,
      instagram_link TEXT,
      twitter_link TEXT,
      marketing_interest TEXT,
      interview_interest TEXT,
      press_release_file TEXT,
      profile_image_file TEXT,
      created_at TEXT DEFAULT (datetime('now'))
    )
  `);

  db.run(`
    CREATE TABLE IF NOT EXISTS playlist_submissions (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      artist_name TEXT NOT NULL,
      track_name TEXT NOT NULL,
      email TEXT NOT NULL,
      spotify_link TEXT,
      playlist_selection TEXT,
      marketing_interest TEXT,
      challenges TEXT,
      consent INTEGER DEFAULT 0,
      created_at TEXT DEFAULT (datetime('now'))
    )
  `);

  // --- Registration Queue ---
  // Tracks which releases need PRO/BMI registration prep and their status.
  // Admin reviews pre-filled data, manually submits on BMI, then logs
  // the confirmation number back here. Slack notification fires on new entries.
  db.run(`
    CREATE TABLE IF NOT EXISTS registration_queue (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      user_id INTEGER NOT NULL,
      release_title TEXT NOT NULL,
      platform TEXT NOT NULL DEFAULT 'bmi',
      status TEXT NOT NULL DEFAULT 'pending',
      field_completeness TEXT,
      missing_fields TEXT,
      confirmation_number TEXT,
      admin_notes TEXT,
      slack_notified INTEGER DEFAULT 0,
      created_at TEXT DEFAULT (datetime('now')),
      reviewed_at TEXT,
      submitted_at TEXT,
      confirmed_at TEXT,
      UNIQUE(user_id, release_title, platform),
      FOREIGN KEY (user_id) REFERENCES users(id)
    )
  `);
  db.run('CREATE INDEX IF NOT EXISTS idx_regqueue_status ON registration_queue(status)');
  db.run('CREATE INDEX IF NOT EXISTS idx_regqueue_user ON registration_queue(user_id)');

  // --- Seed Admin Accounts ---
  // ADMIN_PASSWORD must come from env. No fallback — failing closed prevents
  // a known-literal password from ever being seeded into the DB.
  const ADMIN_PASSWORD = process.env.ADMIN_PASSWORD;
  const ADMINS = [
    { email: 'josephmadiganmusic@gmail.com' },
    { email: 'official.stevenperez@gmail.com' }
  ];

  if (!ADMIN_PASSWORD) {
    console.warn('[BOOT] ADMIN_PASSWORD not set — skipping admin seeding. Existing admin accounts are unaffected.');
  } else {
    for (const admin of ADMINS) {
      const existing = dbHelpers.prepare('SELECT id, role, subscription_status FROM users WHERE email = ?').get(admin.email);
      if (!existing) {
        const hash = bcrypt.hashSync(ADMIN_PASSWORD, 10);
        dbHelpers.prepare('INSERT INTO users (email, password, role, subscription_status) VALUES (?, ?, ?, ?)').run(admin.email, hash, 'admin', 'active');
        console.log('[BOOT] seeded new admin account: ' + admin.email);
      } else if (existing.role !== 'admin' || existing.subscription_status !== 'active') {
        // Promote an existing non-admin user whose email was added to the
        // ADMINS list after that user already registered. Without this
        // branch, adding a new admin email to the list never takes effect
        // for a pre-existing account because the INSERT is skipped by the
        // !exists guard. Password is NOT touched on promotion — we only
        // elevate role + ensure active subscription_status so the account
        // can immediately use admin-only endpoints.
        dbHelpers.prepare("UPDATE users SET role = 'admin', subscription_status = 'active' WHERE id = ?").run(existing.id);
        console.log('[BOOT] promoted existing account to admin: ' + admin.email);
      }
    }
  }

  // --- Test account: ensure jocejm20@gmail.com is Elite Plus for testing ---
  {
    const testUser = dbHelpers.prepare('SELECT id, subscription_tier, subscription_status FROM users WHERE email = ? AND deleted_at IS NULL').get('jocejm20@gmail.com');
    if (testUser && (testUser.subscription_tier !== 'elite_plus' || testUser.subscription_status !== 'active')) {
      dbHelpers.prepare("UPDATE users SET subscription_tier = 'elite_plus', subscription_status = 'active', updated_at = datetime('now') WHERE id = ?").run(testUser.id);
      console.log('[BOOT] promoted jocejm20@gmail.com to elite_plus (test account)');
      flushDbNow();
    } else if (!testUser) {
      console.log('[BOOT] jocejm20@gmail.com not found — will be promoted on next registration');
    }
  }
}

// --- Stripe Setup ---
const stripe = process.env.STRIPE_SECRET_KEY ? new Stripe(process.env.STRIPE_SECRET_KEY) : null;
const STRIPE_PRICE_ID = process.env.STRIPE_PRICE_ID || '';
const STRIPE_ELITE_PRICE_ID = process.env.STRIPE_ELITE_PRICE_ID || '';
const STRIPE_ELITE_PLUS_PRICE_ID = process.env.STRIPE_ELITE_PLUS_PRICE_ID || '';
const STRIPE_REDEMPTION_PRICE_ID = process.env.STRIPE_REDEMPTION_PRICE_ID || '';
// Outreach List Add-On — one-time purchase, two price points. Trial users pay
// $250 (no subscription required, purchase IS the access grant — see R9 in
// task/lessons.md), Pro/Elite users pay $100.
const STRIPE_OUTREACH_PRICE_TRIAL = process.env.STRIPE_OUTREACH_PRICE_TRIAL || '';
const STRIPE_OUTREACH_PRICE_PRO = process.env.STRIPE_OUTREACH_PRICE_PRO || '';
// Canonical category slugs. Order here drives button order in the Email
// Generator category bar. `podcast` ships even though the first CSV upload
// has zero podcast rows — the UI shows a "No contacts yet" placeholder.
const OUTREACH_CATEGORIES = ['sirius', 'iheart', 'fm_am', 'online_radio', 'press', 'press_release', 'blog', 'spotify_playlist', 'podcast'];
const OUTREACH_CATEGORY_LABELS = {
  sirius: 'Sirius XM',
  iheart: 'iHeart',
  fm_am: 'FM / AM',
  online_radio: 'Online Radio',
  press: 'Press',
  press_release: 'Press Release',
  blog: 'Blog',
  spotify_playlist: 'Spotify Playlist',
  podcast: 'Podcast'
};
const STRIPE_WEBHOOK_SECRET = process.env.STRIPE_WEBHOOK_SECRET || '';

// Map Stripe price IDs to internal subscription tiers. Used by both checkout
// (validate requested tier → resolve price) and webhook (resolve incoming
// price ID → tier to persist on the user row).
const TIER_PRICE_MAP = {
  pro: STRIPE_PRICE_ID,
  elite: STRIPE_ELITE_PRICE_ID,
  elite_plus: STRIPE_ELITE_PLUS_PRICE_ID
};
function tierFromPriceId(priceId) {
  if (!priceId) return null;
  if (STRIPE_ELITE_PLUS_PRICE_ID && priceId === STRIPE_ELITE_PLUS_PRICE_ID) return 'elite_plus';
  if (STRIPE_ELITE_PRICE_ID && priceId === STRIPE_ELITE_PRICE_ID) return 'elite';
  if (STRIPE_PRICE_ID && priceId === STRIPE_PRICE_ID) return 'pro';
  return null;
}
function isEliteTier(tier) { return tier === 'elite' || tier === 'elite_plus'; }

// --- Onboarding credential encryption (AES-256-GCM) ---
// Elite/Elite Plus customers submit distribution + social credentials so
// Joseph can do manual concierge work. We store these encrypted at rest with
// a server-only key, decryption restricted to admin route + audit logged.
// Boot fatal if Elite price IDs are configured but the encryption key is missing —
// we never want a misconfigured deploy to silently store plaintext creds.
const ONBOARDING_ENCRYPTION_KEY_HEX = process.env.ONBOARDING_ENCRYPTION_KEY || '';
let ONBOARDING_KEY_BUF = null;
if (STRIPE_ELITE_PRICE_ID || STRIPE_ELITE_PLUS_PRICE_ID) {
  if (!ONBOARDING_ENCRYPTION_KEY_HEX) {
    console.error('[BOOT] FATAL: STRIPE_ELITE_PRICE_ID or STRIPE_ELITE_PLUS_PRICE_ID set but ONBOARDING_ENCRYPTION_KEY missing. Aborting boot.');
    process.exit(1);
  }
  try {
    ONBOARDING_KEY_BUF = Buffer.from(ONBOARDING_ENCRYPTION_KEY_HEX, 'hex');
    if (ONBOARDING_KEY_BUF.length !== 32) throw new Error('must be 32 bytes (64 hex chars)');
  } catch (e) {
    console.error('[BOOT] FATAL: ONBOARDING_ENCRYPTION_KEY invalid:', e.message, '— generate with: node -e "console.log(require(\'crypto\').randomBytes(32).toString(\'hex\'))"');
    process.exit(1);
  }
}
function encryptOnboarding(obj) {
  if (!ONBOARDING_KEY_BUF) throw new Error('Onboarding encryption not configured');
  const iv = crypto.randomBytes(12);
  const cipher = crypto.createCipheriv('aes-256-gcm', ONBOARDING_KEY_BUF, iv);
  const plaintext = Buffer.from(JSON.stringify(obj), 'utf8');
  const enc = Buffer.concat([cipher.update(plaintext), cipher.final()]);
  return {
    data_encrypted: enc.toString('hex'),
    iv: iv.toString('hex'),
    auth_tag: cipher.getAuthTag().toString('hex')
  };
}
function decryptOnboarding(row) {
  if (!ONBOARDING_KEY_BUF) throw new Error('Onboarding encryption not configured');
  const iv = Buffer.from(row.iv, 'hex');
  const tag = Buffer.from(row.auth_tag, 'hex');
  const enc = Buffer.from(row.data_encrypted, 'hex');
  const decipher = crypto.createDecipheriv('aes-256-gcm', ONBOARDING_KEY_BUF, iv);
  decipher.setAuthTag(tag);
  const dec = Buffer.concat([decipher.update(enc), decipher.final()]);
  return JSON.parse(dec.toString('utf8'));
}

// --- Email Setup (Resend — HTTPS API, works on Railway) ---
const resend = process.env.RESEND_API_KEY ? new Resend(process.env.RESEND_API_KEY) : null;
const EMAIL_FROM = process.env.EMAIL_FROM || 'Rollout Heaven <onboarding@resend.dev>';

// Admin emails that receive notifications for Elite/Elite Plus activity.
const ADMIN_NOTIFY_EMAILS = [
  'josephmadiganmusic@gmail.com'
];

// Fire-and-forget admin email notification. Failures are logged but never
// block the user-facing request. Skips silently if Resend is not configured.
async function notifyAdmins(subject, htmlBody) {
  if (!resend) return;
  for (const to of ADMIN_NOTIFY_EMAILS) {
    try {
      await resend.emails.send({
        from: EMAIL_FROM,
        to,
        subject: `[Rollout Heaven] ${subject}`,
        html: `
          <div style="font-family: 'Segoe UI', Arial, sans-serif; max-width: 560px; margin: 0 auto; background: #080b1e; color: #e0e0e0; border-radius: 12px; overflow: hidden;">
            <div style="text-align: center; padding: 24px 24px 12px;">
              <h1 style="margin: 0; font-size: 20px; background: linear-gradient(135deg, #7b2ff7, #00d4ff); -webkit-background-clip: text; -webkit-text-fill-color: transparent;">Rollout Heaven — Admin Alert</h1>
            </div>
            <div style="padding: 16px 32px 32px; font-size: 14px; line-height: 1.6;">
              ${htmlBody}
            </div>
            <div style="padding: 12px 32px; background: #0d1029; text-align: center; font-size: 11px; color: #6b7094;">
              <a href="https://${CUSTOM_DOMAIN}/admin" style="color: #00d4ff;">Open Admin Dashboard</a>
            </div>
          </div>
        `
      });
    } catch (e) {
      console.error('[NOTIFY] admin email failed for', to, ':', e.message);
    }
  }
}

// Independent control over verification enforcement. Defaults to ON in
// production so a missing/expired RESEND_API_KEY can never silently disable
// verification (which would let attackers spin up unlimited free trials).
// Set REQUIRE_EMAIL_VERIFICATION=false explicitly to opt out (e.g., local dev).
const REQUIRE_EMAIL_VERIFICATION = process.env.REQUIRE_EMAIL_VERIFICATION
  ? process.env.REQUIRE_EMAIL_VERIFICATION !== 'false'
  : process.env.NODE_ENV === 'production';

if (REQUIRE_EMAIL_VERIFICATION && !resend) {
  console.error('[BOOT] FATAL: REQUIRE_EMAIL_VERIFICATION is on but RESEND_API_KEY is not set. Aborting boot.');
  process.exit(1);
}

async function sendVerificationEmail(email, token) {
  if (!resend) throw new Error('RESEND_API_KEY not configured');
  const verifyUrl = `https://${CUSTOM_DOMAIN}/api/verify-email?token=${token}`;
  await resend.emails.send({
    from: EMAIL_FROM,
    to: email,
    subject: 'Verify Your Email - Rollout Heaven',
    html: `
      <div style="font-family: 'Segoe UI', Arial, sans-serif; max-width: 480px; margin: 0 auto; background: #080b1e; color: #e0e0e0; border-radius: 12px; overflow: hidden;">
        <div style="text-align: center; padding: 32px 24px 16px;">
          <h1 style="margin: 0; font-size: 24px; background: linear-gradient(135deg, #7b2ff7, #00d4ff); -webkit-background-clip: text; -webkit-text-fill-color: transparent;">Rollout Heaven</h1>
          <p style="color: #6b7094; font-size: 12px; letter-spacing: 2px; margin-top: 4px;">RELEASE MANAGEMENT PLATFORM</p>
        </div>
        <div style="padding: 24px 32px 32px;">
          <h2 style="color: #fff; font-size: 18px; margin-bottom: 12px;">Verify Your Email</h2>
          <p style="color: #9ea2b8; font-size: 14px; line-height: 1.6; margin-bottom: 24px;">Click the button below to verify your email and activate your 7-day free trial.</p>
          <div style="text-align: center; margin-bottom: 24px;">
            <a href="${verifyUrl}" style="display: inline-block; padding: 14px 40px; background: linear-gradient(135deg, #7b2ff7, #00d4ff); color: #fff; text-decoration: none; border-radius: 8px; font-weight: 700; font-size: 14px;">Verify Email</a>
          </div>
          <p style="color: #6b7094; font-size: 12px; line-height: 1.5;">If the button doesn't work, copy and paste this link:<br><a href="${verifyUrl}" style="color: #00d4ff; word-break: break-all;">${verifyUrl}</a></p>
          <p style="color: #6b7094; font-size: 12px; margin-top: 16px;">This link expires in 24 hours.</p>
        </div>
      </div>
    `
  });
}

// --- Middleware ---
app.use((req, res, next) => {
  if (req.path === '/webhook') return next(); // raw body for Stripe
  // Audio transcription needs a bigger cap (base64 MP3s inflate ~33%);
  // it gets its own route-level parser at the endpoint.
  if (req.path === '/api/intake/transcribe-lyrics') return next();
  // Slack interactions come as x-www-form-urlencoded; need raw body for signature
  if (req.path === '/api/slack/interactions') return next();
  express.json({ limit: '10mb' })(req, res, next);
});
app.use(express.urlencoded({ extended: true }));

// Persistent session store — file-backed, lives on the same Railway volume
// as the sqlite DB. Survives restarts and avoids the MemoryStore leak warning.
const SESSIONS_DIR = path.join(DATA_DIR, 'sessions');
if (!fs.existsSync(SESSIONS_DIR)) fs.mkdirSync(SESSIONS_DIR, { recursive: true });

// Session secret MUST come from env in production. In dev, generate a
// random per-process secret so sessions still work locally but cannot be
// forged with a known literal.
const SESSION_SECRET = process.env.SESSION_SECRET
  || (process.env.NODE_ENV === 'production'
      ? (() => { throw new Error('SESSION_SECRET env var required in production'); })()
      : crypto.randomBytes(32).toString('hex'));

app.use(session({
  store: new FileStore({
    path: SESSIONS_DIR,
    ttl: 30 * 24 * 60 * 60, // 30 days, matches Remember Me max
    retries: 2,
    reapInterval: 60 * 60,  // sweep expired sessions hourly
    logFn: () => {}         // silence noisy info logs
  }),
  secret: SESSION_SECRET,
  resave: false,
  saveUninitialized: false,
  cookie: {
    // Always secure in production. Previously this required BOTH NODE_ENV
    // and RAILWAY_ENVIRONMENT — a missing env var silently downgraded
    // cookies to plaintext over HTTP. (H2)
    secure: process.env.NODE_ENV === 'production',
    httpOnly: true,
    sameSite: 'lax'
    // maxAge set per-login based on "Remember Me" checkbox
  },
  proxy: true
}));

// Trust Railway proxy
app.set('trust proxy', 1);

// --- Security headers (inline; avoids adding helmet as a dependency) (M2) ---
app.use((req, res, next) => {
  res.setHeader('X-Content-Type-Options', 'nosniff');
  res.setHeader('X-Frame-Options', 'SAMEORIGIN');
  res.setHeader('Referrer-Policy', 'strict-origin-when-cross-origin');
  res.setHeader('Permissions-Policy', 'geolocation=(), microphone=(), camera=()');
  if (process.env.NODE_ENV === 'production') {
    res.setHeader('Strict-Transport-Security', 'max-age=31536000; includeSubDomains');
  }
  next();
});

// --- Rate limiting (in-memory; single-instance only — see M4) ---
// Bucket key is `${routeName}:${userId || ip}`. Each bucket holds an array of
// timestamps within the window. Cheaper than pulling in express-rate-limit
// for ~5 routes, and works fine because the app already cannot scale beyond
// one process (sql.js + session-file-store).
const _rlBuckets = new Map();
function rateLimit({ name, windowMs, max }) {
  return (req, res, next) => {
    const id = (req.session && req.session.userId) || req.ip || 'anon';
    const key = name + ':' + id;
    const now = Date.now();
    let arr = _rlBuckets.get(key);
    if (!arr) { arr = []; _rlBuckets.set(key, arr); }
    // Drop expired entries.
    while (arr.length && arr[0] <= now - windowMs) arr.shift();
    if (arr.length >= max) {
      const retryAfter = Math.ceil((arr[0] + windowMs - now) / 1000);
      res.set('Retry-After', String(retryAfter));
      return res.status(429).json({ error: 'Too many requests. Try again shortly.' });
    }
    arr.push(now);
    next();
  };
}
// Sweep stale buckets every 5 minutes so the Map doesn't grow forever.
setInterval(() => {
  const cutoff = Date.now() - 60 * 60 * 1000;
  for (const [k, arr] of _rlBuckets) {
    if (!arr.length || arr[arr.length - 1] < cutoff) _rlBuckets.delete(k);
  }
}, 5 * 60 * 1000).unref();

const rlAuth = rateLimit({ name: 'auth', windowMs: 15 * 60 * 1000, max: 10 });
// Separate bucket for resend-verification so a user pounding the resend
// button can't lock themselves out of /api/login (both used to share rlAuth).
const rlResend = rateLimit({ name: 'resend', windowMs: 60 * 60 * 1000, max: 5 });
const rlSignup = rateLimit({ name: 'signup', windowMs: 60 * 60 * 1000, max: 5 });
const rlClaude = rateLimit({ name: 'claude', windowMs: 60 * 60 * 1000, max: 60 });
const rlResearch = rateLimit({ name: 'research', windowMs: 60 * 60 * 1000, max: 100 });
const rlSupport = rateLimit({ name: 'support', windowMs: 60 * 60 * 1000, max: 5 });
// Outreach List — AI-tweaked category intro generator (see R9). Generous
// enough that a user can cycle through all 8 categories + re-roll several
// times while writing an email, tight enough to cap Claude cost per user.
const rlOutreachIntro = rateLimit({ name: 'outreach_intro', windowMs: 60 * 60 * 1000, max: 40 });
// Submission Tracker — personalized social DM generator. Higher cap than
// the intro generator because users will click through many social contacts
// in a single session while prospecting.
const rlSocialDm = rateLimit({ name: 'social_dm', windowMs: 60 * 60 * 1000, max: 120 });
// Intake — audio-to-lyrics transcription via Groq Whisper. Client
// chunks audio into 30s segments to stop hallucination cascade, so
// a single 5-minute song fires ~10 requests. 60/hr covers ~6 songs
// per user per hour, still comfortably inside Groq's free-tier cap
// (20 req/min, 2000 req/day on whisper-large-v3).
const rlTranscribe = rateLimit({ name: 'transcribe', windowMs: 60 * 60 * 1000, max: 60 });

// Health check endpoint (must respond before any redirects)
app.get('/health', (req, res) => res.status(200).send('OK'));

// Redirect Railway URL to custom domain
const CUSTOM_DOMAIN = process.env.CUSTOM_DOMAIN || 'rolloutheaven.com';
app.use((req, res, next) => {
  const host = req.hostname;
  // Don't redirect health checks or internal Railway requests
  if (host && host.endsWith('.railway.app') && req.path !== '/health') {
    return res.redirect(301, `https://${CUSTOM_DOMAIN}${req.originalUrl}`);
  }
  next();
});

// --- Auth Helpers ---
function hasAccess(user) {
  if (!user) return false;
  if (user.role === 'admin') return true;
  if (user.subscription_status === 'active') return true;
  if (user.subscription_status === 'trialing' && user.trial_ends_at) {
    return new Date(user.trial_ends_at) > new Date();
  }
  return false;
}

function requireAuth(req, res, next) {
  if (!req.session.userId) return res.redirect('/login');
  const user = dbHelpers.prepare('SELECT * FROM users WHERE id = ? AND deleted_at IS NULL').get(req.session.userId);
  if (!user) { req.session.destroy(); return res.redirect('/login'); }
  req.user = user;
  next();
}

function requireAccess(req, res, next) {
  const isApi = req.path.startsWith('/api/');
  if (!req.session.userId) {
    if (isApi) return res.status(401).json({ error: 'Not logged in' });
    return res.redirect('/login');
  }
  const user = dbHelpers.prepare('SELECT * FROM users WHERE id = ? AND deleted_at IS NULL').get(req.session.userId);
  if (!user) {
    req.session.destroy();
    if (isApi) return res.status(401).json({ error: 'Session invalid' });
    return res.redirect('/login');
  }
  req.user = user;
  if (!hasAccess(user)) {
    if (isApi) return res.status(402).json({ error: 'Subscription required', redirect: '/subscribe' });
    return res.redirect('/subscribe');
  }
  next();
}

// Pro-only guard. Trial users are blocked — only paid (active) and admins
// can hit endpoints that proxy paid third-party APIs (Anthropic, Serper).
// (C-1 fix: previously trialing users got Music Agent Pro features for free
// for 7 days, bypassing the paywall.)
function requireActive(req, res, next) {
  const isApi = req.path.startsWith('/api/');
  if (!req.session.userId) {
    if (isApi) return res.status(401).json({ error: 'Not logged in' });
    return res.redirect('/login');
  }
  const user = dbHelpers.prepare('SELECT * FROM users WHERE id = ? AND deleted_at IS NULL').get(req.session.userId);
  if (!user) {
    req.session.destroy();
    if (isApi) return res.status(401).json({ error: 'Session invalid' });
    return res.redirect('/login');
  }
  req.user = user;
  if (user.role === 'admin' || user.subscription_status === 'active') return next();
  if (isApi) return res.status(402).json({ error: 'Pro subscription required', upgrade: '/subscribe', reason: 'pro_only' });
  return res.redirect('/subscribe');
}

// Outreach List unlock gate. Admins always pass. Everyone else must have a
// row in `outreach_purchases` — the $250/$100 one-time purchase IS the access
// grant (see R9 in task/lessons.md). DO NOT chain `requireActive` in front of
// this: trial users who paid for the Outreach List must be able to use the
// AI intro endpoint even though they can't hit /api/claude directly. That is
// NOT a paywall bypass — it's a separately-sold product whose contract
// requires it to work for any buyer.
function requireOutreachUnlocked(req, res, next) {
  const isApi = req.path.startsWith('/api/');
  if (!req.session.userId) {
    if (isApi) return res.status(401).json({ error: 'Not logged in' });
    return res.redirect('/login');
  }
  const user = dbHelpers.prepare('SELECT * FROM users WHERE id = ? AND deleted_at IS NULL').get(req.session.userId);
  if (!user) {
    req.session.destroy();
    if (isApi) return res.status(401).json({ error: 'Session invalid' });
    return res.redirect('/login');
  }
  req.user = user;
  if (user.role === 'admin') return next();
  const purchase = dbHelpers.prepare('SELECT id FROM outreach_purchases WHERE user_id = ?').get(user.id);
  if (purchase) return next();
  if (isApi) return res.status(402).json({ error: 'Outreach List not purchased', upgrade: '/subscribe#outreach-list', reason: 'outreach_locked' });
  return res.redirect('/subscribe#outreach-list');
}

// --- Favicon (public, no auth) ---
app.get(['/favicon.ico', '/favicon.jpg', '/favicon.png'], (req, res) => {
  res.setHeader('Cache-Control', 'public, max-age=86400');
  res.sendFile(path.join(__dirname, 'favicon.jpg'));
});

// --- Public legal pages (no auth required) ---
app.get(['/privacy', '/privacy-policy', '/privacy.html'], (req, res) => {
  res.sendFile(path.join(__dirname, 'privacy.html'));
});
app.get(['/terms', '/terms-of-service', '/terms.html'], (req, res) => {
  res.sendFile(path.join(__dirname, 'terms.html'));
});

// --- Auth Routes ---
app.get('/login', (req, res) => {
  if (req.session.userId) return res.redirect('/');
  res.sendFile(path.join(__dirname, 'login.html'));
});

app.get('/referrals', requireAuth, (req, res) => {
  res.sendFile(path.join(__dirname, 'referrals.html'));
});

// Pre-computed dummy hash so misses do equivalent CPU work to hits.
// Generated once at boot so we don't burn ~50ms on every cold login miss.
const DUMMY_BCRYPT_HASH = bcrypt.hashSync('dummy-password-for-timing-equalization', 10);

app.post('/api/login', rlAuth, (req, res) => {
  const { email, password, rememberMe } = req.body || {};
  if (!email || !password) return res.status(400).json({ error: 'Email and password required' });
  if (typeof email !== 'string' || typeof password !== 'string') return res.status(400).json({ error: 'Invalid input' });
  if (email.length > 254 || password.length > 200) return res.status(400).json({ error: 'Invalid input' });

  const user = dbHelpers.prepare('SELECT * FROM users WHERE email = ? AND deleted_at IS NULL').get(email.toLowerCase().trim());
  // Always run a bcrypt compare — even on miss — to flatten the timing
  // signal. Without this, attackers can enumerate registered emails by
  // measuring response time. (H1)
  const valid = user
    ? bcrypt.compareSync(password, user.password)
    : (bcrypt.compareSync(password, DUMMY_BCRYPT_HASH), false);
  if (!user || !valid) {
    return res.status(401).json({ error: 'Invalid email or password' });
  }

  // Enforce verification independent of Resend availability — see C7.
  if (REQUIRE_EMAIL_VERIFICATION && !user.email_verified && user.role !== 'admin') {
    return res.status(403).json({ error: 'Please verify your email before logging in. Check your inbox for a verification link.', needsVerification: true, email: user.email });
  }

  // Remember Me: 30 days if checked, session-only if not
  if (rememberMe) {
    req.session.cookie.maxAge = 30 * 24 * 60 * 60 * 1000;
  } else {
    req.session.cookie.maxAge = null; // session cookie — expires on browser close
  }

  req.session.userId = user.id;
  res.json({ success: true, hasAccess: hasAccess(user) });
});

// RFC-5321-ish: local 64, domain 255, total 254. Plus a strict-ish format
// check. Conservative deny set blocks angle brackets / quotes that would
// otherwise enable HTML injection in admin UIs that ever forget to escape.
const EMAIL_RE = /^[A-Za-z0-9._%+\-]+@[A-Za-z0-9.\-]+\.[A-Za-z]{2,}$/;

app.post('/api/signup', rlSignup, async (req, res) => {
  const { email, password, ref } = req.body;
  if (!email || !password) return res.status(400).json({ error: 'Email and password required' });
  if (typeof email !== 'string' || typeof password !== 'string') return res.status(400).json({ error: 'Invalid input' });
  if (password.length < 8 || password.length > 200) return res.status(400).json({ error: 'Password must be 8–200 characters' });
  if (email.length > 254 || !EMAIL_RE.test(email.trim())) return res.status(400).json({ error: 'Invalid email address' });

  const cleanEmail = email.toLowerCase().trim();
  const existing = dbHelpers.prepare('SELECT id FROM users WHERE email = ? AND deleted_at IS NULL').get(cleanEmail);
  if (existing) return res.status(409).json({ error: 'Account already exists. Please log in.' });

  // Validate referral code if provided
  const refCode = (ref && typeof ref === 'string') ? ref.trim().toUpperCase() : null;
  let validRef = null;
  if (refCode) {
    validRef = dbHelpers.prepare('SELECT * FROM referral_codes WHERE code = ? AND active = 1').get(refCode);
  }

  const hash = bcrypt.hashSync(password, 10);
  const trialEnd = new Date(Date.now() + 7 * 24 * 60 * 60 * 1000).toISOString();
  const token = crypto.randomBytes(32).toString('hex');
  const tokenExpires = new Date(Date.now() + 24 * 60 * 60 * 1000).toISOString();

  dbHelpers.prepare(
    'INSERT INTO users (email, password, subscription_status, trial_ends_at, verification_token, verification_expires, referred_by) VALUES (?, ?, ?, ?, ?, ?, ?)'
  ).run(cleanEmail, hash, 'trialing', trialEnd, token, tokenExpires, validRef ? refCode : null);

  // Respond immediately, send email in background (don't block the request).
  // Auto-verification only happens when verification is explicitly disabled —
  // never as a silent fallback when Resend is missing (see C7).
  // Flush the new user row to disk before we respond — signup is a durable
  // write we must not lose in the 100ms debounce window. (C2)
  flushDbNow();
  logOperation(req, 'user.signup', 'user', null, { email: cleanEmail });

  if (REQUIRE_EMAIL_VERIFICATION) {
    if (resend) {
      sendVerificationEmail(cleanEmail, token).catch(err => {
        console.error('[SIGNUP] Email send error:', err.message);
      });
    }
    res.json({ success: true, needsVerification: true });
  } else {
    console.log('[SIGNUP] Verification disabled, auto-verifying', cleanEmail);
    dbHelpers.prepare('UPDATE users SET email_verified = 1 WHERE email = ?').run(cleanEmail);
    flushDbNow();
    res.json({ success: true, needsVerification: false });
  }
});

// --- Email Verification Route ---
app.get('/api/verify-email', (req, res) => {
  const { token } = req.query;
  if (!token) return res.status(400).send('Invalid verification link.');

  const user = dbHelpers.prepare('SELECT * FROM users WHERE verification_token = ? AND deleted_at IS NULL').get(token);
  if (!user) return res.status(400).send('Invalid or expired verification link.');

  if (new Date(user.verification_expires) < new Date()) {
    return res.status(400).send('Verification link has expired. Please request a new one.');
  }

  dbHelpers.prepare('UPDATE users SET email_verified = 1, verification_token = NULL, verification_expires = NULL, updated_at = datetime("now") WHERE id = ?').run(user.id);
  flushDbNow(); // durable write — don't rely on the 100ms debounce (C2/L4)

  // Do NOT auto-login here. Auto-login on a GET request is a login-fixation
  // vector: an attacker could phish a victim with their own verification link
  // and have the victim's browser silently signed in to the attacker's account.
  // The user types credentials on the next screen — small UX cost, big security win.
  res.redirect('/login?verified=1');
});

// --- Resend Verification Email ---
app.post('/api/resend-verification', rlResend, async (req, res) => {
  const { email } = req.body;
  if (!email) return res.status(400).json({ error: 'Email required' });

  const user = dbHelpers.prepare('SELECT * FROM users WHERE email = ? AND deleted_at IS NULL').get(email.toLowerCase().trim());
  if (!user) return res.json({ success: true }); // Don't reveal if account exists
  if (user.email_verified) return res.json({ success: true });

  const token = crypto.randomBytes(32).toString('hex');
  const tokenExpires = new Date(Date.now() + 24 * 60 * 60 * 1000).toISOString();
  dbHelpers.prepare('UPDATE users SET verification_token = ?, verification_expires = ? WHERE id = ?').run(token, tokenExpires, user.id);

  try {
    await Promise.race([
      sendVerificationEmail(user.email, token),
      new Promise((_, reject) => setTimeout(() => reject(new Error('Email send timeout')), 12000))
    ]);
    res.json({ success: true, sent: true });
  } catch (err) {
    console.error('Resend email error:', err.message);
    res.json({ success: true, sent: false });
  }
});

app.post('/api/logout', (req, res) => {
  req.session.destroy();
  res.json({ success: true });
});

app.get('/api/me', (req, res) => {
  if (!req.session.userId) return res.json({ loggedIn: false });
  const user = dbHelpers.prepare('SELECT id, email, role, subscription_status, subscription_tier, onboarding_completed, trial_ends_at FROM users WHERE id = ? AND deleted_at IS NULL').get(req.session.userId);
  if (!user) return res.json({ loggedIn: false });

  let daysLeft = null;
  if (user.subscription_status === 'trialing' && user.trial_ends_at) {
    daysLeft = Math.max(0, Math.ceil((new Date(user.trial_ends_at) - new Date()) / (1000 * 60 * 60 * 24)));
  }

  res.json({ loggedIn: true, ...user, hasAccess: hasAccess(user), trialDaysLeft: daysLeft });
});

// --- Referral: user-facing earnings dashboard ---
app.get('/api/referrals/me', requireAuth, (req, res) => {
  const code = dbHelpers.prepare('SELECT * FROM referral_codes WHERE user_id = ? AND active = 1').get(req.user.id);
  if (!code) return res.json({ hasCode: false });
  const commissions = dbHelpers.prepare(`
    SELECT rc.subscription_tier, rc.commission_cents, rc.status, rc.created_at,
      su.email AS subscriber_email
    FROM referral_commissions rc
    JOIN users su ON su.id = rc.referred_user_id
    WHERE rc.referrer_id = ?
    ORDER BY rc.created_at DESC
  `).all(req.user.id);
  const totalEarned = commissions.reduce((s, c) => s + c.commission_cents, 0);
  const totalPaid = commissions.filter(c => c.status === 'paid').reduce((s, c) => s + c.commission_cents, 0);
  const totalPending = commissions.filter(c => c.status === 'pending').reduce((s, c) => s + c.commission_cents, 0);
  res.json({
    hasCode: true,
    code: code.code,
    commission_rate: code.commission_rate,
    referral_link: `https://${CUSTOM_DOMAIN}/login?ref=${code.code}`,
    payout: {
      stripe_connected: !!(code.stripe_connect_id && code.stripe_onboarding_complete),
      stripe_onboarding_started: !!code.stripe_connect_id,
      stripe_onboarding_complete: !!code.stripe_onboarding_complete
    },
    stats: { total_referrals: commissions.length, total_earned: totalEarned, total_paid: totalPaid, total_pending: totalPending },
    commissions
  });
});

// --- Stripe Connect: referrer payout onboarding ---
// Creates a Stripe Express Connected Account for the referrer and returns
// the hosted onboarding URL. On completion, Stripe redirects back to /referrals.
app.post('/api/referrals/connect-onboard', requireAuth, async (req, res) => {
  if (!stripe) return res.status(503).json({ error: 'Payments not configured' });
  const code = dbHelpers.prepare('SELECT * FROM referral_codes WHERE user_id = ? AND active = 1').get(req.user.id);
  if (!code) return res.status(403).json({ error: 'No referral code found' });

  try {
    let connectId = code.stripe_connect_id;

    // Create connected account if not yet started
    if (!connectId) {
      const account = await stripe.accounts.create({
        type: 'express',
        email: req.user.email,
        metadata: { referral_code: code.code, user_id: String(req.user.id) },
        capabilities: { transfers: { requested: true } }
      });
      connectId = account.id;
      dbHelpers.prepare('UPDATE referral_codes SET stripe_connect_id = ? WHERE id = ?').run(connectId, code.id);
      logOperation(req, 'referral.connect_account_created', 'user', req.user.id, { code: code.code, stripe_connect_id: connectId });
      flushDbNow();
    }

    // Generate onboarding link
    const baseUrl = `https://${CUSTOM_DOMAIN}`;
    const accountLink = await stripe.accountLinks.create({
      account: connectId,
      refresh_url: `${baseUrl}/referrals?connect=retry`,
      return_url: `${baseUrl}/referrals?connect=complete`,
      type: 'account_onboarding'
    });

    res.json({ url: accountLink.url });
  } catch (err) {
    console.error('[CONNECT] onboarding error:', err.message);
    res.status(500).json({ error: 'Failed to start payout setup' });
  }
});

// Check if connected account onboarding is complete (called after redirect)
app.get('/api/referrals/connect-status', requireAuth, async (req, res) => {
  if (!stripe) return res.status(503).json({ error: 'Payments not configured' });
  const code = dbHelpers.prepare('SELECT * FROM referral_codes WHERE user_id = ? AND active = 1').get(req.user.id);
  if (!code || !code.stripe_connect_id) return res.json({ connected: false });

  try {
    const account = await stripe.accounts.retrieve(code.stripe_connect_id);
    const complete = account.details_submitted && account.charges_enabled;
    if (complete && !code.stripe_onboarding_complete) {
      dbHelpers.prepare('UPDATE referral_codes SET stripe_onboarding_complete = 1 WHERE id = ?').run(code.id);
      logOperation(req, 'referral.connect_onboarding_complete', 'user', req.user.id, { code: code.code, stripe_connect_id: code.stripe_connect_id });
      flushDbNow();
    }
    res.json({ connected: complete, details_submitted: account.details_submitted, charges_enabled: account.charges_enabled });
  } catch (err) {
    console.error('[CONNECT] status check error:', err.message);
    res.status(500).json({ error: 'Failed to check payout status' });
  }
});

// Stripe Connect dashboard link — lets referrers view their Stripe Express dashboard
app.get('/api/referrals/connect-dashboard', requireAuth, async (req, res) => {
  if (!stripe) return res.status(503).json({ error: 'Payments not configured' });
  const code = dbHelpers.prepare('SELECT * FROM referral_codes WHERE user_id = ? AND active = 1').get(req.user.id);
  if (!code || !code.stripe_connect_id) return res.status(403).json({ error: 'Payout not set up' });

  try {
    const loginLink = await stripe.accounts.createLoginLink(code.stripe_connect_id);
    res.json({ url: loginLink.url });
  } catch (err) {
    console.error('[CONNECT] dashboard link error:', err.message);
    res.status(500).json({ error: 'Failed to open payout dashboard' });
  }
});

// --- User Data Save/Load (server-side persistence) ---
// Size caps for durable autosave writes (H1). The outer express.json limit
// is 10MB but that's the abuse ceiling; real release bundles are < 64KB.
const USER_DATA_MAX_VALUE_BYTES = 256 * 1024;   // per-key value
const USER_DATA_MAX_BATCH_ITEMS = 50;           // items per save-batch
const USER_DATA_MAX_BATCH_BYTES = 2 * 1024 * 1024; // total serialized batch payload
function _serializeValue(value) {
  return typeof value === 'string' ? value : JSON.stringify(value);
}
// Detect new releases in release_data_all and auto-queue for registration + Slack notify
function detectNewReleases(userId, serializedValue) {
  try {
    let releases;
    try { releases = JSON.parse(serializedValue); } catch (e) { return; }
    if (!releases || typeof releases !== 'object') return;

    const user = dbHelpers.prepare('SELECT email, subscription_tier FROM users WHERE id = ?').get(userId);
    const email = user ? user.email : 'unknown';
    const isElite = user && ['elite', 'elite_plus'].includes(user.subscription_tier);

    for (const [title, release] of Object.entries(releases)) {
      if (!title || !release) continue;

      // Queue for ALL platforms, not just BMI
      let isNewRelease = false;
      const allMissing = {}; // platform → missing fields
      for (const platform of ALL_PLATFORMS) {
        const existing = dbHelpers.prepare(
          'SELECT id, slack_notified FROM registration_queue WHERE user_id = ? AND release_title = ? AND platform = ?'
        ).get(userId, title, platform);
        const completeness = checkPlatformCompleteness(release, platform);

        if (existing) {
          // Update completeness on re-save
          dbHelpers.prepare('UPDATE registration_queue SET field_completeness = ?, missing_fields = ? WHERE id = ?')
            .run(String(completeness.pct), JSON.stringify(completeness.missing), existing.id);
          if (completeness.missing.length === 0 && existing.slack_notified) {
            notifySlack(`:white_check_mark: *${title}* — ${PLATFORM_DEFS[platform].shortName} fields complete!`);
          }
        } else {
          isNewRelease = true;
          dbHelpers.prepare(`
            INSERT OR IGNORE INTO registration_queue (user_id, release_title, platform, status, field_completeness, missing_fields)
            VALUES (?, ?, ?, 'pending', ?, ?)
          `).run(userId, title, platform, String(completeness.pct), JSON.stringify(completeness.missing));
        }
        if (completeness.missing.length > 0) allMissing[platform] = completeness.missing;
      }

      if (isNewRelease) {
        // Summary Slack notification
        const platformSummary = ALL_PLATFORMS.map(p => {
          const c = checkPlatformCompleteness(release, p);
          return `${PLATFORM_DEFS[p].shortName}: ${c.complete ? ':white_check_mark:' : c.pct + '%'}`;
        }).join('  |  ');
        notifySlack(`:musical_note: New release: *${title}* by ${release.primaryArtist || 'Unknown'}\nUser: ${email}\n${platformSummary}`);
      }

      // Auto-DM Elite users if fields are missing and they haven't been notified yet.
      // Triggers on first save AND on re-saves (e.g., after generating a campaign with gaps).
      if (isElite && Object.keys(allMissing).length > 0 && SLACK_BOT_TOKEN) {
        const anyUnnotified = ALL_PLATFORMS.some(p => {
          const row = dbHelpers.prepare('SELECT slack_notified FROM registration_queue WHERE user_id = ? AND release_title = ? AND platform = ?').get(userId, title, p);
          return row && !row.slack_notified && allMissing[p];
        });
        if (anyUnnotified) {
          askEliteUserForMissingFields(userId, title, release, allMissing);
        }
      }
    }
  } catch (e) {
    console.error('[REG-DETECT] Error detecting new releases:', e.message);
  }
}

app.post('/api/data/save', requireAuth, (req, res) => {
  const { key, value } = req.body;
  if (!key) return res.status(400).json({ error: 'Key required' });
  const serialized = _serializeValue(value);
  if (Buffer.byteLength(serialized, 'utf8') > USER_DATA_MAX_VALUE_BYTES) {
    return res.status(413).json({ error: 'Value too large (max 256 KB per key)' });
  }
  dbHelpers.prepare(`
    INSERT INTO user_data (user_id, key, value, updated_at, version) VALUES (?, ?, ?, datetime('now'), 1)
    ON CONFLICT(user_id, key) DO UPDATE SET value = excluded.value, updated_at = datetime('now'), version = COALESCE(version, 0) + 1
  `).run(req.user.id, key, serialized);
  flushDbNow(); // durable user work — C2
  if (key === 'release_data_all') detectNewReleases(req.user.id, serialized);
  res.json({ success: true });
});

app.post('/api/data/save-batch', requireAuth, (req, res) => {
  const { items } = req.body;
  if (!items || !Array.isArray(items)) return res.status(400).json({ error: 'Items array required' });
  // H1 caps: bound item count, per-value size, and total serialized payload
  // so a buggy/malicious client can't hog sql.js memory or stall the flush.
  if (items.length > USER_DATA_MAX_BATCH_ITEMS) {
    return res.status(413).json({ error: 'Too many items (max ' + USER_DATA_MAX_BATCH_ITEMS + ')' });
  }
  const preparedItems = [];
  let totalBytes = 0;
  for (const { key, value } of items) {
    if (!key) continue;
    const serialized = _serializeValue(value);
    const bytes = Buffer.byteLength(serialized, 'utf8');
    if (bytes > USER_DATA_MAX_VALUE_BYTES) {
      return res.status(413).json({ error: 'Value for key "' + key + '" too large (max 256 KB)' });
    }
    totalBytes += bytes;
    if (totalBytes > USER_DATA_MAX_BATCH_BYTES) {
      return res.status(413).json({ error: 'Batch too large (max 2 MB total)' });
    }
    preparedItems.push({ key, serialized });
  }
  const saveBatch = dbHelpers.transaction(() => {
    for (const { key, serialized } of preparedItems) {
      dbHelpers.prepare(`
        INSERT INTO user_data (user_id, key, value, updated_at, version) VALUES (?, ?, ?, datetime('now'), 1)
        ON CONFLICT(user_id, key) DO UPDATE SET value = excluded.value, updated_at = datetime('now'), version = COALESCE(version, 0) + 1
      `).run(req.user.id, key, serialized);
    }
  });
  saveBatch();
  flushDbNow(); // durable user work — C2
  // Detect new releases if release_data_all was in the batch
  const relItem = preparedItems.find(i => i.key === 'release_data_all');
  if (relItem) detectNewReleases(req.user.id, relItem.serialized);
  res.json({ success: true });
});

app.get('/api/data/load', requireAuth, (req, res) => {
  const rows = dbHelpers.prepare('SELECT key, value FROM user_data WHERE user_id = ?').all(req.user.id);
  const data = {};
  for (const row of rows) {
    try { data[row.key] = JSON.parse(row.value); } catch(e) { data[row.key] = row.value; }
  }
  res.json(data);
});

// --- Stripe Routes ---
app.post('/api/create-checkout', requireAuth, async (req, res) => {
  if (!stripe) return res.status(503).json({ error: 'Payments not configured yet' });

  // Tier selection: 'pro' (monthly $14.99, 7-day trial), 'elite' ($1200/yr,
  // immediate charge, manual distribution + registration), 'elite_plus'
  // ($3000/yr, immediate charge, everything in elite + outreach + playlist
  // curation). Default to 'pro' for backwards compat with old subscribe.html.
  const requestedTier = (req.body && typeof req.body.tier === 'string') ? req.body.tier : 'pro';
  if (!['pro', 'elite', 'elite_plus'].includes(requestedTier)) {
    return res.status(400).json({ error: 'Invalid tier' });
  }
  const priceId = TIER_PRICE_MAP[requestedTier];
  if (!priceId) {
    return res.status(503).json({ error: 'This tier is not available yet' });
  }

  try {
    let customerId = req.user.stripe_customer_id;
    if (!customerId) {
      const customer = await stripe.customers.create({ email: req.user.email });
      customerId = customer.id;
      dbHelpers.prepare('UPDATE users SET stripe_customer_id = ? WHERE id = ?').run(customerId, req.user.id);
    }

    // Always use the trusted custom domain — never req.get('host'), which
    // is attacker-controllable via the Host header. (H7)
    const baseUrl = `https://${CUSTOM_DOMAIN}`;
    // Elite tiers go straight to the credential onboarding form on success.
    // Pro stays on the existing subscribe?success=1 → main app flow.
    const successUrl = isEliteTier(requestedTier)
      ? `${baseUrl}/elite-onboarding?session_id={CHECKOUT_SESSION_ID}`
      : `${baseUrl}/subscribe?success=1`;
    const subscriptionData = {
      metadata: { tier: requestedTier }
    };
    // Pro gets a 7-day free trial ONLY if the user has never had one
    // (subscription_status is null/empty — fresh account that somehow
    // skipped the registration trial, or a direct subscribe-page visit).
    // Users who are already trialing (from registration) or who previously
    // had a subscription (canceled, past_due) pay immediately — no double
    // free trial. Elite tiers are always immediate (manual labor per order).
    if (requestedTier === 'pro' && !req.user.subscription_status) {
      subscriptionData.trial_period_days = 7;
    }

    const session = await stripe.checkout.sessions.create({
      customer: customerId,
      mode: 'subscription',
      payment_method_types: ['card'],
      line_items: [{ price: priceId, quantity: 1 }],
      subscription_data: subscriptionData,
      metadata: { tier: requestedTier },
      success_url: successUrl,
      cancel_url: `${baseUrl}/subscribe?canceled=1`,
    });

    res.json({ url: session.url });
  } catch (err) {
    console.error('Stripe error:', err.message);
    res.status(500).json({ error: 'Failed to create checkout session' });
  }
});

// Stripe webhook
app.post('/webhook', express.raw({ type: 'application/json' }), (req, res) => {
  if (!stripe || !STRIPE_WEBHOOK_SECRET) return res.sendStatus(400);

  let event;
  try {
    event = stripe.webhooks.constructEvent(req.body, req.headers['stripe-signature'], STRIPE_WEBHOOK_SECRET);
  } catch (err) {
    console.error('Webhook signature verification failed:', err.message);
    return res.sendStatus(400);
  }

  // Idempotency: Stripe retries on any non-2xx, so the same event.id can
  // arrive multiple times. Bail early on duplicates so we never double-fire
  // future side effects (welcome emails, XP grants, etc.). (H5)
  const dup = dbHelpers.prepare('SELECT id FROM stripe_events WHERE id = ?').get(event.id);
  if (dup) return res.sendStatus(200);
  try {
    dbHelpers.prepare('INSERT INTO stripe_events (id, type) VALUES (?, ?)').run(event.id, event.type);
  } catch(_) { /* race: another worker processed it — treat as duplicate */ return res.sendStatus(200); }

  const obj = event.data.object;

  try {
    switch (event.type) {
      case 'customer.subscription.created':
      case 'customer.subscription.updated': {
        const status = obj.status === 'trialing' ? 'trialing' : (obj.status === 'active' ? 'active' : obj.status);
        const trialEnd = obj.trial_end ? new Date(obj.trial_end * 1000).toISOString() : null;
        // Resolve tier from the price ID on the subscription. Falls back to
        // the metadata.tier we attached at checkout if the price ID lookup
        // misses (e.g., env var not set on this instance).
        const priceId = obj.items && obj.items.data && obj.items.data[0] && obj.items.data[0].price && obj.items.data[0].price.id;
        let tier = tierFromPriceId(priceId);
        if (!tier && obj.metadata && obj.metadata.tier && ['pro','elite','elite_plus'].includes(obj.metadata.tier)) {
          tier = obj.metadata.tier;
        }
        if (!tier) tier = 'pro';
        dbHelpers.prepare('UPDATE users SET subscription_status = ?, stripe_subscription_id = ?, trial_ends_at = ?, subscription_tier = ?, updated_at = datetime("now") WHERE stripe_customer_id = ?')
          .run(status, obj.id, trialEnd, tier, obj.customer);

        // --- Referral commission: create on first active subscription ---
        if (event.type === 'customer.subscription.created' && (status === 'active' || status === 'trialing')) {
          try {
            const subUser = dbHelpers.prepare('SELECT id, email, referred_by FROM users WHERE stripe_customer_id = ? AND deleted_at IS NULL').get(obj.customer);
            if (subUser && subUser.referred_by) {
              const refRow = dbHelpers.prepare('SELECT * FROM referral_codes WHERE code = ? AND active = 1').get(subUser.referred_by);
              if (refRow) {
                // Prevent duplicate commission for same user+subscription
                const existingCommission = dbHelpers.prepare('SELECT id FROM referral_commissions WHERE referred_user_id = ? AND stripe_subscription_id = ?').get(subUser.id, obj.id);
                if (!existingCommission) {
                  const TIER_AMOUNTS_CENTS = { pro: 1499, elite: 120000, elite_plus: 300000 };
                  const amountCents = TIER_AMOUNTS_CENTS[tier] || 1499;
                  const commissionCents = Math.round(amountCents * refRow.commission_rate);
                  dbHelpers.prepare(`
                    INSERT INTO referral_commissions (referrer_id, referred_user_id, referral_code, subscription_tier, amount_cents, commission_cents, status, stripe_subscription_id)
                    VALUES (?, ?, ?, ?, ?, ?, 'pending', ?)
                  `).run(refRow.user_id, subUser.id, refRow.code, tier, amountCents, commissionCents, obj.id);
                  logOperation(null, 'referral.commission_created', 'user', subUser.id, {
                    referrer_id: refRow.user_id, code: refRow.code, tier, amountCents, commissionCents
                  });
                  // Notify admin
                  const referrer = dbHelpers.prepare('SELECT email FROM users WHERE id = ?').get(refRow.user_id);
                  notifyAdmins(
                    `Referral Commission — ${subUser.email} subscribed via ${refRow.code}`,
                    `<h2 style="color:#fff; margin-top:0;">New Referral Commission</h2>
                     <p><strong>New subscriber:</strong> ${subUser.email}</p>
                     <p><strong>Referral code:</strong> ${refRow.code}</p>
                     <p><strong>Referrer:</strong> ${referrer ? referrer.email : 'user#' + refRow.user_id}</p>
                     <p><strong>Tier:</strong> ${tier}</p>
                     <p><strong>Subscription amount:</strong> $${(amountCents / 100).toFixed(2)}</p>
                     <p><strong>Commission (${Math.round(refRow.commission_rate * 100)}%):</strong> $${(commissionCents / 100).toFixed(2)}</p>`
                  ).catch(() => {});
                }
              }
            }
          } catch (e) {
            console.error('[REFERRAL] commission tracking error:', e.message);
          }
        }

        break;
      }
      case 'customer.subscription.deleted': {
        // Reset tier back to 'pro' so a future re-subscribe at the monthly
        // tier doesn't inherit a stale 'elite' flag.
        dbHelpers.prepare('UPDATE users SET subscription_status = ?, stripe_subscription_id = NULL, subscription_tier = ?, updated_at = datetime("now") WHERE stripe_customer_id = ?')
          .run('canceled', 'pro', obj.customer);
        break;
      }
      case 'invoice.payment_failed': {
        // Mark as past_due so the UI can prompt the user to update billing.
        dbHelpers.prepare('UPDATE users SET subscription_status = ?, updated_at = datetime("now") WHERE stripe_customer_id = ?')
          .run('past_due', obj.customer);
        break;
      }
      case 'checkout.session.completed': {
        // Defensive: tie the customer ID back to the user if it's somehow not set yet.
        // Stripe doesn't normalize customer_email casing, so match case-insensitively.
        if (obj.customer && obj.customer_email) {
          dbHelpers.prepare('UPDATE users SET stripe_customer_id = ? WHERE LOWER(email) = ? AND (stripe_customer_id IS NULL OR stripe_customer_id = "")')
            .run(obj.customer, obj.customer_email.toLowerCase());
        }
        // Redemption Release one-time payments. Mark the row paid and create
        // an escalated admin ticket so Joseph sees the new order in the queue.
        // Subscription checkouts (Pro/Elite) hit this same case but skip this
        // branch because they don't carry metadata.kind.
        if (obj.metadata && obj.metadata.kind === 'redemption') {
          const redemptionId = parseInt(obj.metadata.redemption_id, 10);
          if (Number.isInteger(redemptionId)) {
            dbHelpers.prepare(`
              UPDATE redemption_requests
              SET status = 'paid',
                  stripe_session_id = ?,
                  stripe_payment_intent_id = ?,
                  paid_at = datetime('now')
              WHERE id = ? AND status = 'pending'
            `).run(obj.id, obj.payment_intent || null, redemptionId);

            const row = dbHelpers.prepare('SELECT * FROM redemption_requests WHERE id = ?').get(redemptionId);
            if (row) {
              const userRow = dbHelpers.prepare('SELECT email FROM users WHERE id = ?').get(row.user_id);
              const customerEmail = userRow ? userRow.email : `user#${row.user_id}`;
              // Pretty-print the registration checklist for the ticket body.
              const REG_LABELS = {
                pro: 'PRO (ASCAP/BMI/SESAC/etc)',
                mlc: 'MLC (mechanical streaming royalties)',
                soundexchange: 'SoundExchange (digital performance)',
                youtube_content_id: 'YouTube Content ID',
                neighboring_rights: 'Neighboring rights (international)',
                publishing_admin: 'Publishing administration',
                mechanical_licensing: 'Mechanical licensing (covers)',
                sync_registration: 'Sync agency registration',
                isrc_upc: 'ISRC / UPC assignment'
              };
              let regsList = '(none specified)';
              if (row.registrations_missing) {
                try {
                  const parsed = JSON.parse(row.registrations_missing);
                  if (Array.isArray(parsed) && parsed.length) {
                    regsList = parsed.map(k => `  - ${REG_LABELS[k] || k}`).join('\n');
                  }
                } catch(_) { /* leave default */ }
              }
              const subject = `Redemption Release paid: ${row.release_title}`;
              const message = [
                `New Redemption Release order — $49.99 paid.`,
                ``,
                `Customer: ${customerEmail}`,
                `Release: ${row.release_title}`,
                `Artist: ${row.artist_name || '—'}`,
                `Songwriter(s): ${row.songwriter_names || '—'}`,
                `PRO affiliation: ${row.pro_affiliation || '—'}`,
                `ISRC: ${row.isrc || '—'}`,
                `DSP link: ${row.dsp_link || '—'}`,
                `Original distributor: ${row.original_distributor || '—'}`,
                `Original release date: ${row.original_release_date || '—'}`,
                `Still live on DSPs: ${row.still_live || '—'}`,
                ``,
                `Registrations missing / needed:`,
                regsList,
                ``,
                row.what_went_wrong ? `Context from artist:\n${row.what_went_wrong}` : '',
                row.extra_notes ? `\nExtra notes:\n${row.extra_notes}` : '',
                ``,
                `Manage in Master Admin → Redemption Requests, or via /api/admin/redemptions/${redemptionId}.`
              ].filter(Boolean).join('\n');
              try {
                dbHelpers.prepare(`
                  INSERT INTO support_tickets (user_id, user_email, subject, message, status, escalated)
                  VALUES (?, ?, ?, ?, 'open', 1)
                `).run(row.user_id, customerEmail, subject, message);
              } catch (e) {
                console.error('[REDEMPTION] ticket create error:', e.message);
                // Best-effort: row is already paid; ticket is just notification.
              }
            }
          }
        }
        // Outreach List one-time purchase. Insert the unlock row keyed on
        // user_id (unique index) — if Stripe retries the webhook, the
        // INSERT OR IGNORE keeps it idempotent. Amount is captured for
        // support lookups (trial buyers paid $250, Pro buyers $100).
        if (obj.metadata && obj.metadata.kind === 'outreach_list') {
          const userId = parseInt(obj.metadata.user_id, 10);
          if (Number.isInteger(userId)) {
            try {
              dbHelpers.prepare(`
                INSERT OR IGNORE INTO outreach_purchases
                  (user_id, stripe_session_id, stripe_payment_intent_id, amount_cents, price_id)
                VALUES (?, ?, ?, ?, ?)
              `).run(
                userId,
                obj.id,
                obj.payment_intent || null,
                obj.amount_total != null ? obj.amount_total : null,
                obj.metadata.price_id || null
              );
            } catch (e) {
              console.error('[OUTREACH] purchase insert error:', e.message);
            }
          }
        }
        break;
      }
    }
  } catch (err) {
    console.error('[STRIPE] Handler error for', event.type, err.message);
    // Roll back the dedupe row so Stripe will retry.
    dbHelpers.prepare('DELETE FROM stripe_events WHERE id = ?').run(event.id);
    return res.sendStatus(500);
  }

  // Journal the webhook event for audit trail.
  logOperation(null, 'stripe.webhook', 'stripe_event', event.id, { type: event.type, customer: obj.customer || null });

  // Flush DB to disk synchronously after a successful webhook so we don't
  // lose subscription state if the process dies inside the 100ms debounce.
  try { flushDbNow(); } catch (e) { console.error('[STRIPE] flushDbNow failed:', e.message); }

  res.sendStatus(200);
});

// --- Beta Spots Countdown ---
// Returns how many of the 10 beta artist slots are taken (active subscribers only).
const BETA_ARTIST_LIMIT = 10;
app.get('/api/beta-spots', (req, res) => {
  const row = dbHelpers.prepare(
    "SELECT COUNT(*) AS cnt FROM users WHERE subscription_status = 'active' AND role != 'admin' AND deleted_at IS NULL"
  ).get();
  res.json({ total: BETA_ARTIST_LIMIT, taken: row.cnt });
});

// --- Subscribe Page ---
app.get('/subscribe', requireAuth, (req, res) => {
  res.sendFile(path.join(__dirname, 'subscribe.html'));
});

// --- Elite Onboarding Page ---
// Served to Elite/Elite Plus customers post-checkout to collect distribution
// + social account credentials. Pro/admin users get bounced back to the app.
app.get('/elite-onboarding', requireAuth, (req, res) => {
  if (req.user.role === 'admin') return res.redirect('/');
  if (!isEliteTier(req.user.subscription_tier)) return res.redirect('/');
  res.sendFile(path.join(__dirname, 'elite-onboarding.html'));
});

// Submit credentials. Encrypts everything at rest, marks the user as
// onboarded, and creates an escalated admin support ticket so Joseph sees
// the new order in the admin queue.
app.post('/api/elite/onboarding', requireAuth, (req, res) => {
  if (!isEliteTier(req.user.subscription_tier)) {
    return res.status(403).json({ error: 'Elite subscription required' });
  }
  const body = req.body || {};

  // All registration credentials are optional at onboarding — artists can
  // add them later from the Registration tab in the app.

  // Whitelist + length-cap every field. We never want a malicious or buggy
  // client to dump arbitrary blobs into the encrypted store.
  const cap = (v, n) => (v == null ? '' : String(v)).slice(0, n);
  const data = {
    distribution: {
      provider: cap(body.distribution_provider, 80),
      username: cap(body.distribution_username, 200),
      password: cap(body.distribution_password, 200),
      notes: cap(body.distribution_notes, 1000)
    },
    registration: {
      pro_org: cap(body.registration_pro, 80),         // ASCAP/BMI/SESAC
      username: cap(body.registration_username, 200),
      password: cap(body.registration_password, 200),
      ipi_number: cap(body.registration_ipi, 80),
      soundexchange_username: cap(body.soundexchange_username, 200),
      soundexchange_password: cap(body.soundexchange_password, 200),
      musixmatch_username: cap(body.musixmatch_username, 200),
      musixmatch_password: cap(body.musixmatch_password, 200),
      mlc_username: cap(body.mlc_username, 200),
      mlc_password: cap(body.mlc_password, 200),
      songtrust_username: cap(body.songtrust_username, 200),
      songtrust_password: cap(body.songtrust_password, 200)
    },
    social: {},
    cadence: {
      release_frequency: cap(body.release_frequency, 200),
      next_release_date: cap(body.next_release_date, 50),
      genre: cap(body.genre, 100)
    },
    elite_plus: {
      email_provider: cap(body.email_provider, 80),
      email_address: cap(body.email_access_address, 300),
      email_password: cap(body.email_access_password, 200),
      email_notes: cap(body.email_access_notes, 500),
      target_stations: cap(body.target_stations, 2000),
      avoid_stations: cap(body.avoid_stations, 2000),
      target_playlists: cap(body.target_playlists, 2000),
      playlist_mood: cap(body.playlist_mood, 500),
      submission_strategy: cap(body.submission_strategy, 1000)
    },
    extra_notes: cap(body.extra_notes, 3000)
  };

  let enc;
  try { enc = encryptOnboarding(data); }
  catch (e) {
    console.error('[ELITE] encrypt error:', e.message);
    return res.status(500).json({ error: 'Failed to save onboarding' });
  }

  // Upsert: a user can re-submit to update creds (e.g., password change).
  const existing = dbHelpers.prepare('SELECT user_id FROM elite_onboarding WHERE user_id = ?').get(req.user.id);
  if (existing) {
    dbHelpers.prepare(`
      UPDATE elite_onboarding
      SET tier = ?, data_encrypted = ?, iv = ?, auth_tag = ?, updated_at = datetime('now')
      WHERE user_id = ?
    `).run(req.user.subscription_tier, enc.data_encrypted, enc.iv, enc.auth_tag, req.user.id);
  } else {
    dbHelpers.prepare(`
      INSERT INTO elite_onboarding (user_id, tier, data_encrypted, iv, auth_tag)
      VALUES (?, ?, ?, ?, ?)
    `).run(req.user.id, req.user.subscription_tier, enc.data_encrypted, enc.iv, enc.auth_tag);
  }
  dbHelpers.prepare('UPDATE users SET onboarding_completed = 1, updated_at = datetime("now") WHERE id = ?').run(req.user.id);

  // Auto-create escalated admin ticket as the notification mechanism. Joseph
  // already monitors the admin ticket queue, so this surfaces new Elite
  // orders without a separate notification system.
  const tierLabel = req.user.subscription_tier === 'elite_plus' ? 'Elite Plus' : 'Elite';
  const subject = `${tierLabel} onboarding submitted: ${req.user.email}`;
  const message = [
    `New ${tierLabel} customer completed onboarding.`,
    ``,
    `Email: ${req.user.email}`,
    `User ID: ${req.user.id}`,
    `Tier: ${req.user.subscription_tier}`,
    ``,
    `View encrypted credentials in Master Admin → Elite Onboarding.`,
    `Or fetch via: GET /api/admin/elite-onboarding/${req.user.id}`,
    ``,
    `Manual work to start:`,
    req.user.subscription_tier === 'elite_plus'
      ? `- Distribution + registration\n- Outreach emails (radio, podcasts)\n- Playlist curation`
      : `- Distribution + registration`
  ].join('\n');
  try {
    dbHelpers.prepare(`
      INSERT INTO support_tickets (user_id, user_email, subject, message, status, escalated)
      VALUES (?, ?, ?, ?, 'open', 1)
    `).run(req.user.id, req.user.email, subject, message);
  } catch (e) {
    console.error('[ELITE] ticket create error:', e.message);
    // Don't fail the request — onboarding is saved, ticket is best-effort.
  }

  logOperation(req, 'elite.onboarding_submit', 'user', req.user.id, { tier: req.user.subscription_tier });
  flushDbNow();

  // Email admin — fire-and-forget, never blocks the response.
  notifyAdmins(
    `${tierLabel} Onboarding Submitted — ${req.user.email}`,
    `<h2 style="color:#fff; margin-top:0;">New ${tierLabel} Onboarding</h2>
     <p><strong>Email:</strong> ${req.user.email}</p>
     <p><strong>User ID:</strong> ${req.user.id}</p>
     <p><strong>Tier:</strong> ${req.user.subscription_tier}</p>
     <p style="margin-top:16px;">View encrypted credentials in <strong>Master Admin → Elite Onboarding</strong>.</p>
     <p><strong>Manual work to start:</strong></p>
     <ul>${req.user.subscription_tier === 'elite_plus'
       ? '<li>Distribution + registration</li><li>Outreach emails (radio, podcasts)</li><li>Playlist curation</li>'
       : '<li>Distribution + registration</li>'}</ul>`
  ).catch(() => {});

  res.json({ success: true });
});

// --- Elite Credential Status (for Registration tab) ---
// Returns which services have credentials on file (no secrets exposed).
app.get('/api/elite/credential-status', requireAuth, (req, res) => {
  if (!isEliteTier(req.user.subscription_tier)) return res.json({ elite: false });
  const row = dbHelpers.prepare('SELECT data_encrypted, iv, auth_tag FROM elite_onboarding WHERE user_id = ?').get(req.user.id);
  if (!row) return res.json({ elite: true, hasOnboarding: false, services: {} });
  try {
    const data = decryptOnboarding(row);
    const has = (u, p) => !!(u && u.trim()) && !!(p && p.trim());
    const services = {
      distribution: has(data.distribution?.username, data.distribution?.password),
      pro: has(data.registration?.username, data.registration?.password),
      soundexchange: has(data.registration?.soundexchange_username, data.registration?.soundexchange_password),
      musixmatch: has(data.registration?.musixmatch_username, data.registration?.musixmatch_password),
      mlc: has(data.registration?.mlc_username, data.registration?.mlc_password),
      songtrust: has(data.registration?.songtrust_username, data.registration?.songtrust_password)
    };
    // Elite Plus also tracks email access
    const tier = dbHelpers.prepare('SELECT subscription_tier FROM users WHERE id = ?').get(req.user.id);
    if (tier && tier.subscription_tier === 'elite_plus') {
      services.email = has(data.elite_plus?.email_address, data.elite_plus?.email_password);
    }
    res.json({ elite: true, hasOnboarding: true, tier: tier?.subscription_tier, services });
  } catch (e) {
    console.error('[ELITE] decrypt error:', e.message);
    res.json({ elite: true, hasOnboarding: true, services: {} });
  }
});

// Update a single service credential from the Registration tab.
app.post('/api/elite/update-credential', requireAuth, (req, res) => {
  if (!isEliteTier(req.user.subscription_tier)) {
    return res.status(403).json({ error: 'Elite subscription required' });
  }
  const { service, username, password } = req.body || {};
  if (!service || !username || !password) {
    return res.status(400).json({ error: 'Service, username, and password are required.' });
  }
  const cap = (v, n) => (v == null ? '' : String(v)).slice(0, n);
  const svc = cap(service, 40);
  const user = cap(username, 200);
  const pass = cap(password, 200);

  // Load existing onboarding data (or start fresh).
  const row = dbHelpers.prepare('SELECT data_encrypted, iv, auth_tag FROM elite_onboarding WHERE user_id = ?').get(req.user.id);
  let data;
  try {
    data = row ? decryptOnboarding(row) : {
      distribution: {}, registration: {}, social: {},
      cadence: {}, elite_plus: {}, extra_notes: ''
    };
  } catch (e) {
    return res.status(500).json({ error: 'Failed to load existing credentials' });
  }

  // Map service name to the correct nested field.
  const fieldMap = {
    distribution: ['distribution', 'username', 'password'],
    pro: ['registration', 'username', 'password'],
    soundexchange: ['registration', 'soundexchange_username', 'soundexchange_password'],
    musixmatch: ['registration', 'musixmatch_username', 'musixmatch_password'],
    mlc: ['registration', 'mlc_username', 'mlc_password'],
    songtrust: ['registration', 'songtrust_username', 'songtrust_password'],
    email: ['elite_plus', 'email_address', 'email_password']
  };
  const mapping = fieldMap[svc];
  if (!mapping) return res.status(400).json({ error: 'Unknown service' });

  if (!data[mapping[0]]) data[mapping[0]] = {};
  data[mapping[0]][mapping[1]] = user;
  data[mapping[0]][mapping[2]] = pass;

  let enc;
  try { enc = encryptOnboarding(data); }
  catch (e) { return res.status(500).json({ error: 'Encryption failed' }); }

  if (row) {
    dbHelpers.prepare(`
      UPDATE elite_onboarding SET data_encrypted = ?, iv = ?, auth_tag = ?, updated_at = datetime('now')
      WHERE user_id = ?
    `).run(enc.data_encrypted, enc.iv, enc.auth_tag, req.user.id);
  } else {
    dbHelpers.prepare(`
      INSERT INTO elite_onboarding (user_id, tier, data_encrypted, iv, auth_tag)
      VALUES (?, ?, ?, ?, ?)
    `).run(req.user.id, req.user.subscription_tier, enc.data_encrypted, enc.iv, enc.auth_tag);
    dbHelpers.prepare('UPDATE users SET onboarding_completed = 1, updated_at = datetime("now") WHERE id = ?').run(req.user.id);
  }

  logOperation(req, 'elite.credential_update', 'user', req.user.id, { service: svc });
  flushDbNow();
  res.json({ success: true });
});

// --- Redemption Release ($49.99 one-time service) ---
// Trial users can VIEW the page (so they see what they're missing) but only
// Pro/Elite/admin can submit. The form POST is gated by requireActive, so
// trial users get a 402 and the frontend routes them to /subscribe.
app.get('/redemption', requireAuth, (req, res) => {
  res.sendFile(path.join(__dirname, 'redemption.html'));
});

app.post('/api/redemption/request', requireActive, async (req, res) => {
  if (!stripe) return res.status(503).json({ error: 'Payments not configured yet' });
  if (!STRIPE_REDEMPTION_PRICE_ID) {
    return res.status(503).json({ error: 'Redemption Release is not available yet' });
  }

  const body = req.body || {};
  const cap = (v, n) => (v == null ? '' : String(v)).slice(0, n).trim();
  const release_title = cap(body.release_title, 200);
  if (!release_title) return res.status(400).json({ error: 'Release title is required' });

  const stillLiveRaw = cap(body.still_live, 20).toLowerCase();
  const still_live = ['yes','no','unsure'].includes(stillLiveRaw) ? stillLiveRaw : '';

  // Whitelist PRO affiliations so we don't get junk in the column.
  const proRaw = cap(body.pro_affiliation, 40);
  const PRO_OPTIONS = ['ASCAP','BMI','SESAC','GMR','SOCAN','PRS','None','Unsure','Other'];
  const pro_affiliation = PRO_OPTIONS.includes(proRaw) ? proRaw : '';

  // Registrations-missing checklist arrives as an array of string keys.
  // Whitelist them so we never store anything not in the known service set,
  // then serialize as JSON for storage.
  const KNOWN_REG_SERVICES = [
    'pro',                  // Performance rights org (ASCAP/BMI/etc)
    'mlc',                  // Mechanical Licensing Collective (US streaming mechanicals)
    'soundexchange',        // Non-interactive digital performance (sound recording)
    'youtube_content_id',   // YouTube Content ID
    'neighboring_rights',   // International neighboring rights (PPL UK, etc)
    'publishing_admin',     // Publishing administration / publisher registration
    'mechanical_licensing', // Cover song / outside mechanical licensing
    'sync_registration',    // Sync agency registration
    'isrc_upc'              // ISRC / UPC code assignment
  ];
  const inputRegs = Array.isArray(body.registrations_missing) ? body.registrations_missing : [];
  const cleanRegs = inputRegs
    .map(s => String(s).toLowerCase().trim())
    .filter(s => KNOWN_REG_SERVICES.includes(s));
  const registrations_missing = cleanRegs.length ? JSON.stringify(cleanRegs) : '';

  // The "what went wrong" field is now optional context — required field
  // is the registrations checklist. Require either at least one missing
  // registration OR a context paragraph so we don't get an empty submission.
  const what_went_wrong = cap(body.what_went_wrong, 3000);
  if (!cleanRegs.length && !what_went_wrong) {
    return res.status(400).json({ error: 'Tell us which registrations are missing, or describe what the release needs.' });
  }

  const row = {
    artist_name: cap(body.artist_name, 200),
    dsp_link: cap(body.dsp_link, 500),
    original_distributor: cap(body.original_distributor, 80),
    original_release_date: cap(body.original_release_date, 50),
    extra_notes: cap(body.extra_notes, 2000),
    isrc: cap(body.isrc, 50),
    songwriter_names: cap(body.songwriter_names, 500)
  };

  try {
    let customerId = req.user.stripe_customer_id;
    if (!customerId) {
      const customer = await stripe.customers.create({ email: req.user.email });
      customerId = customer.id;
      dbHelpers.prepare('UPDATE users SET stripe_customer_id = ? WHERE id = ?').run(customerId, req.user.id);
    }

    const insert = dbHelpers.prepare(`
      INSERT INTO redemption_requests
        (user_id, release_title, artist_name, dsp_link, original_distributor,
         original_release_date, still_live, what_went_wrong, extra_notes,
         isrc, songwriter_names, pro_affiliation, registrations_missing, status)
      VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'pending')
    `).run(
      req.user.id, release_title, row.artist_name, row.dsp_link, row.original_distributor,
      row.original_release_date, still_live, what_went_wrong, row.extra_notes,
      row.isrc, row.songwriter_names, pro_affiliation, registrations_missing
    );
    const redemptionId = insert.lastInsertRowid;
    logOperation(req, 'redemption.created', 'redemption_request', redemptionId, { release_title });
    flushDbNow();

    const baseUrl = `https://${CUSTOM_DOMAIN}`;
    const session = await stripe.checkout.sessions.create({
      customer: customerId,
      mode: 'payment',
      payment_method_types: ['card'],
      line_items: [{ price: STRIPE_REDEMPTION_PRICE_ID, quantity: 1 }],
      metadata: {
        kind: 'redemption',
        redemption_id: String(redemptionId),
        user_id: String(req.user.id)
      },
      success_url: `${baseUrl}/redemption?success=1&id=${redemptionId}`,
      cancel_url: `${baseUrl}/redemption?canceled=1`
    });

    // Stash the session id on the row so we can correlate if the webhook is delayed.
    dbHelpers.prepare('UPDATE redemption_requests SET stripe_session_id = ? WHERE id = ?')
      .run(session.id, redemptionId);
    flushDbNow();

    res.json({ url: session.url });
  } catch (err) {
    console.error('[REDEMPTION] checkout error:', err.message);
    res.status(500).json({ error: 'Failed to start checkout' });
  }
});

// --- Outreach List purchase (one-time Stripe Checkout) ---
// Trial users pay $250, Pro/Elite users pay $100. The $250 path is
// intentional — trial users get the list AND the AI intro generator even
// though they can't hit /api/claude directly. The purchase IS the access
// grant for the Outreach List sub-product (see R9 in task/lessons.md).
// No refunds (product decision); once purchased, unlocked forever.
app.post('/api/outreach/purchase', requireAuth, async (req, res) => {
  if (!stripe) return res.status(503).json({ error: 'Payments not configured yet' });
  if (!STRIPE_OUTREACH_PRICE_TRIAL || !STRIPE_OUTREACH_PRICE_PRO) {
    return res.status(503).json({ error: 'Outreach List is not available yet' });
  }

  const user = req.user;
  // Admins don't need to purchase — they already have access via
  // requireOutreachUnlocked's role check.
  if (user.role === 'admin') {
    return res.status(400).json({ error: 'Admin accounts already have Outreach List access' });
  }

  // Idempotency — if the user already has a purchase row, short-circuit.
  const existing = dbHelpers.prepare('SELECT id FROM outreach_purchases WHERE user_id = ?').get(user.id);
  if (existing) {
    return res.status(400).json({ error: 'Outreach List already purchased', alreadyUnlocked: true });
  }

  // Active Pro/Elite/Elite Plus subscribers = $100. Everyone else
  // (trialing, canceled, past_due, no subscription) = $250. Trial
  // users must upgrade to a paid subscription to earn the discount.
  const priceId = user.subscription_status === 'active'
    ? STRIPE_OUTREACH_PRICE_PRO
    : STRIPE_OUTREACH_PRICE_TRIAL;

  try {
    let customerId = user.stripe_customer_id;
    if (!customerId) {
      const customer = await stripe.customers.create({ email: user.email });
      customerId = customer.id;
      dbHelpers.prepare('UPDATE users SET stripe_customer_id = ? WHERE id = ?').run(customerId, user.id);
      flushDbNow();
    }

    const baseUrl = `https://${CUSTOM_DOMAIN}`;
    const session = await stripe.checkout.sessions.create({
      customer: customerId,
      mode: 'payment',
      payment_method_types: ['card'],
      line_items: [{ price: priceId, quantity: 1 }],
      metadata: {
        kind: 'outreach_list',
        user_id: String(user.id),
        price_id: priceId
      },
      success_url: `${baseUrl}/?outreach_purchase=success`,
      cancel_url: `${baseUrl}/subscribe?outreach_canceled=1#outreach-list`
    });

    res.json({ url: session.url });
  } catch (err) {
    console.error('[OUTREACH] checkout error:', err.message);
    res.status(500).json({ error: 'Failed to start checkout' });
  }
});

// --- Research API (Serper web search) ---
const SERPER_API_KEY = process.env.SERPER_API_KEY || '';

async function serperSearch(query, num = 10) {
  if (!SERPER_API_KEY) throw new Error('Search API not configured');
  // 15s hard ceiling — Serper occasionally hangs and we don't want a single
  // slow upstream call to tie up an Express worker indefinitely.
  const resp = await fetch('https://google.serper.dev/search', {
    method: 'POST',
    headers: { 'X-API-KEY': SERPER_API_KEY, 'Content-Type': 'application/json' },
    body: JSON.stringify({ q: query, num }),
    signal: AbortSignal.timeout(15000)
  });
  if (!resp.ok) throw new Error('Search API failed');
  return resp.json();
}

// Simple in-memory cache (key -> { data, expires })
const researchCache = new Map();
function cacheGet(key) {
  const entry = researchCache.get(key);
  if (entry && Date.now() < entry.expires) return entry.data;
  researchCache.delete(key);
  return null;
}
function cacheSet(key, data, ttlMs = 3600000) {
  researchCache.set(key, { data, expires: Date.now() + ttlMs });
  // True LRU eviction. Map iteration order is insertion order, so the
  // first key is the oldest. Previously this only removed expired entries
  // and let fresh entries grow unbounded — a memory leak. (H8)
  while (researchCache.size > 200) {
    const oldest = researchCache.keys().next().value;
    researchCache.delete(oldest);
  }
}

// Trial users are allowed here — the release cap is client-side
// (TRIAL_RELEASE_LIMIT=5) and cost is bounded by rlResearch + the
// trialing daily token cap. See R11 in task/lessons.md.
app.post('/api/research', requireAccess, rlResearch, async (req, res) => {
  const { action, genre, artistName, query, similarArtists } = req.body;
  if (!action) return res.status(400).json({ error: 'Missing action' });

  const cacheKey = JSON.stringify({ action, genre, artistName, query, similarArtists });
  const cached = cacheGet(cacheKey);
  if (cached) return res.json(cached);

  try {
    let result;

    switch (action) {
      case 'findPlaylists': {
        if (!genre) return res.status(400).json({ error: 'Genre required' });
        const searches = await Promise.all([
          serperSearch(`${genre} spotify playlist submit song`),
          serperSearch(`${genre} independent artist playlist spotify`),
          serperSearch(`${genre} new music playlist curators accepting submissions`)
        ]);
        const playlists = [];
        for (const s of searches) {
          if (s.organic) {
            for (const r of s.organic) {
              playlists.push({ title: r.title, link: r.link, snippet: r.snippet || '' });
            }
          }
        }
        const seenDomains = new Set();
        result = { playlists: playlists.filter(p => {
          try { const domain = new URL(p.link).hostname + p.title; if (seenDomains.has(domain)) return false; seenDomains.add(domain); return true; }
          catch { return true; }
        })};
        break;
      }

      case 'findContacts': {
        if (!query) return res.status(400).json({ error: 'Search query required' });
        const searches = await Promise.all([
          serperSearch(`${query} email contact music submission`),
          serperSearch(`${query} instagram linktree music`)
        ]);
        const contacts = [];
        for (const s of searches) {
          if (s.organic) {
            for (const r of s.organic) {
              contacts.push({
                title: r.title,
                link: r.link,
                snippet: r.snippet || ''
              });
            }
          }
        }
        result = { contacts };
        break;
      }

      case 'findBlogs': {
        if (!genre) return res.status(400).json({ error: 'Genre required' });
        const searches = await Promise.all([
          serperSearch(`${genre} music blog submit song`),
          serperSearch(`${genre} music blog submission form 2026`),
          serperSearch(`indie ${genre} music review blog accepting submissions`)
        ]);
        const blogs = [];
        for (const s of searches) {
          if (s.organic) {
            for (const r of s.organic) {
              blogs.push({ title: r.title, link: r.link, snippet: r.snippet || '' });
            }
          }
        }
        // Dedupe by domain
        const seenDomains = new Set();
        result = { blogs: blogs.filter(b => {
          try { const domain = new URL(b.link).hostname; if (seenDomains.has(domain)) return false; seenDomains.add(domain); return true; }
          catch { return true; }
        })};
        break;
      }

      case 'findPodcasts': {
        if (!genre) return res.status(400).json({ error: 'Genre required' });
        const searches = await Promise.all([
          serperSearch(`${genre} music podcast guest interview`),
          serperSearch(`${genre} artist interview podcast submit`)
        ]);
        const podcasts = [];
        for (const s of searches) {
          if (s.organic) {
            for (const r of s.organic) {
              podcasts.push({ title: r.title, link: r.link, snippet: r.snippet || '' });
            }
          }
        }
        const seenDomains = new Set();
        result = { podcasts: podcasts.filter(p => {
          try { const domain = new URL(p.link).hostname; if (seenDomains.has(domain)) return false; seenDomains.add(domain); return true; }
          catch { return true; }
        })};
        break;
      }

      case 'analyzeCompetition': {
        if (!similarArtists || !similarArtists.length) return res.status(400).json({ error: 'Similar artists required' });
        const competitorSearches = [];
        for (const artist of similarArtists.slice(0, 5)) {
          competitorSearches.push(serperSearch(`"${artist}" spotify playlists featured on`));
          competitorSearches.push(serperSearch(`"${artist}" music blog feature interview`));
        }
        const allResults = await Promise.all(competitorSearches);
        const competitors = [];
        for (let i = 0; i < similarArtists.slice(0, 5).length; i++) {
          const playlistResults = allResults[i * 2]?.organic || [];
          const pressResults = allResults[i * 2 + 1]?.organic || [];
          competitors.push({
            name: similarArtists[i],
            playlists: playlistResults.slice(0, 5).map(r => ({ title: r.title, link: r.link, snippet: r.snippet || '' })),
            press: pressResults.slice(0, 5).map(r => ({ title: r.title, link: r.link, snippet: r.snippet || '' }))
          });
        }
        result = { competitors };
        break;
      }

      case 'socialScout': {
        if (!genre) return res.status(400).json({ error: 'Genre required' });
        const searches = await Promise.all([
          serperSearch(`${genre} music promotion instagram page`),
          serperSearch(`${genre} music tiktok influencer promoting artists`),
          serperSearch(`${genre} music repost page instagram`)
        ]);
        const accounts = [];
        for (const s of searches) {
          if (s.organic) {
            for (const r of s.organic) {
              accounts.push({ title: r.title, link: r.link, snippet: r.snippet || '' });
            }
          }
        }
        result = { accounts };
        break;
      }

      case 'tiktokTrends': {
        if (!genre) return res.status(400).json({ error: 'Genre required' });
        const searches = await Promise.all([
          serperSearch(`site:ads.tiktok.com/business/creativecenter/inspiration/popular/hashtag ${genre}`, 10),
          serperSearch(`tiktok creative center trending songs ${genre} music`, 10),
          serperSearch(`tiktok creative center trending ${genre} creators artists`, 10),
          serperSearch(`tiktok ${genre} trending sounds viral 2026`, 10),
          serperSearch(`tiktok ${genre} music trends what is trending now`, 10)
        ]);
        const trends = { hashtags: [], songs: [], creators: [], videos: [] };
        for (let i = 0; i < searches.length; i++) {
          const s = searches[i];
          const items = [];
          if (s.organic) {
            for (const r of s.organic) {
              items.push({ title: r.title, link: r.link, snippet: r.snippet || '' });
            }
          }
          if (i === 0) trends.hashtags = items;
          else if (i === 1) trends.songs = items;
          else if (i === 2) trends.creators = items;
          else { trends.videos = trends.videos.concat(items); }
        }
        result = trends;
        break;
      }

      case 'instagramMonitor': {
        if (!genre) return res.status(400).json({ error: 'Genre required' });
        const searches = await Promise.all([
          serperSearch(`instagram ${genre} music accounts to follow independent artists`, 10),
          serperSearch(`instagram ${genre} repost pages promotion music`, 10),
          serperSearch(`#${genre.replace(/\s+/g, '')} instagram top posts music`, 10),
          serperSearch(`instagram ${genre} music content strategy hooks captions 2026`, 10),
          serperSearch(`instagram reels ${genre} music trending audio format`, 10)
        ]);
        const monitor = { accounts: [], repostPages: [], hashtags: [], strategies: [], reelTrends: [] };
        const keys = ['accounts', 'repostPages', 'hashtags', 'strategies', 'reelTrends'];
        for (let i = 0; i < searches.length; i++) {
          const items = [];
          if (searches[i].organic) {
            for (const r of searches[i].organic) {
              items.push({ title: r.title, link: r.link, snippet: r.snippet || '' });
            }
          }
          monitor[keys[i]] = items;
        }
        result = monitor;
        break;
      }

      default:
        return res.status(400).json({ error: 'Unknown action: ' + action });
    }

    cacheSet(cacheKey, result);
    res.json(result);
  } catch (err) {
    console.error('Research API error:', err.message);
    res.status(500).json({ error: err.message });
  }
});

// API config check endpoint
app.get('/api/research/status', requireAccess, (req, res) => {
  res.json({
    search: !!SERPER_API_KEY
  });
});

// --- Admin Middleware ---
function requireAdmin(req, res, next) {
  if (!req.session.userId) return res.status(401).json({ error: 'Not logged in' });
  const user = dbHelpers.prepare('SELECT * FROM users WHERE id = ? AND deleted_at IS NULL').get(req.session.userId);
  if (!user || user.role !== 'admin') return res.status(403).json({ error: 'Admin access required' });
  req.user = user;
  next();
}

// --- Admin Impersonation ---
// Allows admin to log in as any non-admin user to see their view and make
// changes on their behalf. The original admin session is preserved so they
// can switch back. Every impersonation start/stop is audit-logged.

app.post('/api/admin/impersonate/:id', requireAdmin, (req, res) => {
  const targetId = parseInt(req.params.id, 10);
  if (!Number.isInteger(targetId) || targetId <= 0) return res.status(400).json({ error: 'Invalid id' });
  const target = dbHelpers.prepare('SELECT * FROM users WHERE id = ? AND deleted_at IS NULL').get(targetId);
  if (!target) return res.status(404).json({ error: 'User not found' });
  if (target.role === 'admin') return res.status(403).json({ error: 'Cannot impersonate other admins' });

  // Preserve the real admin identity so we can restore it later
  req.session.realAdminId = req.session.realAdminId || req.session.userId;
  req.session.userId = targetId;
  req.session.impersonating = true;

  logOperation(req, 'admin.impersonate_start', 'user', targetId, {
    admin_id: req.session.realAdminId,
    admin_email: req.user.email,
    target_email: target.email
  });
  res.json({ success: true, impersonating: target.email });
});

app.post('/api/admin/stop-impersonation', (req, res) => {
  if (!req.session.realAdminId) return res.status(400).json({ error: 'Not impersonating' });
  const adminId = req.session.realAdminId;
  const wasImpersonating = req.session.userId;

  req.session.userId = adminId;
  delete req.session.realAdminId;
  delete req.session.impersonating;

  logOperation(req, 'admin.impersonate_stop', 'user', wasImpersonating, { admin_id: adminId });
  res.json({ success: true, restored: true });
});

// Expose impersonation state to the client so it can show a banner
app.get('/api/admin/impersonation-status', (req, res) => {
  if (!req.session.userId) return res.status(401).json({ error: 'Not logged in' });
  if (!req.session.impersonating || !req.session.realAdminId) {
    return res.json({ impersonating: false });
  }
  const admin = dbHelpers.prepare('SELECT email FROM users WHERE id = ?').get(req.session.realAdminId);
  const target = dbHelpers.prepare('SELECT email FROM users WHERE id = ?').get(req.session.userId);
  res.json({
    impersonating: true,
    admin_email: admin ? admin.email : 'unknown',
    target_email: target ? target.email : 'unknown',
    target_id: req.session.userId
  });
});

// --- Admin API ---

// Analytics & user list
app.get('/api/admin/users', requireAdmin, (req, res) => {
  const users = dbHelpers.prepare(`
    SELECT id, email, role, subscription_status, subscription_tier, onboarding_completed, trial_ends_at, created_at
    FROM users WHERE deleted_at IS NULL ORDER BY created_at DESC
  `).all();
  const total = users.length;
  const active = users.filter(u => u.subscription_status === 'active').length;
  const trialing = users.filter(u => u.subscription_status === 'trialing').length;
  const admins = users.filter(u => u.role === 'admin').length;
  const expired = users.filter(u => {
    if (u.role === 'admin' || u.subscription_status === 'active') return false;
    if (u.subscription_status === 'trialing' && u.trial_ends_at) {
      return new Date(u.trial_ends_at) <= new Date();
    }
    return u.subscription_status === 'none' || u.subscription_status === 'canceled';
  }).length;
  res.json({ users, stats: { total, active, trialing, admins, expired } });
});

// Soft-delete user (admin only, can't delete admins). Preserves all data
// with deleted_at timestamps. A full JSON snapshot is archived in
// deleted_users_archive for recovery. Encrypted creds (elite_onboarding)
// are hard-deleted — retaining them post-account-deletion is a liability.
app.delete('/api/admin/users/:id', requireAdmin, (req, res) => {
  const id = parseInt(req.params.id, 10);
  if (!Number.isInteger(id) || id <= 0) return res.status(400).json({ error: 'Invalid id' });
  const user = dbHelpers.prepare('SELECT * FROM users WHERE id = ? AND deleted_at IS NULL').get(id);
  if (!user) return res.status(404).json({ error: 'User not found' });
  if (user.role === 'admin') return res.status(403).json({ error: 'Cannot delete admin accounts' });

  // --- Snapshot all user data before soft-deleting (recovery safety net) ---
  const snapshot = {
    user,
    user_data: dbHelpers.prepare('SELECT * FROM user_data WHERE user_id = ?').all(id),
    user_xp: dbHelpers.prepare('SELECT * FROM user_xp WHERE user_id = ?').all(id),
    user_achievements: dbHelpers.prepare('SELECT * FROM user_achievements WHERE user_id = ?').all(id),
    xp_log: dbHelpers.prepare('SELECT * FROM xp_log WHERE user_id = ?').all(id),
    support_tickets: dbHelpers.prepare('SELECT * FROM support_tickets WHERE user_id = ?').all(id),
    api_usage: dbHelpers.prepare('SELECT * FROM api_usage WHERE user_id = ?').all(id),
    submission_progress: dbHelpers.prepare('SELECT * FROM submission_progress WHERE user_id = ?').all(id),
    outreach_purchases: dbHelpers.prepare('SELECT * FROM outreach_purchases WHERE user_id = ?').all(id),
    redemption_requests: dbHelpers.prepare('SELECT * FROM redemption_requests WHERE user_id = ?').all(id),
  };
  dbHelpers.prepare(`
    INSERT INTO deleted_users_archive (user_id, user_email, snapshot_json, deleted_by)
    VALUES (?, ?, ?, ?)
  `).run(id, user.email, JSON.stringify(snapshot), req.session.userId);

  // --- Soft-delete: mark with timestamp instead of destroying ---
  const now = new Date().toISOString();
  dbHelpers.prepare("UPDATE users SET deleted_at = ?, updated_at = ? WHERE id = ?").run(now, now, id);
  dbHelpers.prepare("UPDATE user_data SET deleted_at = ? WHERE user_id = ?").run(now, id);
  dbHelpers.prepare("UPDATE user_xp SET deleted_at = ? WHERE user_id = ?").run(now, id);
  dbHelpers.prepare("UPDATE user_achievements SET deleted_at = ? WHERE user_id = ?").run(now, id);
  dbHelpers.prepare("UPDATE xp_log SET deleted_at = ? WHERE user_id = ?").run(now, id);
  dbHelpers.prepare("UPDATE support_tickets SET deleted_at = ? WHERE user_id = ?").run(now, id);
  dbHelpers.prepare("UPDATE api_usage SET deleted_at = ? WHERE user_id = ?").run(now, id);
  dbHelpers.prepare("UPDATE submission_progress SET deleted_at = ? WHERE user_id = ?").run(now, id);
  dbHelpers.prepare("UPDATE outreach_purchases SET deleted_at = ? WHERE user_id = ?").run(now, id);
  dbHelpers.prepare("UPDATE redemption_requests SET deleted_at = ? WHERE user_id = ?").run(now, id);

  // Hard-delete encrypted credentials only — retaining creds after account
  // deletion is a security liability, not a safety benefit.
  dbHelpers.prepare('DELETE FROM elite_onboarding WHERE user_id = ?').run(id);
  dbHelpers.prepare('DELETE FROM elite_onboarding_access_log WHERE user_id = ?').run(id);

  logOperation(req, 'user.soft_delete', 'user', id, { email: user.email, tier: user.subscription_tier });
  flushDbNow(); // durable admin-action write — C2
  res.json({ success: true, deleted: user.email, recoverable: true });
});

// Admin: update a user's subscription tier. Used for testing and manual
// tier assignments (e.g., comp'd Elite Plus accounts).
app.post('/api/admin/users/:id/tier', requireAdmin, (req, res) => {
  const id = parseInt(req.params.id, 10);
  if (!Number.isInteger(id) || id <= 0) return res.status(400).json({ error: 'Invalid id' });
  const tier = String((req.body || {}).tier || '').toLowerCase();
  if (!['pro', 'elite', 'elite_plus'].includes(tier)) {
    return res.status(400).json({ error: 'tier must be pro, elite, or elite_plus' });
  }
  const user = dbHelpers.prepare('SELECT * FROM users WHERE id = ? AND deleted_at IS NULL').get(id);
  if (!user) return res.status(404).json({ error: 'User not found' });
  const oldTier = user.subscription_tier;
  dbHelpers.prepare(`
    UPDATE users SET subscription_tier = ?, subscription_status = 'active', updated_at = datetime('now')
    WHERE id = ?
  `).run(tier, id);
  logOperation(req, 'admin.tier_change', 'user', id, { email: user.email, from: oldTier, to: tier });
  flushDbNow();
  res.json({ success: true, email: user.email, oldTier, newTier: tier });
});

// List all Elite/Elite Plus users with onboarding status, for the admin
// Elite Onboarding view. Doesn't decrypt anything — that's a separate
// audited route.
app.get('/api/admin/elite-onboarding', requireAdmin, (req, res) => {
  const rows = dbHelpers.prepare(`
    SELECT u.id, u.email, u.subscription_tier, u.subscription_status, u.onboarding_completed,
           u.created_at, eo.submitted_at, eo.updated_at
    FROM users u
    LEFT JOIN elite_onboarding eo ON eo.user_id = u.id
    WHERE u.subscription_tier IN ('elite', 'elite_plus') AND u.deleted_at IS NULL
    ORDER BY u.created_at DESC
  `).all();
  res.json({ users: rows });
});

// Decrypt an Elite user's stored credentials. Audit-logged on every access.
// Admin-only — never expose to the user themselves (they already have the
// credentials they entered; this route exists for Joseph to do manual work).
app.get('/api/admin/elite-onboarding/:userId', requireAdmin, (req, res) => {
  const userId = parseInt(req.params.userId, 10);
  if (!Number.isInteger(userId) || userId <= 0) return res.status(400).json({ error: 'Invalid id' });
  const row = dbHelpers.prepare('SELECT * FROM elite_onboarding WHERE user_id = ?').get(userId);
  if (!row) return res.status(404).json({ error: 'No onboarding record' });
  let plaintext;
  try { plaintext = decryptOnboarding(row); }
  catch (e) {
    console.error('[ELITE] decrypt error for user', userId, ':', e.message);
    return res.status(500).json({ error: 'Failed to decrypt' });
  }
  // Audit log — every plaintext view recorded with the admin who did it.
  try {
    dbHelpers.prepare(`
      INSERT INTO elite_onboarding_access_log (user_id, admin_id, admin_email)
      VALUES (?, ?, ?)
    `).run(userId, req.user.id, req.user.email);
  } catch (e) { console.error('[ELITE] access log write failed:', e.message); }
  logOperation(req, 'elite.credentials_viewed', 'user', userId, { admin: req.user.email });
  const userRow = dbHelpers.prepare('SELECT email, subscription_tier FROM users WHERE id = ?').get(userId);
  res.json({
    user_id: userId,
    user_email: userRow ? userRow.email : null,
    tier: row.tier,
    submitted_at: row.submitted_at,
    updated_at: row.updated_at,
    data: plaintext
  });
});

// Support tickets — list all
app.get('/api/admin/tickets', requireAdmin, (req, res) => {
  const tickets = dbHelpers.prepare(`
    SELECT * FROM support_tickets WHERE deleted_at IS NULL ORDER BY
      CASE WHEN status = 'open' AND escalated = 1 THEN 0
           WHEN status = 'open' THEN 1
           ELSE 2 END,
      created_at DESC
  `).all();
  res.json({ tickets });
});

// Support tickets — update (admin notes, status, etc.)
app.post('/api/admin/tickets/:id', requireAdmin, (req, res) => {
  const id = parseInt(req.params.id, 10);
  if (!Number.isInteger(id) || id <= 0) return res.status(400).json({ error: 'Invalid id' });
  const { status, admin_notes } = req.body || {};
  const ticket = dbHelpers.prepare('SELECT * FROM support_tickets WHERE id = ?').get(id);
  if (!ticket) return res.status(404).json({ error: 'Ticket not found' });
  if (status && ['open','resolved','closed'].includes(status)) {
    dbHelpers.prepare('UPDATE support_tickets SET status = ?, updated_at = datetime("now") WHERE id = ?').run(status, id);
  }
  if (admin_notes !== undefined) {
    dbHelpers.prepare('UPDATE support_tickets SET admin_notes = ?, updated_at = datetime("now") WHERE id = ?').run(String(admin_notes).slice(0, 5000), id);
  }
  logOperation(req, 'admin.ticket_update', 'support_ticket', id, { status: status || null, has_notes: admin_notes !== undefined });
  flushDbNow(); // durable admin-action write — C2
  res.json({ success: true });
});

// Redemption requests — admin list. Paid orders sort first so Joseph sees
// what needs work, then pending (likely abandoned checkouts), then completed.
app.get('/api/admin/redemptions', requireAdmin, (req, res) => {
  const rows = dbHelpers.prepare(`
    SELECT r.*, u.email AS user_email
    FROM redemption_requests r
    LEFT JOIN users u ON u.id = r.user_id
    WHERE r.deleted_at IS NULL
    ORDER BY
      CASE r.status
        WHEN 'paid' THEN 0
        WHEN 'pending' THEN 1
        WHEN 'completed' THEN 2
        ELSE 3
      END,
      r.created_at DESC
  `).all();
  res.json({ redemptions: rows });
});

// Redemption requests — admin update status / notes. Status transitions are
// whitelisted; canceled is allowed so Joseph can clear out abandoned checkouts.
app.post('/api/admin/redemptions/:id', requireAdmin, (req, res) => {
  const id = parseInt(req.params.id, 10);
  if (!Number.isInteger(id) || id <= 0) return res.status(400).json({ error: 'Invalid id' });
  const row = dbHelpers.prepare('SELECT * FROM redemption_requests WHERE id = ?').get(id);
  if (!row) return res.status(404).json({ error: 'Redemption not found' });
  const { status, admin_notes } = req.body || {};
  if (status && ['pending','paid','completed','canceled'].includes(status)) {
    if (status === 'completed') {
      dbHelpers.prepare("UPDATE redemption_requests SET status = ?, completed_at = datetime('now'), updated_at = datetime('now') WHERE id = ?").run(status, id);
    } else {
      dbHelpers.prepare("UPDATE redemption_requests SET status = ?, updated_at = datetime('now') WHERE id = ?").run(status, id);
    }
  }
  if (admin_notes !== undefined) {
    dbHelpers.prepare('UPDATE redemption_requests SET admin_notes = ? WHERE id = ?')
      .run(String(admin_notes).slice(0, 5000), id);
  }
  logOperation(req, 'admin.redemption_update', 'redemption_request', id, { status: status || null, prev_status: row.status });
  flushDbNow();
  res.json({ success: true });
});

// --- Operation Journal (admin read-only view) ---
// Paginated append-only audit trail. Never exposes a delete/truncate endpoint.
app.get('/api/admin/journal', requireAdmin, (req, res) => {
  const limit = Math.min(parseInt(req.query.limit, 10) || 100, 500);
  const offset = Math.max(parseInt(req.query.offset, 10) || 0, 0);
  const rows = dbHelpers.prepare(`
    SELECT * FROM operation_journal ORDER BY id DESC LIMIT ? OFFSET ?
  `).all(limit, offset);
  const total = dbHelpers.prepare('SELECT COUNT(*) AS cnt FROM operation_journal').get();
  res.json({ entries: rows, total: total ? total.cnt : 0, limit, offset });
});

// --- Admin: Referral Code & Commission Management ---

// List all referral codes with earnings summary
app.get('/api/admin/referrals', requireAdmin, (req, res) => {
  const codes = dbHelpers.prepare(`
    SELECT rc.*, u.email AS referrer_email,
      (SELECT COUNT(*) FROM referral_commissions WHERE referral_code = rc.code) AS total_referrals,
      (SELECT COALESCE(SUM(commission_cents), 0) FROM referral_commissions WHERE referral_code = rc.code) AS total_earned_cents,
      (SELECT COALESCE(SUM(commission_cents), 0) FROM referral_commissions WHERE referral_code = rc.code AND status = 'pending') AS pending_cents,
      (SELECT COALESCE(SUM(commission_cents), 0) FROM referral_commissions WHERE referral_code = rc.code AND status = 'paid') AS paid_cents
    FROM referral_codes rc
    JOIN users u ON u.id = rc.user_id
    ORDER BY rc.created_at DESC
  `).all();
  res.json({ codes });
});

// Create a referral code for a user
app.post('/api/admin/referrals', requireAdmin, (req, res) => {
  const { user_id, code, commission_rate } = req.body || {};
  const userId = parseInt(user_id, 10);
  if (!Number.isInteger(userId) || userId <= 0) return res.status(400).json({ error: 'Valid user_id required' });

  const user = dbHelpers.prepare('SELECT id, email FROM users WHERE id = ? AND deleted_at IS NULL').get(userId);
  if (!user) return res.status(404).json({ error: 'User not found' });

  // Generate or validate code
  let refCode = code ? String(code).trim().toUpperCase().replace(/[^A-Z0-9]/g, '').slice(0, 20) : null;
  if (!refCode) {
    // Auto-generate: first 4 chars of email username + 4 random chars
    const prefix = user.email.split('@')[0].replace(/[^a-zA-Z0-9]/g, '').slice(0, 4).toUpperCase();
    refCode = prefix + crypto.randomBytes(2).toString('hex').toUpperCase();
  }

  const rate = parseFloat(commission_rate) || 0.10;
  if (rate <= 0 || rate > 1) return res.status(400).json({ error: 'commission_rate must be between 0 and 1' });

  const existing = dbHelpers.prepare('SELECT id FROM referral_codes WHERE code = ?').get(refCode);
  if (existing) return res.status(409).json({ error: 'Code already exists' });

  dbHelpers.prepare('INSERT INTO referral_codes (user_id, code, commission_rate) VALUES (?, ?, ?)').run(userId, refCode, rate);
  logOperation(req, 'referral.code_created', 'user', userId, { code: refCode, rate });
  flushDbNow();
  res.json({ success: true, code: refCode, user_email: user.email, commission_rate: rate });
});

// Deactivate a referral code
app.post('/api/admin/referrals/:code/deactivate', requireAdmin, (req, res) => {
  const code = String(req.params.code).toUpperCase();
  const row = dbHelpers.prepare('SELECT * FROM referral_codes WHERE code = ?').get(code);
  if (!row) return res.status(404).json({ error: 'Code not found' });
  dbHelpers.prepare('UPDATE referral_codes SET active = 0 WHERE code = ?').run(code);
  logOperation(req, 'referral.code_deactivated', 'user', row.user_id, { code });
  flushDbNow();
  res.json({ success: true, code });
});

// List all commissions (filterable by referrer or status)
app.get('/api/admin/commissions', requireAdmin, (req, res) => {
  const { referrer_id, status } = req.query;
  let sql = `
    SELECT rc.*,
      ru.email AS referrer_email,
      su.email AS subscriber_email
    FROM referral_commissions rc
    JOIN users ru ON ru.id = rc.referrer_id
    JOIN users su ON su.id = rc.referred_user_id
    WHERE 1=1
  `;
  const params = [];
  if (referrer_id) { sql += ' AND rc.referrer_id = ?'; params.push(parseInt(referrer_id, 10)); }
  if (status) { sql += ' AND rc.status = ?'; params.push(status); }
  sql += ' ORDER BY rc.created_at DESC';
  const rows = dbHelpers.prepare(sql).all(...params);
  const totals = dbHelpers.prepare(`
    SELECT
      COUNT(*) AS total_commissions,
      COALESCE(SUM(commission_cents), 0) AS total_cents,
      COALESCE(SUM(CASE WHEN status = 'pending' THEN commission_cents ELSE 0 END), 0) AS pending_cents,
      COALESCE(SUM(CASE WHEN status = 'paid' THEN commission_cents ELSE 0 END), 0) AS paid_cents
    FROM referral_commissions
  `).get();
  res.json({ commissions: rows, totals });
});

// Mark commissions as paid
app.post('/api/admin/commissions/:id/pay', requireAdmin, async (req, res) => {
  const id = parseInt(req.params.id, 10);
  if (!Number.isInteger(id) || id <= 0) return res.status(400).json({ error: 'Invalid id' });
  const row = dbHelpers.prepare('SELECT * FROM referral_commissions WHERE id = ?').get(id);
  if (!row) return res.status(404).json({ error: 'Commission not found' });
  if (row.status === 'paid') return res.status(400).json({ error: 'Already paid' });

  // Attempt Stripe Transfer if referrer has a connected account
  const refCode = dbHelpers.prepare('SELECT * FROM referral_codes WHERE user_id = ? AND active = 1').get(row.referrer_id);
  let transferId = null;
  if (stripe && refCode && refCode.stripe_connect_id && refCode.stripe_onboarding_complete) {
    try {
      const transfer = await stripe.transfers.create({
        amount: row.commission_cents,
        currency: 'usd',
        destination: refCode.stripe_connect_id,
        description: `Referral commission: ${row.referral_code} — ${row.subscription_tier} subscription`,
        metadata: { commission_id: String(id), referral_code: row.referral_code }
      });
      transferId = transfer.id;
    } catch (err) {
      console.error('[CONNECT] transfer failed:', err.message);
      return res.status(500).json({ error: 'Stripe transfer failed: ' + err.message });
    }
  }

  dbHelpers.prepare("UPDATE referral_commissions SET status = 'paid', paid_at = datetime('now'), stripe_transfer_id = ? WHERE id = ?").run(transferId, id);
  logOperation(req, 'referral.commission_paid', 'user', row.referrer_id, { commission_id: id, cents: row.commission_cents, transfer_id: transferId });
  flushDbNow();
  res.json({ success: true, id, amount: '$' + (row.commission_cents / 100).toFixed(2), stripe_transfer: !!transferId });
});

// --- Outreach List admin importer ---
// Admin-only CSV ingest for the curated contact list. Two-step flow:
//   1. POST /api/admin/outreach/import — dry run. Parses the CSV,
//      normalizes category labels, diffs against the current published
//      version, and returns a preview. Nothing is written.
//   2. POST /api/admin/outreach/publish — commits the parsed rows as a
//      new version, bumps outreach_list_version.current_version, and
//      writes the change_summary JSON.
// Admin pastes the CSV contents into a <textarea> in Master Admin; we
// avoid a multer dependency for what is a once-per-release-cycle action.

// Human-label → canonical-slug map. The source CSV uses pretty labels
// (e.g. "SiriusXM Radio") because it's hand-curated; server storage uses
// canonical slugs (see OUTREACH_CATEGORIES).
const OUTREACH_CATEGORY_LABEL_MAP = {
  'siriusxm radio': 'sirius',
  'sirius xm radio': 'sirius',
  'sirius': 'sirius',
  'iheart radio': 'iheart',
  'iheartradio': 'iheart',
  'iheart': 'iheart',
  'fm/am radio': 'fm_am',
  'fm-am radio': 'fm_am',
  'fm / am radio': 'fm_am',
  'fm am radio': 'fm_am',
  'fm am': 'fm_am',
  'online radio': 'online_radio',
  'internet radio': 'online_radio',
  'press': 'press',
  'media': 'press',
  'press release': 'press_release',
  'press releases': 'press_release',
  'press_release': 'press_release',
  'press-release': 'press_release',
  'pr': 'press_release',
  'pr wire': 'press_release',
  'newswire': 'press_release',
  'blog': 'blog',
  'blogs': 'blog',
  'spotify playlist': 'spotify_playlist',
  'spotify playlists': 'spotify_playlist',
  'playlist': 'spotify_playlist',
  'podcast': 'podcast',
  'podcasts': 'podcast'
};
function normalizeCategoryLabel(raw) {
  if (!raw) return '';
  const k = String(raw).trim().toLowerCase();
  return OUTREACH_CATEGORY_LABEL_MAP[k] || '';
}

// Minimal RFC-4180-ish CSV parser. Handles quoted fields, embedded commas,
// escaped quotes ("") and CRLF line endings. Good enough for the curated
// outreach CSV; not a general-purpose replacement for `csv-parse`.
function parseCsv(text) {
  const rows = [];
  let field = '';
  let row = [];
  let i = 0;
  let inQuotes = false;
  const pushField = () => { row.push(field); field = ''; };
  const pushRow = () => { rows.push(row); row = []; };
  while (i < text.length) {
    const c = text[i];
    if (inQuotes) {
      if (c === '"') {
        if (text[i + 1] === '"') { field += '"'; i += 2; continue; }
        inQuotes = false; i++; continue;
      }
      field += c; i++; continue;
    }
    if (c === '"') { inQuotes = true; i++; continue; }
    if (c === ',') { pushField(); i++; continue; }
    if (c === '\r') { i++; continue; }
    if (c === '\n') { pushField(); pushRow(); i++; continue; }
    field += c; i++;
  }
  // Tail flush — last field/row without trailing newline.
  if (field.length || row.length) { pushField(); pushRow(); }
  // Drop entirely-empty trailing rows.
  while (rows.length && rows[rows.length - 1].every(f => !f || !f.trim())) rows.pop();
  return rows;
}

// Parse the raw CSV text into a list of contact records using the known
// column headers. Returns {rows, errors} where errors is per-row context
// so the admin can fix source data rather than silently dropping rows.
function parseOutreachCsv(csvText) {
  const out = { rows: [], errors: [] };
  const grid = parseCsv(String(csvText || ''));
  if (!grid.length) { out.errors.push('CSV is empty'); return out; }
  const header = grid[0].map(h => String(h || '').trim().toLowerCase());
  const colIdx = {
    name: header.indexOf('name'),
    category: header.indexOf('category'),
    submission_type: header.indexOf('submission type'),
    submission_value: header.indexOf('submission value'),
    website: header.indexOf('website'),
    phone: header.indexOf('phone'),
    notes: header.indexOf('notes')
  };
  if (colIdx.name < 0 || colIdx.category < 0) {
    out.errors.push('CSV must include Name and Category columns');
    return out;
  }
  for (let r = 1; r < grid.length; r++) {
    const g = grid[r];
    const get = (k) => (colIdx[k] >= 0 && g[colIdx[k]] != null) ? String(g[colIdx[k]]).trim() : '';
    const name = get('name');
    const rawCat = get('category');
    if (!name && !rawCat) continue; // skip blank line
    const cat = normalizeCategoryLabel(rawCat);
    if (!name) { out.errors.push(`Row ${r + 1}: missing Name`); continue; }
    if (!cat) { out.errors.push(`Row ${r + 1}: unknown Category "${rawCat}"`); continue; }
    out.rows.push({
      name: name.slice(0, 200),
      category: cat,
      submission_type: get('submission_type').slice(0, 40).toLowerCase() || 'unknown',
      submission_value: get('submission_value').slice(0, 500),
      website: get('website').slice(0, 500),
      phone: get('phone').slice(0, 80),
      notes: get('notes').slice(0, 1000)
    });
  }
  return out;
}

// Build a diff between the currently-published contact set (by category)
// and an incoming parsed set. "Added" = name not in current set for that
// category; "removed" = name in current set but not in incoming. Case-
// insensitive name comparison so minor capitalization drift doesn't churn.
function diffOutreachContacts(incomingRows) {
  const currentVersion = dbHelpers.prepare("SELECT current_version FROM outreach_list_version WHERE singleton_key = 'current'").get();
  const version = currentVersion ? currentVersion.current_version : 0;
  const current = version > 0
    ? dbHelpers.prepare('SELECT category, name FROM outreach_contacts WHERE version = ?').all(version)
    : [];
  const byCatCurrent = {};
  for (const cat of OUTREACH_CATEGORIES) byCatCurrent[cat] = new Set();
  for (const r of current) {
    if (byCatCurrent[r.category]) byCatCurrent[r.category].add(String(r.name).toLowerCase());
  }
  const byCatIncoming = {};
  for (const cat of OUTREACH_CATEGORIES) byCatIncoming[cat] = new Set();
  for (const r of incomingRows) {
    byCatIncoming[r.category].add(String(r.name).toLowerCase());
  }
  const by_category = {};
  let added_total = 0;
  let removed_total = 0;
  for (const cat of OUTREACH_CATEGORIES) {
    const added = [...byCatIncoming[cat]].filter(n => !byCatCurrent[cat].has(n));
    const removed = [...byCatCurrent[cat]].filter(n => !byCatIncoming[cat].has(n));
    by_category[cat] = {
      incoming: byCatIncoming[cat].size,
      current: byCatCurrent[cat].size,
      added: added.length,
      removed: removed.length
    };
    added_total += added.length;
    removed_total += removed.length;
  }
  return { current_version: version, added_total, removed_total, by_category };
}

app.get('/api/admin/outreach/status', requireAdmin, (req, res) => {
  const row = dbHelpers.prepare("SELECT current_version, change_summary, updated_at FROM outreach_list_version WHERE singleton_key = 'current'").get();
  const version = row ? row.current_version : 0;
  const counts = dbHelpers.prepare('SELECT category, COUNT(*) as n FROM outreach_contacts WHERE version = ? GROUP BY category').all(version);
  const byCategory = {};
  for (const cat of OUTREACH_CATEGORIES) byCategory[cat] = 0;
  for (const c of counts) { if (byCategory.hasOwnProperty(c.category)) byCategory[c.category] = c.n; }
  let changeSummary = null;
  try { changeSummary = row && row.change_summary ? JSON.parse(row.change_summary) : null; } catch(_) {}
  res.json({
    current_version: version,
    updated_at: row ? row.updated_at : null,
    total_contacts: counts.reduce((s, c) => s + c.n, 0),
    by_category: byCategory,
    last_change_summary: changeSummary
  });
});

app.post('/api/admin/outreach/import', requireAdmin, (req, res) => {
  const csvText = String((req.body && req.body.csv) || '');
  if (!csvText.trim()) return res.status(400).json({ error: 'CSV body is empty' });
  if (csvText.length > 2 * 1024 * 1024) return res.status(413).json({ error: 'CSV too large (max 2MB)' });
  const parsed = parseOutreachCsv(csvText);
  const diff = diffOutreachContacts(parsed.rows);
  res.json({
    dry_run: true,
    parsed_count: parsed.rows.length,
    parse_errors: parsed.errors,
    diff
  });
});

app.post('/api/admin/outreach/publish', requireAdmin, (req, res) => {
  const csvText = String((req.body && req.body.csv) || '');
  if (!csvText.trim()) return res.status(400).json({ error: 'CSV body is empty' });
  if (csvText.length > 2 * 1024 * 1024) return res.status(413).json({ error: 'CSV too large (max 2MB)' });
  const parsed = parseOutreachCsv(csvText);
  if (!parsed.rows.length) {
    return res.status(400).json({ error: 'No valid rows to publish', parse_errors: parsed.errors });
  }

  const diff = diffOutreachContacts(parsed.rows);
  const newVersion = diff.current_version + 1;

  // Transactional publish: insert all rows with the new version number,
  // then bump the version pointer + change summary. sql.js doesn't support
  // BEGIN/COMMIT through prepare(), so we run raw SQL transaction statements.
  try {
    db.run('BEGIN');
    const stmt = dbHelpers.prepare(`
      INSERT INTO outreach_contacts
        (version, category, name, submission_type, submission_value, website, phone, notes)
      VALUES (?, ?, ?, ?, ?, ?, ?, ?)
    `);
    for (const r of parsed.rows) {
      stmt.run(newVersion, r.category, r.name, r.submission_type, r.submission_value, r.website, r.phone, r.notes);
    }
    const summary = { version: newVersion, ...diff };
    dbHelpers.prepare(`
      UPDATE outreach_list_version
      SET current_version = ?, change_summary = ?, updated_at = datetime('now')
      WHERE singleton_key = 'current'
    `).run(newVersion, JSON.stringify(summary));
    db.run('COMMIT');
  } catch (e) {
    try { db.run('ROLLBACK'); } catch(_) {}
    console.error('[OUTREACH] publish error:', e.message);
    return res.status(500).json({ error: 'Publish failed: ' + e.message });
  }
  logOperation(req, 'admin.outreach_publish', 'outreach_list', newVersion, { inserted: parsed.rows.length, errors: parsed.errors.length });
  flushDbNow();

  res.json({
    success: true,
    published_version: newVersion,
    inserted: parsed.rows.length,
    parse_errors: parsed.errors,
    diff
  });
});

// --- Outreach List: catalog, status, and AI intro endpoints ---
// Seed templates for the 8 category intro buttons in the Email Generator.
// These are prompt starters for Claude, NOT the final output. Every click
// produces a fresh AI-tweaked intro using release metadata (see R9 in
// task/lessons.md). The fallback token-fill path uses these strings
// verbatim — only triggered when Claude is unreachable.
const OUTREACH_SEED_INTROS = {
  sirius: `Hi {curator_name}, I wanted to reach out about my new release "{song_title}". It's a {genre} track dropping {release_date} and I think it would resonate with the Sirius XM audience — specifically listeners looking for authentic {mood} records.`,
  iheart: `Hi {curator_name}, I'm the artist behind "{song_title}", a new {genre} single out {release_date}. I'd love to see if it's a fit for iHeart rotation — the response so far has been {traction_angle}.`,
  fm_am: `Hi {curator_name}, reaching out from the {artist_name} camp about a new single, "{song_title}". It's {genre} with crossover appeal and we'd love to get it in front of the {station_context} audience.`,
  online_radio: `Hey {curator_name}, I'm sending over my latest single "{song_title}" ({genre}, out {release_date}). I follow what you're doing on the show and think this track lines up with the sound you've been playing.`,
  press: `Hi {curator_name}, quick pitch: "{song_title}" is a new {genre} release from {artist_name} dropping {release_date}. The story behind it is {story_hook} — happy to send more context if you'd like to write it up.`,
  press_release: `FOR IMMEDIATE RELEASE — {artist_name} announces the release of "{song_title}", a new {genre} single arriving {release_date}. The record leans into {mood} and {story_hook}. Full press kit, artwork, and streaming links available on request. Contact: {artist_name} management.`,
  blog: `Hey {curator_name}, longtime reader. I wanted to put my new single "{song_title}" on your radar — {genre}, out {release_date}. {story_hook}`,
  spotify_playlist: `Hi {curator_name}, I'd love to pitch my new single "{song_title}" for {playlist_name}. It's {genre} with a {mood} feel, releases {release_date}, and I think it sits well next to the artists you've been curating.`,
  podcast: `Hey {curator_name}, big fan of the show. I'd love to come on and talk about my new single "{song_title}" and the story behind it — {story_hook}. Available {release_window}.`
};

// Hydrate OUTREACH_STATE for a given user — what the frontend needs to
// render locks, banners, and contact lists. Returns unlocked+version info
// for users without a purchase so the UI can show the upsell card.
function buildOutreachState(user) {
  const unlocked = user.role === 'admin'
    ? { id: 0, banner_dismissed_version: 0 }
    : dbHelpers.prepare('SELECT id, banner_dismissed_version FROM outreach_purchases WHERE user_id = ?').get(user.id);
  const verRow = dbHelpers.prepare("SELECT current_version, change_summary FROM outreach_list_version WHERE singleton_key = 'current'").get();
  const version = verRow ? verRow.current_version : 0;
  let changeSummary = null;
  try { changeSummary = verRow && verRow.change_summary ? JSON.parse(verRow.change_summary) : null; } catch(_) {}
  return {
    unlocked: !!unlocked,
    current_version: version,
    banner_dismissed_version: unlocked ? (unlocked.banner_dismissed_version || 0) : 0,
    last_change_summary: changeSummary,
    categories: OUTREACH_CATEGORIES,
    category_labels: OUTREACH_CATEGORY_LABELS
  };
}

// Public status — anyone logged in can call this. Used by the frontend
// to decide whether to show the locked card or the full tracker UI.
app.get('/api/outreach/status', requireAuth, (req, res) => {
  res.json(buildOutreachState(req.user));
});

// Locked to unlocked users only. Returns the full contact list for the
// current published version, grouped by category. Also returns the user's
// per-contact submission progress so the UI can render status icons.
app.get('/api/outreach/contacts', requireOutreachUnlocked, (req, res) => {
  const verRow = dbHelpers.prepare("SELECT current_version FROM outreach_list_version WHERE singleton_key = 'current'").get();
  const version = verRow ? verRow.current_version : 0;
  // Phone-only contacts are excluded from the tracker — cold-calling is not
  // a supported outreach motion and the client will not render them. Email
  // contacts are ALSO excluded from the tracker (see R13): the tracker only
  // hosts click-through outreach (forms, websites, playlist forms, socials)
  // because email reach-outs are handled via the per-category Google Contacts
  // CSV export (/api/outreach/export/:category.csv), so opening each email
  // contact one-by-one in the overlay would be redundant. Defense in depth:
  // filtering server-side ensures these rows never leave the DB to the
  // browser, even if a future client-side bug forgets to drop them.
  const contacts = version > 0
    ? dbHelpers.prepare("SELECT id, category, name, submission_type, submission_value, website, phone, notes FROM outreach_contacts WHERE version = ? AND submission_type != 'phone' AND submission_type != 'email' ORDER BY category, name").all(version)
    : [];
  const progress = dbHelpers.prepare('SELECT contact_id, status, release_id, submitted_at FROM submission_progress WHERE user_id = ?').all(req.user.id);
  res.json({ version, contacts, progress });
});

// Dismiss the "List Updated" banner for the current version. Stores the
// dismissed version number so a future publish will re-surface it.
app.post('/api/outreach/dismiss-banner', requireOutreachUnlocked, (req, res) => {
  if (req.user.role === 'admin') return res.json({ success: true });
  const verRow = dbHelpers.prepare("SELECT current_version FROM outreach_list_version WHERE singleton_key = 'current'").get();
  const version = verRow ? verRow.current_version : 0;
  dbHelpers.prepare('UPDATE outreach_purchases SET banner_dismissed_version = ? WHERE user_id = ?')
    .run(version, req.user.id);
  flushDbNow();
  res.json({ success: true, dismissed_version: version });
});

// Google Contacts CSV export for a single category. Format matches the
// Google Contacts import spec: Name, E-mail 1 - Value, Group Membership.
app.get('/api/outreach/export/:category.csv', requireOutreachUnlocked, (req, res) => {
  const cat = String(req.params.category || '').toLowerCase();
  if (!OUTREACH_CATEGORIES.includes(cat)) return res.status(400).send('Unknown category');
  const verRow = dbHelpers.prepare("SELECT current_version FROM outreach_list_version WHERE singleton_key = 'current'").get();
  const version = verRow ? verRow.current_version : 0;
  const rows = version > 0
    ? dbHelpers.prepare('SELECT name, submission_type, submission_value FROM outreach_contacts WHERE version = ? AND category = ? ORDER BY name').all(version, cat)
    : [];
  const groupLabel = OUTREACH_CATEGORY_LABELS[cat] || cat;
  const csvEscape = (s) => {
    const str = String(s || '');
    if (/[",\n]/.test(str)) return '"' + str.replace(/"/g, '""') + '"';
    return str;
  };
  const lines = ['Name,E-mail 1 - Value,Group Membership'];
  for (const r of rows) {
    // Google Contacts only makes sense for email contacts — skip form,
    // social, phone, and anything without a valid-looking email address
    // so the exported CSV never contains blank-email rows that would
    // pollute the user's Google Contacts with unaddressable entries.
    if (r.submission_type !== 'email') continue;
    const email = String(r.submission_value || '').trim();
    if (!email || email.indexOf('@') < 1) continue;
    lines.push([csvEscape(r.name), csvEscape(email), csvEscape('Rollout Heaven :: ' + groupLabel)].join(','));
  }
  res.setHeader('Content-Type', 'text/csv; charset=utf-8');
  res.setHeader('Content-Disposition', `attachment; filename="outreach_${cat}_v${version}.csv"`);
  res.send(lines.join('\n'));
});

// AI-tweaked intro generator — THE R9 ENDPOINT. Gated by
// requireOutreachUnlocked ONLY (NOT requireActive), because the $250
// purchase IS the access grant for trial users. Calls Claude directly
// with the seed template + release metadata + category context, returns
// a personalized paragraph. Token cost is bounded by rlOutreachIntro
// (40/hr/user) + a small max_tokens cap here.
app.post('/api/outreach/generate-intro', requireOutreachUnlocked, rlOutreachIntro, async (req, res) => {
  const CLAUDE_KEY = process.env.CLAUDE_API_KEY || '';
  if (!CLAUDE_KEY) return res.status(503).json({ error: 'AI features not configured', fallback: true });

  const { category, releaseData } = req.body || {};
  const cat = String(category || '').toLowerCase();
  if (!OUTREACH_CATEGORIES.includes(cat)) {
    return res.status(400).json({ error: 'Unknown category' });
  }
  const seed = OUTREACH_SEED_INTROS[cat] || '';
  const rd = releaseData && typeof releaseData === 'object' ? releaseData : {};

  // Cap all incoming metadata fields so a malicious/buggy client can't
  // pump megabytes of context into the Claude prompt.
  const cap = (v, n) => (v == null ? '' : String(v)).slice(0, n).trim();
  const meta = {
    song_title: cap(rd.songTitle || rd.song_title, 200),
    artist_name: cap(rd.artistName || rd.artist_name || rd.artists, 200),
    genre: cap(rd.genre, 100),
    release_date: cap(rd.releaseDate || rd.release_date, 50),
    bio: cap(rd.bio || rd.artistBio, 1500),
    intro_text: cap(rd.introText || rd.intro, 1000),
    materials: cap(rd.materials || rd.availableMaterials, 500),
    mood: cap(rd.mood, 100),
    label: cap(rd.label, 100)
  };
  if (!meta.song_title) {
    return res.status(400).json({ error: 'Release must have a song title' });
  }

  const system = 'You are a music marketing assistant writing personalized outreach intros for indie musicians. Return ONLY the intro paragraph — no greeting preamble, no signoff, no commentary, no quotation marks. 2-4 sentences max. Natural, specific, conversational. Never invent facts not present in the release metadata.';
  const userPrompt = [
    `Category: ${OUTREACH_CATEGORY_LABELS[cat]} (${cat})`,
    ``,
    `Seed template (use it as a reference for tone/length, but rewrite it so every recipient gets a unique message, and do NOT keep literal {placeholder} tokens — either substitute them with real metadata or drop them):`,
    seed,
    ``,
    `Release metadata:`,
    `- Song title: ${meta.song_title}`,
    meta.artist_name ? `- Artist: ${meta.artist_name}` : '',
    meta.genre ? `- Genre: ${meta.genre}` : '',
    meta.release_date ? `- Release date: ${meta.release_date}` : '',
    meta.mood ? `- Mood / vibe: ${meta.mood}` : '',
    meta.label ? `- Label: ${meta.label}` : '',
    meta.materials ? `- Available materials: ${meta.materials}` : '',
    meta.bio ? `- Artist bio: ${meta.bio}` : '',
    meta.intro_text ? `- Artist-written intro (reference, not to copy verbatim): ${meta.intro_text}` : '',
    ``,
    `Write the intro paragraph now.`
  ].filter(Boolean).join('\n');

  try {
    const aiResp = await fetch('https://api.anthropic.com/v1/messages', {
      method: 'POST',
      headers: {
        'x-api-key': CLAUDE_KEY,
        'anthropic-version': '2023-06-01',
        'content-type': 'application/json'
      },
      body: JSON.stringify({
        model: DEFAULT_CLAUDE_MODEL,
        max_tokens: 400,
        system,
        messages: [{ role: 'user', content: userPrompt }]
      }),
      signal: AbortSignal.timeout(60000)
    });
    if (!aiResp.ok) {
      const errText = await aiResp.text();
      console.error('[OUTREACH INTRO] Anthropic error:', aiResp.status, errText.slice(0, 500));
      return res.status(502).json({ error: 'AI service unavailable', fallback: true });
    }
    const data = await aiResp.json();
    const text = (data.content && data.content[0] && data.content[0].text) || '';
    // Record usage for admin accounting. Admins skip the cap.
    try {
      if (req.user.role !== 'admin') {
        recordClaudeUsage(req.user.id, data.usage?.input_tokens || 0, data.usage?.output_tokens || 0);
      }
    } catch(_) {}
    if (!text.trim()) {
      return res.status(502).json({ error: 'Empty AI response', fallback: true });
    }
    // Notify admin when Elite/Elite Plus users generate outreach intros.
    if (isEliteTier(req.user.subscription_tier)) {
      const tierLabel = req.user.subscription_tier === 'elite_plus' ? 'Elite Plus' : 'Elite';
      notifyAdmins(
        `${tierLabel} Outreach Intro Generated — ${req.user.email}`,
        `<h2 style="color:#fff; margin-top:0;">Outreach Intro Generated</h2>
         <p><strong>User:</strong> ${req.user.email} (${tierLabel})</p>
         <p><strong>Category:</strong> ${cat}</p>
         <p><strong>Song:</strong> ${meta.song_title}</p>
         ${meta.artist_name ? `<p><strong>Artist:</strong> ${meta.artist_name}</p>` : ''}`
      ).catch(() => {});
    }

    res.json({ intro: text.trim(), category: cat, model: DEFAULT_CLAUDE_MODEL });
  } catch (err) {
    console.error('[OUTREACH INTRO] fetch error:', err.message);
    res.status(502).json({ error: 'AI request failed', fallback: true });
  }
});

// Submission Tracker — personalized social DM generator. Calls Claude with
// one contact's (name, platform, handle) + the current release metadata and
// returns a short, contact-specific DM body. Client caches per contact+song
// so re-opening the overlay doesn't re-burn tokens. Rate limit = 120/hr/user.
app.post('/api/outreach/social-dm', requireOutreachUnlocked, rlSocialDm, async (req, res) => {
  const CLAUDE_KEY = process.env.CLAUDE_API_KEY || '';
  if (!CLAUDE_KEY) return res.status(503).json({ error: 'AI features not configured', fallback: true });

  const cap = (v, n) => (v == null ? '' : String(v)).slice(0, n).trim();
  const contactIn = req.body && req.body.contact && typeof req.body.contact === 'object' ? req.body.contact : {};
  const rd = req.body && req.body.releaseData && typeof req.body.releaseData === 'object' ? req.body.releaseData : {};

  const contact = {
    name: cap(contactIn.name, 200),
    platform: cap(contactIn.platform, 50) || 'Instagram',
    handle: cap(contactIn.handle, 100),
    category: cap(contactIn.category, 100),
    notes: cap(contactIn.notes, 500),
  };
  const meta = {
    song_title: cap(rd.songTitle || rd.song_title, 200),
    artist_name: cap(rd.primaryArtist || rd.stageName || rd.artistName, 200),
    feat: cap(rd.featArtist, 200),
    genre: cap([rd.genrePrimary, rd.genreSecondary].filter(Boolean).join(' / ') || rd.genre, 100),
    release_date: cap(rd.releaseDate || rd.release_date, 50),
    mood: cap(rd.mood, 100),
    bio: cap(rd.bio || rd.artistBio, 1500),
    link: cap(rd.linkSpotify || rd.linkApple || rd.linkYTMusic || rd.linkYTVideo, 500),
  };
  if (!meta.song_title || !contact.name) {
    return res.status(400).json({ error: 'Contact name and song title required' });
  }

  const system = 'You write short, warm, human direct messages for indie Christian hip hop artists reaching out on social media. Return ONLY the DM body. No subject line, no preamble, no commentary, no quotation marks. Max 4 short sentences. Sound like a real person, not a template. Never invent facts not in the metadata. End with a soft ask (listen / let me know what you think) and a brief sign-off like Blessings or Appreciate you. Do NOT include hashtags. IMPORTANT: Do NOT use em dashes (\u2014) or en dashes (\u2013) anywhere in your response. Use periods, commas, or short separate sentences instead. Em dashes read as AI-generated and kill the human tone.';
  const userPrompt = [
    `Platform: ${contact.platform}`,
    `Recipient name: ${contact.name}`,
    contact.handle ? `Recipient handle: @${contact.handle}` : '',
    contact.category ? `Recipient category: ${contact.category}` : '',
    contact.notes ? `Notes about this recipient: ${contact.notes}` : '',
    ``,
    `Release metadata:`,
    `- Song title: ${meta.song_title}`,
    meta.artist_name ? `- Artist: ${meta.artist_name}${meta.feat ? ' feat. ' + meta.feat : ''}` : '',
    meta.genre ? `- Genre: ${meta.genre}` : '',
    meta.release_date ? `- Release date: ${meta.release_date}` : '',
    meta.mood ? `- Mood / vibe: ${meta.mood}` : '',
    meta.bio ? `- Artist bio: ${meta.bio}` : '',
    meta.link ? `- Listen link (include at the end on its own line): ${meta.link}` : '',
    ``,
    `Write the DM now. Greet ${contact.name} by name.`
  ].filter(Boolean).join('\n');

  try {
    const aiResp = await fetch('https://api.anthropic.com/v1/messages', {
      method: 'POST',
      headers: {
        'x-api-key': CLAUDE_KEY,
        'anthropic-version': '2023-06-01',
        'content-type': 'application/json'
      },
      body: JSON.stringify({
        model: DEFAULT_CLAUDE_MODEL,
        max_tokens: 400,
        system,
        messages: [{ role: 'user', content: userPrompt }]
      }),
      signal: AbortSignal.timeout(60000)
    });
    if (!aiResp.ok) {
      const errText = await aiResp.text();
      console.error('[OUTREACH SOCIAL-DM] Anthropic error:', aiResp.status, errText.slice(0, 500));
      return res.status(502).json({ error: 'AI service unavailable', fallback: true });
    }
    const data = await aiResp.json();
    const text = (data.content && data.content[0] && data.content[0].text) || '';
    try {
      if (req.user.role !== 'admin') {
        recordClaudeUsage(req.user.id, data.usage?.input_tokens || 0, data.usage?.output_tokens || 0);
      }
    } catch(_) {}
    if (!text.trim()) {
      return res.status(502).json({ error: 'Empty AI response', fallback: true });
    }
    // Strip em/en dashes even if the model slips past the system-prompt ban.
    // Em dash becomes comma-space, en dash becomes simple hyphen.
    const cleaned = text.trim().replace(/\u2014/g, ', ').replace(/\u2013/g, '-');

    // Notify admin when Elite/Elite Plus users generate social DMs.
    if (isEliteTier(req.user.subscription_tier)) {
      const tierLabel = req.user.subscription_tier === 'elite_plus' ? 'Elite Plus' : 'Elite';
      notifyAdmins(
        `${tierLabel} Social DM Generated — ${req.user.email}`,
        `<h2 style="color:#fff; margin-top:0;">Social DM Generated</h2>
         <p><strong>User:</strong> ${req.user.email} (${tierLabel})</p>
         <p><strong>Contact:</strong> ${contact.name} (${contact.platform})</p>
         <p><strong>Song:</strong> ${meta.song_title || 'N/A'}</p>`
      ).catch(() => {});
    }

    res.json({ dm: cleaned, platform: contact.platform, model: DEFAULT_CLAUDE_MODEL });
  } catch (err) {
    console.error('[OUTREACH SOCIAL-DM] fetch error:', err.message);
    res.status(502).json({ error: 'AI request failed', fallback: true });
  }
});

// Intake — audio-to-lyrics transcription via Groq Whisper-large-v3.
// Accepts a base64-encoded audio file (mp3/wav/m4a/etc), posts it to
// Groq as multipart/form-data, returns the raw transcript text.
// Genius-format section headers are added in a separate Claude step
// (see /api/intake/format-lyrics-genius). Rate-limited via rlTranscribe
// (20/hr/user) and access-gated via requireAccess (trial users get full
// intake per R11). Route-specific body parser lifts the JSON cap to
// 30MB to accommodate base64-inflated audio (~22MB raw audio ceiling,
// well below Groq's 25MB binary limit).
app.post('/api/intake/transcribe-lyrics',
  express.json({ limit: '30mb' }),
  requireAccess,
  rlTranscribe,
  async (req, res) => {
    const GROQ_KEY = process.env.GROQ_API_KEY || '';
    if (!GROQ_KEY) return res.status(503).json({ error: 'Transcription not configured' });

    const body = req.body || {};
    const audioBase64 = typeof body.audioBase64 === 'string' ? body.audioBase64 : '';
    const mimeType = typeof body.mimeType === 'string' ? body.mimeType.slice(0, 100) : '';
    const filename = typeof body.filename === 'string' ? body.filename.slice(0, 200).replace(/[^\w.\-]/g, '_') : 'audio.mp3';
    const hints = (body.promptHints && typeof body.promptHints === 'object') ? body.promptHints : {};

    if (!audioBase64) return res.status(400).json({ error: 'audioBase64 required' });
    if (!mimeType.startsWith('audio/')) return res.status(400).json({ error: 'mimeType must be audio/*' });

    // Build a Whisper prompt from intake metadata. Whisper uses the
    // prompt parameter as vocabulary priming for proper nouns, slang,
    // and project-specific terms. Artist/song/producer names that
    // would otherwise transcribe as nonsense words land correctly
    // when they show up verbatim here. Capped at 1000 chars (well
    // under Whisper's ~244-token soft limit where older context gets
    // silently truncated). Empty strings and missing fields dropped.
    const cap = (v, n) => (v == null ? '' : String(v)).slice(0, n).trim();
    const promptParts = [];
    const pSong = cap(hints.songTitle, 200);
    const pArtist = cap(hints.primaryArtist, 150);
    const pFeat = cap(hints.featArtist, 150);
    const pProducer = cap(hints.producer, 150);
    const pGenre = cap(hints.genrePrimary, 100);
    const pLabel = cap(hints.label, 150);
    if (pArtist) promptParts.push(pFeat ? `${pArtist} featuring ${pFeat}` : pArtist);
    if (pSong) promptParts.push(`"${pSong}"`);
    if (pProducer) promptParts.push(`produced by ${pProducer}`);
    if (pGenre) promptParts.push(pGenre);
    if (pLabel) promptParts.push(pLabel);
    // CHH priming always appended — biases Whisper toward Christian
    // hip hop vocabulary (Jesus, gospel, scripture names, common CHH
    // ad-libs) since the user base is 100% CHH artists.
    promptParts.push('Christian hip hop lyrics');
    const whisperPrompt = (promptParts.join('. ') + '.').slice(0, 1000);

    let buf;
    try {
      buf = Buffer.from(audioBase64, 'base64');
    } catch (e) {
      return res.status(400).json({ error: 'Invalid base64 audio payload' });
    }
    // Groq hard limit is 25MB for the uploaded file itself.
    if (buf.length === 0) return res.status(400).json({ error: 'Empty audio payload' });
    if (buf.length > 25 * 1024 * 1024) {
      return res.status(413).json({ error: 'Audio file too large (max 25MB). Try compressing to 128kbps MP3.' });
    }

    try {
      const form = new FormData();
      form.append('file', new Blob([buf], { type: mimeType }), filename);
      form.append('model', 'whisper-large-v3');
      form.append('response_format', 'text');
      form.append('temperature', '0');
      form.append('language', 'en');
      form.append('prompt', whisperPrompt);

      const groqResp = await fetch('https://api.groq.com/openai/v1/audio/transcriptions', {
        method: 'POST',
        headers: { 'Authorization': `Bearer ${GROQ_KEY}` },
        body: form,
        signal: AbortSignal.timeout(120000)
      });

      if (!groqResp.ok) {
        const errText = await groqResp.text();
        console.error('[TRANSCRIBE] Groq error:', groqResp.status, errText.slice(0, 500));
        if (groqResp.status === 429) return res.status(429).json({ error: 'Transcription rate limit hit, try again in a minute' });
        return res.status(502).json({ error: 'Transcription service unavailable' });
      }

      // response_format=text returns a plain string body, not JSON.
      const text = (await groqResp.text()).trim();
      if (!text) return res.status(502).json({ error: 'Empty transcription' });

      // Strip em/en dashes even on raw transcription (project-wide rule,
      // see feedback_no_em_dashes_in_ai_output.md).
      const cleaned = text.replace(/\u2014/g, ', ').replace(/\u2013/g, '-');
      res.json({ text: cleaned, model: 'whisper-large-v3', bytes: buf.length });
    } catch (err) {
      console.error('[TRANSCRIBE] fetch error:', err.message);
      res.status(502).json({ error: 'Transcription request failed' });
    }
  }
);

// Intake — Genius-format lyric sectioning via Claude. Takes the current
// lyrics textarea content (either a raw Whisper transcript or manually
// typed lyrics) and injects Genius-style section headers ([Intro],
// [Verse 1], [Chorus], [Verse 2], [Bridge], [Outro], etc). Preserves the
// user's exact words and line breaks; only adds bracketed headers and
// normalizes obvious line-break issues. Shares rlClaude with other
// Claude endpoints. Trial users can access (requireAccess, per R11).
app.post('/api/intake/format-lyrics-genius', requireAccess, rlClaude, async (req, res) => {
  const CLAUDE_KEY = process.env.CLAUDE_API_KEY || '';
  if (!CLAUDE_KEY) return res.status(503).json({ error: 'AI features not configured' });

  const raw = typeof (req.body && req.body.lyrics) === 'string' ? req.body.lyrics : '';
  const trimmed = raw.trim();
  if (!trimmed) return res.status(400).json({ error: 'lyrics required' });
  // Hard cap to keep prompt bounded. 12KB is ~2500 words, well above any
  // realistic song.
  if (trimmed.length > 12000) {
    return res.status(413).json({ error: 'Lyrics too long (max ~12,000 characters)' });
  }

  const system = 'You format song lyrics into the genius.com convention by inserting bracketed section headers. Return ONLY the formatted lyrics. No preamble, no commentary, no code fences, no quotes, no explanations. PRESERVE the user\'s exact words, spelling, and line breaks. Do NOT paraphrase, rewrite, correct, or add lyrics. Do NOT remove lyrics. Only two things are allowed: (1) insert bracketed section headers on their own line before the relevant block, and (2) collapse runs of 3+ blank lines to a single blank line. Section headers use these exact formats: [Intro], [Verse 1], [Verse 2], [Pre-Chorus], [Chorus], [Post-Chorus], [Refrain], [Bridge], [Breakdown], [Interlude], [Hook], [Outro]. Number verses sequentially starting at 1. Detect repeated choruses by matching content and label them all [Chorus] without numbering. If the song has a clear hook that repeats, use [Hook] consistently. If you cannot confidently identify a section, default to [Verse N]. Add a single blank line between sections. Do NOT use em dashes (U+2014) or en dashes (U+2013) anywhere.';

  const userPrompt = `Format these lyrics for genius.com. Remember: insert bracketed section headers only, preserve words and line breaks exactly, return only the formatted lyrics.\n\n---\n${trimmed}\n---`;

  try {
    const aiResp = await fetch('https://api.anthropic.com/v1/messages', {
      method: 'POST',
      headers: {
        'x-api-key': CLAUDE_KEY,
        'anthropic-version': '2023-06-01',
        'content-type': 'application/json'
      },
      body: JSON.stringify({
        model: DEFAULT_CLAUDE_MODEL,
        max_tokens: 4000,
        system,
        messages: [{ role: 'user', content: userPrompt }]
      }),
      signal: AbortSignal.timeout(90000)
    });
    if (!aiResp.ok) {
      const errText = await aiResp.text();
      console.error('[FORMAT-LYRICS] Anthropic error:', aiResp.status, errText.slice(0, 500));
      return res.status(502).json({ error: 'AI service unavailable' });
    }
    const data = await aiResp.json();
    const text = (data.content && data.content[0] && data.content[0].text) || '';
    try {
      if (req.user.role !== 'admin') {
        recordClaudeUsage(req.user.id, data.usage?.input_tokens || 0, data.usage?.output_tokens || 0);
      }
    } catch(_) {}
    if (!text.trim()) return res.status(502).json({ error: 'Empty AI response' });
    const cleaned = text.trim().replace(/\u2014/g, ', ').replace(/\u2013/g, '-');
    res.json({ lyrics: cleaned, model: DEFAULT_CLAUDE_MODEL });
  } catch (err) {
    console.error('[FORMAT-LYRICS] fetch error:', err.message);
    res.status(502).json({ error: 'AI request failed' });
  }
});

// Generate a 300-500 word professional press release from intake metadata.
// Best-practice rules baked into the system prompt: FOR IMMEDIATE RELEASE
// header, headline, City/State/Date dateline, lead/body/summary structure,
// optional artist quote (only if metadata supports it), boilerplate, ###
// close marker, contact block. requireAccess (R11 — trial users included).
app.post('/api/intake/generate-press-release', requireAccess, rlClaude, async (req, res) => {
  const CLAUDE_KEY = process.env.CLAUDE_API_KEY || '';
  if (!CLAUDE_KEY) return res.status(503).json({ error: 'AI features not configured' });

  const rd = (req.body && typeof req.body.releaseData === 'object') ? req.body.releaseData : {};
  const cap = (v, n) => (v == null ? '' : String(v)).slice(0, n).trim();
  const meta = {
    songTitle: cap(rd.songTitle, 200),
    primaryArtist: cap(rd.primaryArtist, 200),
    featArtist: cap(rd.featArtist, 200),
    producer: cap(rd.producer, 200),
    albumName: cap(rd.albumName, 200),
    genrePrimary: cap(rd.genrePrimary, 100),
    genreSecondary: cap(rd.genreSecondary, 100),
    releaseDate: cap(rd.releaseDate, 50),
    label: cap(rd.label, 200),
    bio: cap(rd.bio, 2000),
    songDescription: cap(rd.songDescription, 2000),
    hometownCity: cap(rd.hometownCity, 100),
    hometownState: cap(rd.hometownState, 100),
    legalName: cap(rd.legalName || rd.stageName, 200),
    stageName: cap(rd.stageName, 200),
    email: cap(rd.email, 200),
    website: cap(rd.website, 300),
    linkSpotify: cap(rd.linkSpotify, 500),
    linkApple: cap(rd.linkApple, 500),
    linkYTVideo: cap(rd.linkYTVideo, 500),
    instagram: cap(rd.instagram, 200),
    tiktok: cap(rd.tiktok, 200),
    twitter: cap(rd.twitter, 200),
    facebook: cap(rd.facebook, 200)
  };
  if (!meta.songTitle) return res.status(400).json({ error: 'Song title required' });
  if (!meta.primaryArtist) return res.status(400).json({ error: 'Primary artist required' });

  const system = [
    'You are a professional music publicist writing a press release for an indie music release. Output ONLY the press release text - no preamble, no commentary, no code fences, no markdown formatting, no quotes around the whole thing.',
    '',
    'STRUCTURE (exact order, blank line between each block):',
    '1. First line: FOR IMMEDIATE RELEASE',
    '2. A single concise headline in plain text - clear, descriptive, no clickbait, do NOT wrap the headline in quotation marks.',
    '3. Dateline + lead paragraph as one block: "City, State - Month DD, YYYY - " followed inline by a 2-4 sentence lead covering who, what, when, where, why, plus one compelling differentiator.',
    '4. Body paragraph (3-5 sentences) with facts, context, and what makes the song stand out. If a quote can be drawn faithfully from the song description supplied below, include exactly one direct quote from the artist on its own block, formatted as: "Quote text," says [Artist Name]. If the metadata cannot support a real quote, OMIT the quote entirely - do NOT invent one.',
    '5. Summary paragraph (2-3 sentences) with a clear call to action and listening links / socials inline.',
    '6. About [Artist Name] boilerplate - 2-3 sentences serving as an elevator pitch, drawn from the supplied bio.',
    '7. A single ### line.',
    '8. Contact block: "Contact:" line, then name / website / email each on their own line if supplied. Omit any contact line for which no value was supplied.',
    '',
    'HARD CONSTRAINTS:',
    '- Total length 300-500 words. Never longer than one page.',
    '- Third person throughout, except inside the artist quote (which is first person).',
    '- Never invent facts not present in the supplied metadata. If a piece is missing (no hometown, no quote, no label, no link), omit it gracefully - do NOT hallucinate.',
    '- Do NOT use em dashes (U+2014) or en dashes (U+2013) anywhere. Use a regular hyphen "-" or rewrite the sentence.',
    '- No marketing fluff like "groundbreaking", "game-changing", "next-level", "must-listen".',
    '- Do not make claims about chart positions, awards, or sales figures unless they are explicitly in the bio.',
    '- If hometown is missing, use the label location if supplied; otherwise drop the city from the dateline and use just "Date - " in the dateline (still including the date).'
  ].join('\n');

  const lines = [
    `Song title: ${meta.songTitle}`,
    `Primary artist: ${meta.primaryArtist}`,
    meta.featArtist ? `Featuring: ${meta.featArtist}` : '',
    meta.producer ? `Producer: ${meta.producer}` : '',
    meta.albumName ? `Album / single name: ${meta.albumName}` : '',
    meta.genrePrimary ? `Primary genre: ${meta.genrePrimary}` : '',
    meta.genreSecondary ? `Secondary genre: ${meta.genreSecondary}` : '',
    meta.releaseDate ? `Release date: ${meta.releaseDate}` : '',
    meta.label ? `Label: ${meta.label}` : '',
    (meta.hometownCity || meta.hometownState) ? `Hometown: ${[meta.hometownCity, meta.hometownState].filter(Boolean).join(', ')}` : '',
    meta.songDescription ? `Song description / pitch (use this to ground the body paragraph and any quote): ${meta.songDescription}` : '',
    meta.bio ? `Artist bio (use this for the About boilerplate): ${meta.bio}` : '',
    meta.linkSpotify ? `Spotify link: ${meta.linkSpotify}` : '',
    meta.linkApple ? `Apple Music link: ${meta.linkApple}` : '',
    meta.linkYTVideo ? `YouTube video link: ${meta.linkYTVideo}` : '',
    meta.instagram ? `Instagram: ${meta.instagram}` : '',
    meta.tiktok ? `TikTok: ${meta.tiktok}` : '',
    meta.twitter ? `X / Twitter: ${meta.twitter}` : '',
    meta.facebook ? `Facebook: ${meta.facebook}` : '',
    meta.website ? `Website: ${meta.website}` : '',
    meta.legalName ? `Contact name: ${meta.legalName}` : '',
    meta.email ? `Contact email: ${meta.email}` : ''
  ].filter(Boolean);

  const userPrompt = 'Write a press release for the release described below. Follow the structure and constraints exactly.\n\n' + lines.join('\n');

  try {
    const aiResp = await fetch('https://api.anthropic.com/v1/messages', {
      method: 'POST',
      headers: {
        'x-api-key': CLAUDE_KEY,
        'anthropic-version': '2023-06-01',
        'content-type': 'application/json'
      },
      body: JSON.stringify({
        model: DEFAULT_CLAUDE_MODEL,
        max_tokens: 1200,
        system,
        messages: [{ role: 'user', content: userPrompt }]
      }),
      signal: AbortSignal.timeout(90000)
    });
    if (!aiResp.ok) {
      const errText = await aiResp.text();
      console.error('[PRESS-RELEASE] Anthropic error:', aiResp.status, errText.slice(0, 500));
      return res.status(502).json({ error: 'AI service unavailable' });
    }
    const data = await aiResp.json();
    const text = (data.content && data.content[0] && data.content[0].text) || '';
    try {
      if (req.user.role !== 'admin') {
        recordClaudeUsage(req.user.id, data.usage?.input_tokens || 0, data.usage?.output_tokens || 0);
      }
    } catch(_) {}
    if (!text.trim()) return res.status(502).json({ error: 'Empty AI response' });
    const cleaned = text.trim().replace(/\u2014/g, '-').replace(/\u2013/g, '-');

    // Notify admin when Elite/Elite Plus users generate a press release.
    if (isEliteTier(req.user.subscription_tier)) {
      const tierLabel = req.user.subscription_tier === 'elite_plus' ? 'Elite Plus' : 'Elite';
      notifyAdmins(
        `${tierLabel} Press Release Generated — ${req.user.email}`,
        `<h2 style="color:#fff; margin-top:0;">Press Release Generated</h2>
         <p><strong>User:</strong> ${req.user.email} (${tierLabel})</p>
         <p><strong>Song:</strong> ${meta.songTitle}</p>
         <p><strong>Artist:</strong> ${meta.primaryArtist}</p>
         ${meta.releaseDate ? `<p><strong>Release Date:</strong> ${meta.releaseDate}</p>` : ''}
         ${meta.genrePrimary ? `<p><strong>Genre:</strong> ${meta.genrePrimary}</p>` : ''}`
      ).catch(() => {});
    }

    res.json({ pressRelease: cleaned, model: DEFAULT_CLAUDE_MODEL });
  } catch (err) {
    console.error('[PRESS-RELEASE] fetch error:', err.message);
    res.status(502).json({ error: 'AI request failed' });
  }
});

// Mark a submission as complete + grant +25 XP inline. Unique index on
// (user_id, contact_id) means repeat submissions for the same contact are
// idempotent — we don't double-grant XP on re-clicks.
app.post('/api/outreach/submission/mark', requireOutreachUnlocked, (req, res) => {
  const contactId = parseInt((req.body || {}).contact_id, 10);
  if (!Number.isInteger(contactId) || contactId <= 0) return res.status(400).json({ error: 'contact_id required' });
  const releaseId = String((req.body || {}).release_id || '').slice(0, 100) || null;
  const notes = String((req.body || {}).notes || '').slice(0, 1000) || null;
  const contact = dbHelpers.prepare('SELECT id, category FROM outreach_contacts WHERE id = ?').get(contactId);
  if (!contact) return res.status(404).json({ error: 'Contact not found' });

  // Idempotent insert — if a row already exists, skip the XP grant.
  const existing = dbHelpers.prepare('SELECT id FROM submission_progress WHERE user_id = ? AND contact_id = ?').get(req.user.id, contactId);
  let awardedXp = 0;
  if (!existing) {
    dbHelpers.prepare(`
      INSERT INTO submission_progress (user_id, contact_id, status, release_id, notes)
      VALUES (?, ?, 'submitted', ?, ?)
    `).run(req.user.id, contactId, releaseId, notes);
    // Inline XP grant — match the existing xp_log + user_xp pattern.
    // 25 XP per submission.
    try {
      const XP = 25;
      dbHelpers.prepare(`
        INSERT INTO xp_log (user_id, action, xp_amount, description)
        VALUES (?, 'outreach_submission', ?, 'Outreach submission')
      `).run(req.user.id, XP);
      dbHelpers.prepare(`
        INSERT INTO user_xp (user_id, total_xp, level, last_active_date)
        VALUES (?, ?, 1, date('now'))
        ON CONFLICT(user_id) DO UPDATE SET
          total_xp = total_xp + ?,
          last_active_date = date('now')
      `).run(req.user.id, XP, XP);
      awardedXp = XP;
    } catch (e) {
      console.error('[OUTREACH] XP grant error:', e.message);
    }
    flushDbNow();
  }
  res.json({ success: true, awarded_xp: awardedXp, already_submitted: !!existing });
});

// Support tickets — user submits a ticket
app.post('/api/support/submit', requireAuth, rlSupport, async (req, res) => {
  let { subject, message } = req.body;
  if (!subject || !message) return res.status(400).json({ error: 'Subject and message required' });
  // Cap input lengths so a malicious or buggy client can't burn Claude tokens
  // by sending megabytes of context. Mirrors the limits used elsewhere.
  subject = String(subject).slice(0, 200);
  message = String(message).slice(0, 5000);

  // AI auto-response attempt using Claude
  let aiResponse = null;
  let escalated = 0;
  const CLAUDE_KEY = process.env.CLAUDE_API_KEY || '';

  if (CLAUDE_KEY) {
    try {
      const aiResp = await fetch('https://api.anthropic.com/v1/messages', {
        method: 'POST',
        headers: {
          'x-api-key': CLAUDE_KEY,
          'anthropic-version': '2023-06-01',
          'content-type': 'application/json'
        },
        body: JSON.stringify({
          model: DEFAULT_CLAUDE_MODEL,
          max_tokens: 500,
          system: `You are Rollout Heaven's support assistant. Rollout Heaven is a music release management SaaS tool. Answer the user's question helpfully and concisely. If you cannot confidently answer (billing issues, account problems, bugs, feature requests, or anything requiring human judgment), respond with exactly "ESCALATE" and nothing else.`,
          messages: [{ role: 'user', content: `Subject: ${subject}\n\n${message}` }]
        })
      });
      if (aiResp.ok) {
        const data = await aiResp.json();
        const text = data.content?.[0]?.text || '';
        if (text.trim() === 'ESCALATE') {
          escalated = 1;
        } else {
          aiResponse = text;
        }
      } else {
        escalated = 1;
      }
    } catch(e) {
      escalated = 1;
    }
  } else {
    escalated = 1;
  }

  dbHelpers.prepare(`
    INSERT INTO support_tickets (user_id, user_email, subject, message, ai_response, escalated)
    VALUES (?, ?, ?, ?, ?, ?)
  `).run(req.user.id, req.user.email, subject, message, aiResponse, escalated);
  flushDbNow(); // durable write — C2

  res.json({ success: true, aiResponse, escalated });
});

// User's own tickets
app.get('/api/support/my-tickets', requireAuth, (req, res) => {
  const tickets = dbHelpers.prepare('SELECT * FROM support_tickets WHERE user_id = ? AND deleted_at IS NULL ORDER BY created_at DESC').all(req.user.id);
  res.json({ tickets });
});

// --- Gamification System ---

// Level thresholds: level N requires LEVEL_XP[N-1] total XP
const LEVEL_XP = [0, 100, 300, 600, 1000, 1500, 2200, 3000, 4000, 5200, 6500, 8000, 10000, 12500, 15500, 19000, 23000, 27500, 32500, 38000];
const LEVEL_NAMES = ['Newcomer','Beat Dropper','Vibe Setter','Playlist Pusher','Hook Writer','Content Creator','Trend Spotter','Campaign Runner','Release Pro','Chart Climber','Buzz Builder','Fan Magnet','Brand Builder','Industry Mover','Hit Maker','Platinum Push','Label Ready','Viral Force','Legendary','Hall of Fame'];

// XP values per action
const XP_VALUES = {
  login: 10,
  campaign_generate: 50,
  task_complete: 15,
  email_generate: 25,
  research_run: 20,
  playlist_submit: 20,
  content_copy: 5,
  release_complete: 100,
  streak_bonus: 25  // bonus per day of streak (3+ days)
};

// Achievement definitions
const ACHIEVEMENTS = [
  // Getting Started
  { id: 'first_login', name: 'First Steps', desc: 'Log in for the first time', icon: '🚀', category: 'Getting Started', xp: 25 },
  { id: 'first_campaign', name: 'Campaign Launched', desc: 'Generate your first campaign', icon: '🎯', category: 'Getting Started', xp: 50 },
  { id: 'first_email', name: 'Inbox Ready', desc: 'Generate your first email', icon: '📧', category: 'Getting Started', xp: 25 },
  { id: 'first_research', name: 'Scout Mode', desc: 'Run your first research query', icon: '🔍', category: 'Getting Started', xp: 25 },
  // Consistency
  { id: 'streak_3', name: 'On a Roll', desc: '3-day login streak', icon: '🔥', category: 'Consistency', xp: 50 },
  { id: 'streak_7', name: 'Week Warrior', desc: '7-day login streak', icon: '⚡', category: 'Consistency', xp: 100 },
  { id: 'streak_14', name: 'Unstoppable', desc: '14-day login streak', icon: '💎', category: 'Consistency', xp: 200 },
  { id: 'streak_30', name: 'Monthly Legend', desc: '30-day login streak', icon: '👑', category: 'Consistency', xp: 500 },
  // Productivity
  { id: 'tasks_5', name: 'Task Crusher', desc: 'Complete 5 checklist items', icon: '✅', category: 'Productivity', xp: 50 },
  { id: 'tasks_25', name: 'Grind Mode', desc: 'Complete 25 checklist items', icon: '💪', category: 'Productivity', xp: 150 },
  { id: 'tasks_50', name: 'Machine', desc: 'Complete 50 checklist items', icon: '🤖', category: 'Productivity', xp: 300 },
  { id: 'tasks_100', name: 'Centurion', desc: 'Complete 100 checklist items', icon: '🏆', category: 'Productivity', xp: 500 },
  // Content & Outreach
  { id: 'emails_5', name: 'Email Pro', desc: 'Generate 5 emails', icon: '📬', category: 'Outreach', xp: 75 },
  { id: 'emails_25', name: 'Outreach King', desc: 'Generate 25 emails', icon: '👑', category: 'Outreach', xp: 250 },
  { id: 'research_10', name: 'Deep Diver', desc: 'Run 10 research queries', icon: '🧠', category: 'Research', xp: 100 },
  { id: 'research_50', name: 'Intel Master', desc: 'Run 50 research queries', icon: '🕵️', category: 'Research', xp: 300 },
  { id: 'playlists_10', name: 'Playlist Hunter', desc: 'Submit to 10 playlists', icon: '🎵', category: 'Outreach', xp: 100 },
  { id: 'playlists_50', name: 'Playlist Legend', desc: 'Submit to 50 playlists', icon: '🎶', category: 'Outreach', xp: 400 },
  { id: 'content_25', name: 'Content Machine', desc: 'Copy 25 content pieces', icon: '📝', category: 'Content', xp: 75 },
  { id: 'content_100', name: 'Content Factory', desc: 'Copy 100 content pieces', icon: '🏭', category: 'Content', xp: 250 },
  // Campaigns & Releases
  { id: 'campaigns_3', name: 'Triple Threat', desc: 'Generate 3 campaigns', icon: '🎪', category: 'Campaigns', xp: 100 },
  { id: 'campaigns_10', name: 'Campaign Veteran', desc: 'Generate 10 campaigns', icon: '🎖️', category: 'Campaigns', xp: 300 },
  { id: 'releases_1', name: 'First Release', desc: 'Complete your first release', icon: '💿', category: 'Releases', xp: 100 },
  { id: 'releases_5', name: 'Discography Builder', desc: 'Complete 5 releases', icon: '📀', category: 'Releases', xp: 300 },
  { id: 'releases_10', name: 'Catalog King', desc: 'Complete 10 releases', icon: '🎤', category: 'Releases', xp: 500 },
  // Levels
  { id: 'level_5', name: 'Rising Star', desc: 'Reach Level 5', icon: '⭐', category: 'Levels', xp: 100 },
  { id: 'level_10', name: 'Established', desc: 'Reach Level 10', icon: '🌟', category: 'Levels', xp: 250 },
  { id: 'level_15', name: 'Elite', desc: 'Reach Level 15', icon: '💫', category: 'Levels', xp: 500 },
  { id: 'level_20', name: 'Hall of Fame', desc: 'Reach Level 20', icon: '🏛️', category: 'Levels', xp: 1000 },
  // XP Milestones
  { id: 'xp_1000', name: 'First Thousand', desc: 'Earn 1,000 XP', icon: '🔋', category: 'XP Milestones', xp: 50 },
  { id: 'xp_5000', name: 'Five Stack', desc: 'Earn 5,000 XP', icon: '🔥', category: 'XP Milestones', xp: 150 },
  { id: 'xp_10000', name: 'XP Legend', desc: 'Earn 10,000 XP', icon: '💰', category: 'XP Milestones', xp: 300 },
];

function getLevel(totalXp) {
  let level = 1;
  for (let i = LEVEL_XP.length - 1; i >= 0; i--) {
    if (totalXp >= LEVEL_XP[i]) { level = i + 1; break; }
  }
  return Math.min(level, 20);
}

function ensureUserXp(userId) {
  const existing = dbHelpers.prepare('SELECT id FROM user_xp WHERE user_id = ?').get(userId);
  if (!existing) {
    dbHelpers.prepare('INSERT INTO user_xp (user_id) VALUES (?)').run(userId);
  }
}

function checkAndUnlockAchievements(userId) {
  const xp = dbHelpers.prepare('SELECT * FROM user_xp WHERE user_id = ?').get(userId);
  if (!xp) return [];
  const unlocked = dbHelpers.prepare('SELECT achievement_id FROM user_achievements WHERE user_id = ?').all(userId);
  const unlockedIds = new Set(unlocked.map(a => a.achievement_id));
  const newlyUnlocked = [];

  const checks = {
    first_login: xp.logins_total >= 1,
    first_campaign: xp.campaigns_generated >= 1,
    first_email: xp.emails_generated >= 1,
    first_research: xp.research_runs >= 1,
    streak_3: xp.current_streak >= 3 || xp.longest_streak >= 3,
    streak_7: xp.current_streak >= 7 || xp.longest_streak >= 7,
    streak_14: xp.current_streak >= 14 || xp.longest_streak >= 14,
    streak_30: xp.current_streak >= 30 || xp.longest_streak >= 30,
    tasks_5: xp.tasks_completed >= 5,
    tasks_25: xp.tasks_completed >= 25,
    tasks_50: xp.tasks_completed >= 50,
    tasks_100: xp.tasks_completed >= 100,
    emails_5: xp.emails_generated >= 5,
    emails_25: xp.emails_generated >= 25,
    research_10: xp.research_runs >= 10,
    research_50: xp.research_runs >= 50,
    playlists_10: xp.playlists_submitted >= 10,
    playlists_50: xp.playlists_submitted >= 50,
    content_25: xp.content_copied >= 25,
    content_100: xp.content_copied >= 100,
    campaigns_3: xp.campaigns_generated >= 3,
    campaigns_10: xp.campaigns_generated >= 10,
    releases_1: xp.releases_completed >= 1,
    releases_5: xp.releases_completed >= 5,
    releases_10: xp.releases_completed >= 10,
    level_5: xp.level >= 5,
    level_10: xp.level >= 10,
    level_15: xp.level >= 15,
    level_20: xp.level >= 20,
    xp_1000: xp.total_xp >= 1000,
    xp_5000: xp.total_xp >= 5000,
    xp_10000: xp.total_xp >= 10000,
  };

  for (const [achId, met] of Object.entries(checks)) {
    if (met && !unlockedIds.has(achId)) {
      const ach = ACHIEVEMENTS.find(a => a.id === achId);
      if (ach) {
        dbHelpers.prepare('INSERT OR IGNORE INTO user_achievements (user_id, achievement_id) VALUES (?, ?)').run(userId, achId);
        // Award bonus XP for achievement
        const newTotal = xp.total_xp + ach.xp;
        const newLevel = getLevel(newTotal);
        dbHelpers.prepare('UPDATE user_xp SET total_xp = ?, level = ?, updated_at = datetime("now") WHERE user_id = ?').run(newTotal, newLevel, userId);
        dbHelpers.prepare('INSERT INTO xp_log (user_id, action, xp_amount, description) VALUES (?, ?, ?, ?)').run(userId, 'achievement', ach.xp, 'Unlocked: ' + ach.name);
        xp.total_xp = newTotal;
        xp.level = newLevel;
        newlyUnlocked.push(ach);
      }
    }
  }
  return newlyUnlocked;
}

// GET gamification state
app.get('/api/gamification', requireAuth, (req, res) => {
  ensureUserXp(req.user.id);

  // Update login streak
  const xp = dbHelpers.prepare('SELECT * FROM user_xp WHERE user_id = ?').get(req.user.id);
  const today = new Date().toISOString().split('T')[0];

  if (xp.last_active_date !== today) {
    const yesterday = new Date(Date.now() - 86400000).toISOString().split('T')[0];
    let newStreak = xp.current_streak;
    if (xp.last_active_date === yesterday) {
      newStreak += 1;
    } else {
      newStreak = 1; // streak broken (M7: outer if guarantees not today)
    }
    const longest = Math.max(xp.longest_streak, newStreak);
    const loginXp = XP_VALUES.login + (newStreak >= 3 ? XP_VALUES.streak_bonus : 0);
    const newTotal = xp.total_xp + loginXp;
    const newLevel = getLevel(newTotal);
    dbHelpers.prepare('UPDATE user_xp SET last_active_date = ?, current_streak = ?, longest_streak = ?, logins_total = logins_total + 1, total_xp = ?, level = ?, updated_at = datetime("now") WHERE user_id = ?')
      .run(today, newStreak, longest, newTotal, newLevel, req.user.id);
    dbHelpers.prepare('INSERT INTO xp_log (user_id, action, xp_amount, description) VALUES (?, ?, ?, ?)').run(req.user.id, 'login', loginXp, 'Daily login' + (newStreak >= 3 ? ' (streak bonus!)' : ''));
  }

  // Check achievements
  const newAchievements = checkAndUnlockAchievements(req.user.id);

  // Re-fetch updated data
  const updated = dbHelpers.prepare('SELECT * FROM user_xp WHERE user_id = ?').get(req.user.id);
  const achievements = dbHelpers.prepare('SELECT achievement_id, unlocked_at FROM user_achievements WHERE user_id = ?').all(req.user.id);
  const recentXp = dbHelpers.prepare('SELECT action, xp_amount, description, created_at FROM xp_log WHERE user_id = ? ORDER BY created_at DESC LIMIT 20').all(req.user.id);

  // Calculate level progress
  const currentLevelXp = LEVEL_XP[updated.level - 1] || 0;
  const nextLevelXp = LEVEL_XP[updated.level] || LEVEL_XP[LEVEL_XP.length - 1];
  const progressXp = updated.total_xp - currentLevelXp;
  const neededXp = nextLevelXp - currentLevelXp;

  res.json({
    xp: updated.total_xp,
    level: updated.level,
    levelName: LEVEL_NAMES[updated.level - 1] || 'Legend',
    nextLevelName: LEVEL_NAMES[updated.level] || 'Max Level',
    progressXp,
    neededXp,
    progressPercent: neededXp > 0 ? Math.min(100, Math.round((progressXp / neededXp) * 100)) : 100,
    streak: updated.current_streak,
    longestStreak: updated.longest_streak,
    stats: {
      tasks_completed: updated.tasks_completed,
      emails_generated: updated.emails_generated,
      research_runs: updated.research_runs,
      playlists_submitted: updated.playlists_submitted,
      campaigns_generated: updated.campaigns_generated,
      content_copied: updated.content_copied,
      releases_completed: updated.releases_completed,
      logins_total: updated.logins_total
    },
    achievements: achievements.map(a => {
      const def = ACHIEVEMENTS.find(d => d.id === a.achievement_id);
      return { ...def, unlocked_at: a.unlocked_at };
    }).filter(Boolean),
    allAchievements: ACHIEVEMENTS,
    newAchievements,
    recentXp,
    levelThresholds: LEVEL_XP,
    levelNames: LEVEL_NAMES
  });
});

// POST award XP for an action
app.post('/api/gamification/award', requireAuth, (req, res) => {
  const { action } = req.body;
  if (!action || !XP_VALUES[action]) return res.status(400).json({ error: 'Invalid action' });

  ensureUserXp(req.user.id);
  const xp = dbHelpers.prepare('SELECT * FROM user_xp WHERE user_id = ?').get(req.user.id);
  const amount = XP_VALUES[action];
  const newTotal = xp.total_xp + amount;
  const newLevel = getLevel(newTotal);
  const leveledUp = newLevel > xp.level;

  // Static SQL per action — no string interpolation of column names. Even
  // though the previous version was safe-by-construction (statMap was a
  // hardcoded object), the pattern was a future-SQLi trap. (M1)
  const STAT_UPDATE_SQL = {
    campaign_generate: 'UPDATE user_xp SET total_xp = ?, level = ?, campaigns_generated = campaigns_generated + 1, updated_at = datetime("now") WHERE user_id = ?',
    task_complete:     'UPDATE user_xp SET total_xp = ?, level = ?, tasks_completed = tasks_completed + 1, updated_at = datetime("now") WHERE user_id = ?',
    email_generate:    'UPDATE user_xp SET total_xp = ?, level = ?, emails_generated = emails_generated + 1, updated_at = datetime("now") WHERE user_id = ?',
    research_run:      'UPDATE user_xp SET total_xp = ?, level = ?, research_runs = research_runs + 1, updated_at = datetime("now") WHERE user_id = ?',
    playlist_submit:   'UPDATE user_xp SET total_xp = ?, level = ?, playlists_submitted = playlists_submitted + 1, updated_at = datetime("now") WHERE user_id = ?',
    content_copy:      'UPDATE user_xp SET total_xp = ?, level = ?, content_copied = content_copied + 1, updated_at = datetime("now") WHERE user_id = ?',
    release_complete:  'UPDATE user_xp SET total_xp = ?, level = ?, releases_completed = releases_completed + 1, updated_at = datetime("now") WHERE user_id = ?'
  };
  const sql = STAT_UPDATE_SQL[action] || 'UPDATE user_xp SET total_xp = ?, level = ?, updated_at = datetime("now") WHERE user_id = ?';
  dbHelpers.prepare(sql).run(newTotal, newLevel, req.user.id);

  dbHelpers.prepare('INSERT INTO xp_log (user_id, action, xp_amount, description) VALUES (?, ?, ?, ?)').run(req.user.id, action, amount, action.replace(/_/g, ' '));

  // Check for new achievements
  const newAchievements = checkAndUnlockAchievements(req.user.id);

  // Re-fetch for accurate totals (achievements may have added XP)
  const final = dbHelpers.prepare('SELECT total_xp, level FROM user_xp WHERE user_id = ?').get(req.user.id);
  const finalLeveledUp = final.level > xp.level;
  flushDbNow(); // durable XP/achievement write — C2

  res.json({
    success: true,
    xpAwarded: amount,
    totalXp: final.total_xp,
    level: final.level,
    levelName: LEVEL_NAMES[final.level - 1] || 'Legend',
    leveledUp: finalLeveledUp,
    newLevel: finalLeveledUp ? final.level : null,
    newLevelName: finalLeveledUp ? (LEVEL_NAMES[final.level - 1] || 'Legend') : null,
    newAchievements
  });
});

// GET leaderboard — admin only. (H-2 fix: previously requireAuth, which let
// any logged-in trial user pull the top-50 customer email list. Emails are
// PII and a phishing target; restricted to admins.)
app.get('/api/gamification/leaderboard', requireAdmin, (req, res) => {
  const leaders = dbHelpers.prepare(`
    SELECT u.email, x.total_xp, x.level, x.current_streak, x.tasks_completed, x.campaigns_generated
    FROM user_xp x JOIN users u ON u.id = x.user_id
    ORDER BY x.total_xp DESC LIMIT 50
  `).all();
  res.json({ leaderboard: leaders });
});

// --- Claude API Proxy (keeps API key server-side) ---

// Allowlist of models the proxy will forward to. Anything else is rejected
// before we touch Anthropic. Keep this list in sync with the #claudeModel
// dropdown in MARKETING-COMMAND-CENTER.html (~line 1702) — any option the
// frontend offers MUST exist here or the user gets "Model not allowed".
const ALLOWED_CLAUDE_MODELS = new Set([
  // Current (Claude 4.6 family — matches frontend dropdown)
  'claude-sonnet-4-6',
  'claude-opus-4-6',
  'claude-haiku-4-5-20251001',
  // Legacy — keep for backward compatibility with any stored/cached requests
  'claude-sonnet-4-20250514',
  'claude-3-5-sonnet-20241022',
  'claude-3-5-haiku-20241022'
]);
const DEFAULT_CLAUDE_MODEL = 'claude-sonnet-4-6';

// Daily token caps per user role. Trial users get a small allowance to
// evaluate the product; paid users get a generous cap that still bounds
// catastrophic abuse (e.g. compromised credentials). Admins are unlimited.
const CLAUDE_DAILY_TOKEN_CAP = {
  trialing: 100000,   // ~30 generations
  active:   2000000,  // ~600 generations — bounds runaway clients
  admin:    Infinity
};

function todayUtc() { return new Date().toISOString().slice(0, 10); }

function getClaudeUsageToday(userId) {
  const row = dbHelpers.prepare(
    'SELECT input_tokens, output_tokens, requests FROM api_usage WHERE user_id = ? AND provider = ? AND day = ?'
  ).get(userId, 'claude', todayUtc());
  return row || { input_tokens: 0, output_tokens: 0, requests: 0 };
}

function recordClaudeUsage(userId, inputTokens, outputTokens) {
  dbHelpers.prepare(`
    INSERT INTO api_usage (user_id, provider, day, input_tokens, output_tokens, requests)
    VALUES (?, 'claude', ?, ?, ?, 1)
    ON CONFLICT(user_id, provider, day) DO UPDATE SET
      input_tokens = input_tokens + excluded.input_tokens,
      output_tokens = output_tokens + excluded.output_tokens,
      requests = requests + 1
  `).run(userId, todayUtc(), inputTokens || 0, outputTokens || 0);
}

function claudeQuotaForUser(user) {
  if (user.role === 'admin') return CLAUDE_DAILY_TOKEN_CAP.admin;
  if (user.subscription_status === 'active') return CLAUDE_DAILY_TOKEN_CAP.active;
  if (user.subscription_status === 'trialing') return CLAUDE_DAILY_TOKEN_CAP.trialing;
  return 0;
}

// Trial users are allowed here — blocking them broke first-release
// generation (campaign pipeline fires 5 parallel /api/claude calls).
// Abuse is bounded by: rlClaude (60/hr), CLAUDE_DAILY_TOKEN_CAP.trialing
// (100k/day), the 5-release client-side cap (TRIAL_RELEASE_LIMIT in HTML),
// and the strict request-shape validation below. Music Agent Pro stays
// blocked via the showPanel client-side gate. See R11 in task/lessons.md.
app.post('/api/claude', requireAccess, rlClaude, async (req, res) => {
  const CLAUDE_KEY = process.env.CLAUDE_API_KEY || '';
  if (!CLAUDE_KEY) return res.status(503).json({ error: 'AI features not configured' });

  // Validate request shape strictly. The previous version forwarded any
  // user-supplied system/messages/model — turning the endpoint into an
  // unmetered Anthropic proxy that any trial user could abuse.
  const { model, max_tokens, system, messages } = req.body || {};
  const chosenModel = model || DEFAULT_CLAUDE_MODEL;
  if (!ALLOWED_CLAUDE_MODELS.has(chosenModel)) {
    return res.status(400).json({ error: 'Model not allowed' });
  }
  if (!Array.isArray(messages) || messages.length === 0 || messages.length > 50) {
    return res.status(400).json({ error: 'messages must be a non-empty array (max 50)' });
  }
  // Coarse payload guard — Anthropic will reject anything ridiculous anyway,
  // but cheaper to bounce here before we burn tokens. 8MB matches the
  // express.json() 10MB outer cap with headroom; the upload scanner sends
  // base64-encoded cover art / screenshots which legitimately exceed any
  // text-only ceiling. Trial users can't reach this endpoint at all
  // (requireActive), and active users are bounded by daily token caps +
  // rlClaude (60/hr), so the larger ceiling is safe.
  const bodyBytes = JSON.stringify({ system, messages }).length;
  if (bodyBytes > 8 * 1024 * 1024) return res.status(413).json({ error: 'Request too large' });

  const cappedMaxTokens = Math.min(Math.max(parseInt(max_tokens, 10) || 1024, 1), 8192);

  // Quota check — block before we forward.
  const cap = claudeQuotaForUser(req.user);
  if (cap === 0) return res.status(403).json({ error: 'No active subscription' });
  if (cap !== Infinity) {
    const usage = getClaudeUsageToday(req.user.id);
    const used = (usage.input_tokens || 0) + (usage.output_tokens || 0);
    if (used >= cap) {
      return res.status(429).json({ error: 'Daily AI usage limit reached. Resets at 00:00 UTC.' });
    }
  }

  // Extend request timeout to 240s — Opus + 8k tokens can take 2+ minutes
  req.setTimeout(240000);
  res.setTimeout(240000);

  try {
    const aiResp = await fetch('https://api.anthropic.com/v1/messages', {
      method: 'POST',
      headers: {
        'x-api-key': CLAUDE_KEY,
        'anthropic-version': '2023-06-01',
        'content-type': 'application/json'
      },
      body: JSON.stringify({
        model: chosenModel,
        max_tokens: cappedMaxTokens,
        system: typeof system === 'string' ? system : '',
        messages
      }),
      signal: AbortSignal.timeout(220000)
    });
    if (!aiResp.ok) {
      const errText = await aiResp.text();
      console.error('Anthropic API error:', aiResp.status, errText);
      let detail = errText;
      try { detail = JSON.parse(errText).error?.message || errText; } catch(_) {}
      return res.status(aiResp.status).json({ error: 'AI error ' + aiResp.status + ': ' + detail });
    }
    const data = await aiResp.json();
    // Record usage from Anthropic's reported counts. Failed requests don't count.
    recordClaudeUsage(req.user.id, data.usage?.input_tokens, data.usage?.output_tokens);
    res.json(data);
  } catch(err) {
    console.error('AI proxy error:', err.message, err.stack);
    res.status(500).json({ error: 'AI request failed: ' + err.message });
  }
});

// --- Main App (protected — allows expired trial to see upgrade prompt) ---
// Cache the 408 KB HTML template at boot. Previously this read the file
// synchronously on EVERY request, blocking the event loop. (H6)
const APP_HTML_TEMPLATE = fs.readFileSync(path.join(__dirname, 'MARKETING-COMMAND-CENTER.html'), 'utf8');

// Email is interpolated into the HTML template — escape it to prevent any
// signup-time HTML injection from rendering as live markup.
function escHtml(s) {
  return String(s == null ? '' : s)
    .replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;')
    .replace(/"/g, '&quot;').replace(/'/g, '&#39;');
}

// Public landing page for unauthenticated visitors (Stripe verification, SEO).
// Logged-in users skip straight to the app.
app.get('/', (req, res, next) => {
  if (!req.session.userId) return res.sendFile(path.join(__dirname, 'landing.html'));
  next();
}, requireAuth, (req, res) => {
  let trialDays = -1;
  if (req.user.role !== 'admin' && req.user.subscription_status === 'trialing' && req.user.trial_ends_at) {
    trialDays = Math.max(0, Math.ceil((new Date(req.user.trial_ends_at) - new Date()) / (1000 * 60 * 60 * 24)));
  }
  const isImpersonating = !!(req.session.impersonating && req.session.realAdminId);
  const html = APP_HTML_TEMPLATE
    .replace('%%CLAUDE_API_KEY%%', '') // API key no longer sent to client
    .replace('%%USER_ID%%', String(req.user.id))
    .replace('%%USER_ROLE%%', escHtml(req.user.role || 'user'))
    .replace('%%TRIAL_DAYS%%', String(trialDays))
    .replace('%%SUB_STATUS%%', escHtml(req.user.subscription_status || 'none'))
    .replace('%%USER_EMAIL%%', escHtml(req.user.email || ''))
    .replace('%%USER_TIER%%', escHtml(req.user.subscription_tier || 'pro'))
    .replace('%%ONBOARDING_COMPLETED%%', req.user.onboarding_completed ? '1' : '0')
    .replace('%%IMPERSONATING%%', isImpersonating ? '1' : '0');
  res.set('Cache-Control', 'no-store, no-cache, must-revalidate');
  res.set('Pragma', 'no-cache');
  res.type('html').send(html);
});

// --- Public Submission Form Routes (no auth) ---
const rlSubmission = rateLimit({ name: 'submission', windowMs: 60 * 60 * 1000, max: 10 });

app.get('/music-submission', (req, res) => {
  res.sendFile(path.join(__dirname, 'music-submission.html'));
});
app.get('/playlist-submission', (req, res) => {
  res.sendFile(path.join(__dirname, 'playlist-submission.html'));
});

// Store uploaded submission files in public/uploads
const UPLOADS_DIR = path.join(__dirname, 'public', 'uploads');
if (!fs.existsSync(UPLOADS_DIR)) fs.mkdirSync(UPLOADS_DIR, { recursive: true });

// --- Release Image Upload ---
// Upload cover art or artist photo as base64, returns a permanent URL.
// Stored in public/uploads/release/ so they're served as static files.
const RELEASE_IMG_DIR = path.join(UPLOADS_DIR, 'release');
if (!fs.existsSync(RELEASE_IMG_DIR)) fs.mkdirSync(RELEASE_IMG_DIR, { recursive: true });

app.post('/api/release-image', requireAuth, (req, res) => {
  const { imageData, type } = req.body; // type: 'cover' or 'artist'
  if (!imageData || !type) return res.status(400).json({ error: 'imageData and type required' });
  if (!['cover', 'artist'].includes(type)) return res.status(400).json({ error: 'type must be cover or artist' });

  // imageData is a data:image/... base64 string
  const match = imageData.match(/^data:image\/(png|jpe?g|webp|gif);base64,(.+)$/i);
  if (!match) return res.status(400).json({ error: 'Invalid image data — must be base64 data URI' });

  const ext = match[1].replace('jpeg', 'jpg');
  const buf = Buffer.from(match[2], 'base64');
  if (buf.length > 10 * 1024 * 1024) return res.status(413).json({ error: 'Image too large (max 10MB)' });

  const fname = `${type}_${req.user.id}_${Date.now()}_${crypto.randomBytes(4).toString('hex')}.${ext}`;
  fs.writeFileSync(path.join(RELEASE_IMG_DIR, fname), buf);

  const url = `/uploads/release/${fname}`;
  res.json({ ok: true, url });
});

app.post('/api/music-submission', rlSubmission, (req, res) => {
  const b = req.body || {};
  const artistName = (b.artist_name || '').trim().slice(0, 200);
  const trackName = (b.track_name || '').trim().slice(0, 200);
  const email = (b.email || '').trim().slice(0, 300);
  const spotifyLink = (b.spotify_link || '').trim().slice(0, 500);
  if (!artistName || !trackName || !email || !spotifyLink) {
    return res.status(400).json({ error: 'Artist name, track name, email, and Spotify link are required.' });
  }

  let pressReleaseFile = null;
  let profileImageFile = null;

  // Handle base64 file uploads
  if (b.press_release_data && b.press_release_name) {
    const ext = path.extname(b.press_release_name).slice(0, 10);
    const fname = `pr_${Date.now()}_${crypto.randomBytes(4).toString('hex')}${ext}`;
    const buf = Buffer.from(b.press_release_data, 'base64');
    if (buf.length <= 15 * 1024 * 1024) {
      fs.writeFileSync(path.join(UPLOADS_DIR, fname), buf);
      pressReleaseFile = fname;
    }
  }
  if (b.profile_image_data && b.profile_image_name) {
    const ext = path.extname(b.profile_image_name).slice(0, 10);
    const fname = `img_${Date.now()}_${crypto.randomBytes(4).toString('hex')}${ext}`;
    const buf = Buffer.from(b.profile_image_data, 'base64');
    if (buf.length <= 15 * 1024 * 1024) {
      fs.writeFileSync(path.join(UPLOADS_DIR, fname), buf);
      profileImageFile = fname;
    }
  }

  dbHelpers.prepare(`
    INSERT INTO music_submissions (artist_name, track_name, email, spotify_link,
      facebook_link, instagram_link, twitter_link, marketing_interest,
      interview_interest, press_release_file, profile_image_file)
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
  `).run(
    artistName, trackName, email, spotifyLink,
    (b.facebook_link || '').trim().slice(0, 500),
    (b.instagram_link || '').trim().slice(0, 500),
    (b.twitter_link || '').trim().slice(0, 500),
    (b.marketing_interest || '').trim().slice(0, 100),
    (b.interview_interest || '').trim().slice(0, 100),
    pressReleaseFile, profileImageFile
  );
  res.json({ ok: true });
});

app.post('/api/playlist-submission', rlSubmission, (req, res) => {
  const b = req.body || {};
  const artistName = (b.artist_name || '').trim().slice(0, 200);
  const trackName = (b.track_name || '').trim().slice(0, 200);
  const email = (b.email || '').trim().slice(0, 300);
  const spotifyLink = (b.spotify_link || '').trim().slice(0, 500);
  if (!artistName || !trackName || !email || !spotifyLink) {
    return res.status(400).json({ error: 'Artist name, track name, email, and Spotify link are required.' });
  }
  dbHelpers.prepare(`
    INSERT INTO playlist_submissions (artist_name, track_name, email, spotify_link,
      playlist_selection, marketing_interest, challenges, consent)
    VALUES (?, ?, ?, ?, ?, ?, ?, ?)
  `).run(
    artistName, trackName, email, spotifyLink,
    (b.playlist_selection || '').trim().slice(0, 200),
    (b.marketing_interest || '').trim().slice(0, 100),
    (b.challenges || '').trim().slice(0, 1000),
    b.consent ? 1 : 0
  );
  res.json({ ok: true });
});

// Admin: view submissions
app.get('/api/admin/music-submissions', requireAdmin, (req, res) => {
  const rows = dbHelpers.prepare('SELECT * FROM music_submissions ORDER BY created_at DESC').all();
  res.json(rows);
});
app.get('/api/admin/playlist-submissions', requireAdmin, (req, res) => {
  const rows = dbHelpers.prepare('SELECT * FROM playlist_submissions ORDER BY created_at DESC').all();
  res.json(rows);
});

// --- Registration Prep System ---
// Slack incoming webhook — admin notifications. Set SLACK_WEBHOOK_URL in env.
const SLACK_WEBHOOK_URL = process.env.SLACK_WEBHOOK_URL || '';
// Slack Bot — DMs to Elite users. Set SLACK_BOT_TOKEN + SLACK_SIGNING_SECRET in env.
const SLACK_BOT_TOKEN = process.env.SLACK_BOT_TOKEN || '';
const SLACK_SIGNING_SECRET = process.env.SLACK_SIGNING_SECRET || '';

async function notifySlack(text, blocks) {
  if (!SLACK_WEBHOOK_URL) return;
  try {
    const payload = blocks ? { text, blocks } : { text };
    await fetch(SLACK_WEBHOOK_URL, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(payload),
    });
  } catch (e) {
    console.error('[SLACK] notification failed:', e.message);
  }
}

// Slack Bot API helper
async function slackApi(method, body) {
  if (!SLACK_BOT_TOKEN) return null;
  try {
    const resp = await fetch('https://slack.com/api/' + method, {
      method: 'POST',
      headers: { 'Authorization': 'Bearer ' + SLACK_BOT_TOKEN, 'Content-Type': 'application/json' },
      body: JSON.stringify(body),
    });
    const data = await resp.json();
    if (!data.ok) console.error('[SLACK-BOT] ' + method + ' error:', data.error);
    return data;
  } catch (e) {
    console.error('[SLACK-BOT] ' + method + ' failed:', e.message);
    return null;
  }
}

// Slack workspace invite link — set in env or use a default placeholder
const SLACK_INVITE_URL = process.env.SLACK_INVITE_URL || '';

// Check if current user is in the Slack workspace
app.get('/api/slack/check', requireAuth, async (req, res) => {
  const tier = req.user.subscription_tier || '';
  const isElite = ['elite', 'elite_plus'].includes(tier);
  if (!isElite) return res.json({ required: false, inSlack: false });
  if (!SLACK_BOT_TOKEN) return res.json({ required: true, inSlack: false, inviteUrl: SLACK_INVITE_URL, noBot: true });

  const slackUserId = await resolveSlackUserId(req.user.id, req.user.email);
  res.json({
    required: true,
    inSlack: !!slackUserId,
    inviteUrl: SLACK_INVITE_URL,
  });
});

// Look up Slack user ID by email — caches to users.slack_user_id
async function resolveSlackUserId(userId, email) {
  const user = dbHelpers.prepare('SELECT slack_user_id FROM users WHERE id = ?').get(userId);
  if (user && user.slack_user_id) return user.slack_user_id;
  const result = await slackApi('users.lookupByEmail', { email });
  if (result && result.ok && result.user) {
    dbHelpers.prepare('UPDATE users SET slack_user_id = ? WHERE id = ?').run(result.user.id, userId);
    return result.user.id;
  }
  return null;
}

// Send a DM to a Slack user
async function sendSlackDM(slackUserId, text, blocks) {
  // Open a DM channel
  const conv = await slackApi('conversations.open', { users: slackUserId });
  if (!conv || !conv.ok) return null;
  const channelId = conv.channel.id;
  const payload = { channel: channelId, text };
  if (blocks) payload.blocks = blocks;
  return slackApi('chat.postMessage', payload);
}

// Verify Slack request signature (for interactive endpoints)
function verifySlackSignature(req) {
  if (!SLACK_SIGNING_SECRET) return false;
  const crypto = require('crypto');
  const timestamp = req.headers['x-slack-request-timestamp'];
  const sig = req.headers['x-slack-signature'];
  if (!timestamp || !sig) return false;
  // Reject requests older than 5 minutes
  if (Math.abs(Date.now() / 1000 - parseInt(timestamp)) > 300) return false;
  const sigBaseString = 'v0:' + timestamp + ':' + req.rawBody;
  const mySignature = 'v0=' + crypto.createHmac('sha256', SLACK_SIGNING_SECRET).update(sigBaseString).digest('hex');
  return crypto.timingSafeEqual(Buffer.from(mySignature), Buffer.from(sig));
}

// Build Block Kit form for missing BMI fields
// Build one consolidated Slack DM with deduped fields across all platforms.
// allMissing = { pro: ['Work Title', 'ISRC'], musixmatch: ['ISRC', 'Lyrics'], ... }
function buildConsolidatedMissingBlocks(releaseTitle, artistName, allMissing, userId) {
  // Dedupe: map field key → { label, key, isArray, platforms[] }
  const fieldMap = new Map();
  const platformNames = [];

  for (const [platform, missingLabels] of Object.entries(allMissing)) {
    const def = PLATFORM_DEFS[platform];
    if (!def) continue;
    platformNames.push(def.shortName);
    const allFields = [...def.required, ...(def.optional || [])];
    for (const label of missingLabels) {
      const fieldDef = allFields.find(f => f.label === label);
      if (!fieldDef) continue;
      const existing = fieldMap.get(fieldDef.key);
      if (existing) {
        if (!existing.platforms.includes(def.shortName)) existing.platforms.push(def.shortName);
      } else {
        fieldMap.set(fieldDef.key, { key: fieldDef.key, label, isArray: fieldDef.isArray, platforms: [def.shortName] });
      }
    }
  }

  const platformList = [...new Set(platformNames)].join(', ');
  const platforms = Object.keys(allMissing);

  const blocks = [
    {
      type: 'header',
      text: { type: 'plain_text', text: ':musical_note: Missing info for registration' }
    },
    {
      type: 'section',
      text: { type: 'mrkdwn', text: `*${releaseTitle}*${artistName ? ' by ' + artistName : ''}\n\nWe need a few more details to register this across *${platformList}*. Each field shows which platforms need it — fill in what you can:` }
    },
    { type: 'divider' },
  ];

  const placeholders = {
    songTitle: 'Enter the song title',
    primaryArtist: 'Artist name as it appears on streaming',
    isrc: 'e.g., USXX12345678',
    upc: 'e.g., 012345678901',
    iswc: 'e.g., T-012.345.678-1',
    releaseDate: 'e.g., 2026-05-01',
    genrePrimary: 'e.g., Hip-Hop, R&B, Pop',
    genreSecondary: 'e.g., Soul, Trap, Lo-fi',
    lyrics: 'Paste full lyrics here',
    albumName: 'Album or single name',
    label: 'Label name (or Independent)',
    language: 'e.g., English, Spanish',
    featArtist: 'Featured artist name',
    publisherEntity: 'Your publishing company name',
    explicit: 'Yes or No',
    durationMin: 'Minutes (e.g., 3)',
    durationSec: 'Seconds (e.g., 42)',
    workContent: 'e.g., Lyrics, Instrumental',
    proAffiliation: 'e.g., BMI, ASCAP',
  };

  // Determine songwriter detail level — use the most detailed version needed
  const needsSongwriter = fieldMap.has('songwriters');
  const needsIpi = needsSongwriter && platforms.some(p => ['pro', 'mlc', 'songtrust'].includes(p));
  const needsPro = needsSongwriter && platforms.some(p => ['pro', 'songtrust'].includes(p));

  const inputBlocks = [];
  const added = new Set();

  for (const [key, field] of fieldMap) {
    if (added.has(key)) continue;
    added.add(key);

    const hint = field.platforms.length < platformNames.length ? ' (for ' + field.platforms.join(', ') + ')' : '';

    // Songwriter/writer array — consolidated with max detail
    if (field.isArray && key === 'songwriters') {
      inputBlocks.push({
        type: 'input',
        block_id: 'field_songwriter_name',
        element: {
          type: 'plain_text_input',
          action_id: 'songwriter_name',
          placeholder: { type: 'plain_text', text: 'Legal name of songwriter' },
        },
        label: { type: 'plain_text', text: 'Songwriter Legal Name' + hint },
        optional: true,
      });
      if (needsIpi) {
        inputBlocks.push({
          type: 'input',
          block_id: 'field_songwriter_ipi',
          element: {
            type: 'plain_text_input',
            action_id: 'songwriter_ipi',
            placeholder: { type: 'plain_text', text: 'e.g., 00123456789' },
          },
          label: { type: 'plain_text', text: 'Songwriter IPI Number' },
          optional: true,
        });
      }
      if (needsPro) {
        inputBlocks.push({
          type: 'input',
          block_id: 'field_songwriter_pro',
          element: {
            type: 'static_select',
            action_id: 'songwriter_pro',
            placeholder: { type: 'plain_text', text: 'Select PRO' },
            options: ['BMI', 'ASCAP', 'SESAC', 'GMR', 'SOCAN', 'PRS', 'Other'].map(p => ({
              text: { type: 'plain_text', text: p }, value: p
            })),
          },
          label: { type: 'plain_text', text: 'Songwriter PRO Affiliation' },
          optional: true,
        });
      }
      continue;
    }

    // Publisher array
    if (field.isArray && key === 'publishers') {
      inputBlocks.push({
        type: 'input',
        block_id: 'field_publisher_name',
        element: {
          type: 'plain_text_input',
          action_id: 'publisher_name',
          placeholder: { type: 'plain_text', text: 'Publishing company name' },
        },
        label: { type: 'plain_text', text: 'Publisher Name' + hint },
        optional: true,
      });
      inputBlocks.push({
        type: 'input',
        block_id: 'field_publisher_ipi',
        element: {
          type: 'plain_text_input',
          action_id: 'publisher_ipi',
          placeholder: { type: 'plain_text', text: 'Publisher IPI number' },
        },
        label: { type: 'plain_text', text: 'Publisher IPI Number' },
        optional: true,
      });
      continue;
    }

    // Lyrics — multiline
    if (key === 'lyrics') {
      inputBlocks.push({
        type: 'input',
        block_id: 'field_lyrics',
        element: {
          type: 'plain_text_input',
          action_id: 'lyrics',
          multiline: true,
          placeholder: { type: 'plain_text', text: 'Paste full lyrics here' },
        },
        label: { type: 'plain_text', text: 'Lyrics' + hint },
        optional: true,
      });
      continue;
    }

    // Explicit — select
    if (key === 'explicit') {
      inputBlocks.push({
        type: 'input',
        block_id: 'field_explicit',
        element: {
          type: 'static_select',
          action_id: 'explicit',
          placeholder: { type: 'plain_text', text: 'Select' },
          options: ['Yes', 'No'].map(v => ({ text: { type: 'plain_text', text: v }, value: v })),
        },
        label: { type: 'plain_text', text: 'Explicit Content' + hint },
        optional: true,
      });
      continue;
    }

    // Standard text input
    inputBlocks.push({
      type: 'input',
      block_id: 'field_' + key,
      element: {
        type: 'plain_text_input',
        action_id: key,
        placeholder: { type: 'plain_text', text: placeholders[key] || 'Enter ' + field.label.toLowerCase() },
      },
      label: { type: 'plain_text', text: field.label + hint },
      optional: true,
    });
  }

  blocks.push(...inputBlocks);

  // Submit button — encode all platforms in the value
  blocks.push({ type: 'divider' });
  blocks.push({
    type: 'actions',
    block_id: 'submit_missing_fields',
    elements: [{
      type: 'button',
      text: { type: 'plain_text', text: ':white_check_mark: Submit Info' },
      style: 'primary',
      action_id: 'submit_registration_fields',
      value: JSON.stringify({ userId, releaseTitle, platforms }),
    }]
  });

  return blocks;
}

// Send one consolidated Slack DM to Elite user for all platforms with missing fields.
// allMissing = { pro: ['Work Title', 'ISRC'], musixmatch: ['Lyrics', 'ISRC'], ... }
async function askEliteUserForMissingFields(userId, releaseTitle, release, allMissing) {
  if (!SLACK_BOT_TOKEN) return false;
  if (!allMissing || Object.keys(allMissing).length === 0) return false;
  const user = dbHelpers.prepare('SELECT email, subscription_tier FROM users WHERE id = ? AND deleted_at IS NULL').get(userId);
  if (!user) return false;
  if (!['elite', 'elite_plus'].includes(user.subscription_tier)) return false;

  const slackUserId = await resolveSlackUserId(userId, user.email);
  if (!slackUserId) {
    console.log('[SLACK-BOT] Could not find Slack user for ' + user.email);
    return false;
  }

  const platformNames = Object.keys(allMissing).map(p => PLATFORM_DEFS[p]?.shortName || p).join(', ');
  const blocks = buildConsolidatedMissingBlocks(releaseTitle, release.primaryArtist, allMissing, userId);
  const result = await sendSlackDM(slackUserId, 'Missing info for registration (' + platformNames + '): ' + releaseTitle, blocks);

  if (result && result.ok) {
    // Mark all platforms as notified
    for (const platform of Object.keys(allMissing)) {
      dbHelpers.prepare(`
        UPDATE registration_queue SET slack_notified = 1 WHERE user_id = ? AND release_title = ? AND platform = ?
      `).run(userId, releaseTitle, platform);
    }
    logOperation(null, 'slack.missing_fields_asked', 'user', userId, { release: releaseTitle, platforms: Object.keys(allMissing), allMissing });
    return true;
  }
  return false;
}

// BMI field mapping — which intake fields map to which BMI registration fields
// --- Platform Registration Definitions ---
// Each platform defines required + optional fields, steps, and URLs.
// The same field keys match the intake form (release_data_all values).
const PLATFORM_DEFS = {
  pro: {
    name: 'PRO (BMI or ASCAP)',
    shortName: 'PRO',
    cost: 'Free',
    url: 'https://www.bmi.com/login',
    required: [
      { key: 'songTitle', label: 'Work Title' },
      { key: 'primaryArtist', label: 'Performing Artist' },
      { key: 'songwriters', label: 'Songwriters + IPI + PRO', isArray: true },
      { key: 'isrc', label: 'ISRC' },
    ],
    optional: [
      { key: 'featArtist', label: 'Featured Artist' },
      { key: 'durationMin', label: 'Duration (min)' },
      { key: 'durationSec', label: 'Duration (sec)' },
      { key: 'language', label: 'Language' },
      { key: 'workContent', label: 'Work Content Type' },
      { key: 'genrePrimary', label: 'Genre' },
      { key: 'publisherEntity', label: 'Publisher Entity' },
      { key: 'publishers', label: 'Publishers + IPI', isArray: true },
      { key: 'proAffiliation', label: 'PRO Affiliation' },
      { key: 'upc', label: 'UPC' },
      { key: 'iswc', label: 'ISWC' },
    ],
  },
  distribution: {
    name: 'Distribution',
    shortName: 'Distro',
    cost: 'Varies',
    url: 'https://distrokid.com/dashboard',
    required: [
      { key: 'songTitle', label: 'Song Title' },
      { key: 'primaryArtist', label: 'Artist Name' },
      { key: 'genrePrimary', label: 'Genre' },
      { key: 'releaseDate', label: 'Release Date' },
    ],
    optional: [
      { key: 'featArtist', label: 'Featured Artist' },
      { key: 'albumName', label: 'Album / Single Name' },
      { key: 'label', label: 'Label' },
      { key: 'songwriters', label: 'Songwriters', isArray: true },
      { key: 'isrc', label: 'ISRC' },
      { key: 'upc', label: 'UPC' },
      { key: 'language', label: 'Language' },
      { key: 'genreSecondary', label: 'Secondary Genre' },
      { key: 'explicit', label: 'Explicit' },
    ],
  },
  musixmatch: {
    name: 'Musixmatch (Timed Lyrics)',
    shortName: 'Musixmatch',
    cost: 'Free',
    url: 'https://artists.musixmatch.com',
    required: [
      { key: 'songTitle', label: 'Song Title' },
      { key: 'primaryArtist', label: 'Primary Artist' },
      { key: 'isrc', label: 'ISRC' },
      { key: 'lyrics', label: 'Lyrics' },
    ],
    optional: [
      { key: 'featArtist', label: 'Featured Artist' },
    ],
  },
  soundexchange: {
    name: 'SoundExchange',
    shortName: 'SoundEx',
    cost: 'Free',
    url: 'https://www.soundexchange.com/',
    required: [
      { key: 'primaryArtist', label: 'Artist' },
      { key: 'songTitle', label: 'Recording Title' },
      { key: 'isrc', label: 'ISRC' },
      { key: 'claimBasis', label: 'Basis of Claim' },
      { key: 'percentageClaimed', label: 'Percentage Claimed' },
      { key: 'rightsBegin', label: 'Rights Begin Date' },
      { key: 'rightsEnd', label: 'Rights End Date' },
    ],
    optional: [
      { key: 'nonUsTerritories', label: 'Non-US Territories' },
      { key: 'version', label: 'Recording Version' },
      { key: 'duration', label: 'Duration (HH:MM:SS)' },
      { key: 'genre', label: 'Genre' },
      { key: 'recordingDate', label: 'Recording Date' },
      { key: 'countryOfRecording', label: 'Country of Recording' },
      { key: 'countryOfMastering', label: 'Country of Mastering' },
      { key: 'copyrightOwnerNationality', label: 'Copyright Owner Nationality' },
      { key: 'dateOfFirstRelease', label: 'Date of First Release' },
      { key: 'countriesOfFirstRelease', label: 'Countries of First Release' },
      { key: 'pLine', label: '(P) Line' },
      { key: 'iswc', label: 'ISWC' },
      { key: 'composers', label: 'Composer(s)' },
      { key: 'publishers', label: 'Publisher(s)' },
      { key: 'releaseArtist', label: 'Release Artist' },
      { key: 'albumName', label: 'Release Title (Album)' },
      { key: 'releaseVersion', label: 'Release Version' },
      { key: 'upc', label: 'UPC' },
      { key: 'catalogNumber', label: 'Catalog #' },
      { key: 'releaseDate', label: 'Release Date' },
      { key: 'countryOfRelease', label: 'Country of Release' },
      { key: 'label', label: 'Release Label' },
    ],
  },
  mlc: {
    name: 'The MLC',
    shortName: 'MLC',
    cost: 'Free',
    phases: ['pre-release', 'post-release'], // MLC has two phases
    url: 'https://portal.themlc.com',
    required: [
      { key: 'songTitle', label: 'Song Title' },
      { key: 'songwriters', label: 'Writers + IPI', isArray: true },
    ],
    optional: [
      { key: 'publisherEntity', label: 'Publisher Entity' },
      { key: 'publishers', label: 'Publishers + IPI', isArray: true },
      { key: 'iswc', label: 'ISWC' },
      { key: 'isrc', label: 'ISRC (post-release)' },
    ],
  },
  songtrust: {
    name: 'Songtrust',
    shortName: 'Songtrust',
    cost: '$100 + 15%',
    url: 'https://www.songtrust.com',
    required: [
      { key: 'songTitle', label: 'Song Title' },
      { key: 'songwriters', label: 'Writers + IPI + PRO', isArray: true },
      { key: 'isrc', label: 'ISRC' },
    ],
    optional: [
      { key: 'publisherEntity', label: 'Publisher' },
      { key: 'publishers', label: 'Publishers + IPI', isArray: true },
      { key: 'releaseDate', label: 'Release Date' },
    ],
  },
};
const ALL_PLATFORMS = Object.keys(PLATFORM_DEFS);

// Universal completeness check — works for any platform
function checkPlatformCompleteness(release, platform) {
  const def = PLATFORM_DEFS[platform];
  if (!def || !release) return { complete: false, pct: 0, missing: (def ? def.required.map(f => f.label) : []), present: [] };
  const missing = [];
  const present = [];
  for (const f of def.required) {
    const val = release[f.key];
    if (f.isArray) {
      if (Array.isArray(val) && val.length > 0 && val.some(v => v.name || (typeof v === 'string' && v.trim()))) present.push(f.label);
      else missing.push(f.label);
    } else {
      if (val && String(val).trim()) present.push(f.label);
      else missing.push(f.label);
    }
  }
  let optPresent = 0;
  for (const f of (def.optional || [])) {
    const val = release[f.key];
    if (f.isArray ? (Array.isArray(val) && val.length > 0) : (val && String(val).trim())) optPresent++;
  }
  const total = def.required.length + (def.optional || []).length;
  const filled = present.length + optPresent;
  return { complete: missing.length === 0, pct: Math.round((filled / total) * 100), missing, present };
}

// Backward-compat alias — still used in some places
function checkBmiCompleteness(release) {
  return checkPlatformCompleteness(release, 'pro');
}

// Universal data sheet builder — maps intake fields to platform-specific display
function buildPlatformSheet(release, user, platform) {
  if (!release) return null;
  const sw = (release.songwriters || []).filter(s => s && s.name);
  const pubs = (release.publishers || []).filter(p => p && p.name);
  const dur = (release.durationMin || '0') + ':' + String(release.durationSec || '0').padStart(2, '0');
  const def = PLATFORM_DEFS[platform];

  const sheet = {
    _platform: platform,
    _platformName: def ? def.name : platform,
    _url: def ? def.url : '',
    songTitle: release.songTitle || '',
    primaryArtist: release.primaryArtist || '',
    featArtist: release.featArtist || '',
    isrc: release.isrc || '',
    upc: release.upc || '',
    iswc: release.iswc || '',
    releaseDate: release.releaseDate || '',
    genre: release.genrePrimary || '',
    genreSecondary: release.genreSecondary || '',
    language: release.language || 'English',
    duration: dur,
    workContent: release.workContent || 'Music and Lyrics',
    albumName: release.albumName || '',
    label: release.label || '',
    explicit: release.explicit || '',
    lyrics: release.lyrics ? '(present — ' + release.lyrics.length + ' chars)' : '',
    lyricsRaw: release.lyrics || '',
    proAffiliation: release.proAffiliation || '',
    publisherEntity: release.publisherEntity || '',
    hasPublisher: pubs.length > 0 || !!(release.publisherEntity),
    songwriters: sw.map(s => ({
      name: s.name, role: s.role || 'Both', ipi: s.ipi || '',
      pro: s.pro || release.proAffiliation || '', split: s.split || '',
    })),
    publishers: pubs.map(p => ({ name: p.name, ipi: p.ipi || '', split: p.split || '' })),
    filmTvTheater: 'No',
    userEmail: user ? user.email : '',
    // SoundExchange ISRC Ingest Form fields
    claimBasis: 'Copyright Owner',
    percentageClaimed: '100',
    rightsBegin: release.releaseDate || '',
    rightsEnd: '',  // blank = perpetuity
    nonUsTerritories: '',
    version: release.version || '',
    recordingDate: release.recordingDate || release.releaseDate || '',
    countryOfRecording: release.countryOfRecording || 'US',
    countryOfMastering: release.countryOfMastering || 'US',
    copyrightOwnerNationality: 'US',
    dateOfFirstRelease: release.releaseDate || '',
    countriesOfFirstRelease: 'US',
    pLine: release.pLine || (release.label ? ('℗ ' + new Date().getFullYear() + ' ' + release.label) : ''),
    composers: sw.map(s => s.name).join(', '),
    releaseArtist: release.primaryArtist || '',
    releaseVersion: '',
    catalogNumber: release.catalogNumber || '',
    countryOfRelease: 'US',
  };
  return sheet;
}

// Backward-compat alias
function buildBmiSheet(release, user) {
  return buildPlatformSheet(release, user, 'pro');
}

// List all pending registrations across all users with release data
app.get('/api/admin/registration/pending', requireAdmin, (req, res) => {
  // Get all users who have release_data_all saved
  const rows = dbHelpers.prepare(`
    SELECT ud.user_id, ud.value, u.email, u.subscription_tier, u.subscription_status
    FROM user_data ud
    JOIN users u ON u.id = ud.user_id AND u.deleted_at IS NULL
    WHERE ud.key = 'release_data_all'
  `).all();

  // Get existing queue entries
  const queueRows = dbHelpers.prepare('SELECT * FROM registration_queue ORDER BY created_at DESC').all();
  const queueMap = {};
  for (const q of queueRows) {
    const k = q.user_id + '::' + q.release_title + '::' + q.platform;
    queueMap[k] = q;
  }

  const results = [];
  for (const row of rows) {
    let releases;
    try { releases = JSON.parse(row.value); } catch (e) { continue; }
    if (!releases || typeof releases !== 'object') continue;

    for (const [title, release] of Object.entries(releases)) {
      // Build per-platform status for this release
      const platforms = {};
      for (const platform of ALL_PLATFORMS) {
        const completeness = checkPlatformCompleteness(release, platform);
        const queueKey = row.user_id + '::' + title + '::' + platform;
        const queueEntry = queueMap[queueKey];
        platforms[platform] = {
          name: PLATFORM_DEFS[platform].shortName,
          status: queueEntry ? queueEntry.status : 'new',
          completeness,
          confirmationNumber: queueEntry ? queueEntry.confirmation_number : null,
          queueId: queueEntry ? queueEntry.id : null,
        };
      }
      // Overall completeness = average across platforms
      const avgPct = Math.round(ALL_PLATFORMS.reduce((sum, p) => sum + platforms[p].completeness.pct, 0) / ALL_PLATFORMS.length);
      const confirmedCount = ALL_PLATFORMS.filter(p => platforms[p].status === 'confirmed').length;

      results.push({
        userId: row.user_id,
        userEmail: row.email,
        tier: row.subscription_tier,
        releaseTitle: title,
        platforms,
        avgPct,
        confirmedCount,
        totalPlatforms: ALL_PLATFORMS.length,
      });
    }
  }

  // Sort: fewest confirmed first, then by avg completeness desc
  results.sort((a, b) => {
    if (a.confirmedCount !== b.confirmedCount) return a.confirmedCount - b.confirmedCount;
    return b.avgPct - a.avgPct;
  });

  res.json({ registrations: results, total: results.length, platformDefs: PLATFORM_DEFS });
});

// Get BMI-ready data sheet for a specific user + release
// Platform param is optional — defaults to 'pro' for backward compat
app.get('/api/admin/registration/:userId/sheet/:releaseTitle/:platform?', requireAdmin, (req, res) => {
  const userId = parseInt(req.params.userId, 10);
  if (!Number.isInteger(userId) || userId <= 0) return res.status(400).json({ error: 'Invalid user id' });
  const releaseTitle = decodeURIComponent(req.params.releaseTitle);
  const platform = req.params.platform || 'pro';
  if (!PLATFORM_DEFS[platform]) return res.status(400).json({ error: 'Unknown platform: ' + platform });

  const user = dbHelpers.prepare('SELECT * FROM users WHERE id = ? AND deleted_at IS NULL').get(userId);
  if (!user) return res.status(404).json({ error: 'User not found' });

  const row = dbHelpers.prepare('SELECT value FROM user_data WHERE user_id = ? AND key = ?').get(userId, 'release_data_all');
  if (!row) return res.status(404).json({ error: 'No release data' });

  let releases;
  try { releases = JSON.parse(row.value); } catch (e) { return res.status(500).json({ error: 'Corrupt release data' }); }
  const release = releases[releaseTitle];
  if (!release) return res.status(404).json({ error: 'Release not found' });

  const sheet = buildPlatformSheet(release, user, platform);
  const completeness = checkPlatformCompleteness(release, platform);
  const def = PLATFORM_DEFS[platform];

  logOperation(req, 'admin.registration_sheet_viewed', 'user', userId, { release: releaseTitle, platform });

  res.json({ sheet, completeness, releaseTitle, userId, userEmail: user.email, platform, platformDef: def });
});
// Backward compat alias
app.get('/api/admin/registration/:userId/bmi-sheet/:releaseTitle', requireAdmin, (req, res) => {
  req.params.platform = 'pro';
  res.redirect(307, `/api/admin/registration/${req.params.userId}/sheet/${req.params.releaseTitle}/pro`);
});

// Bulk SoundExchange ISRC Ingest Form XLSX export
app.get('/api/admin/registration/soundexchange-xlsx', requireAdmin, async (req, res) => {
  try {
    const rows = dbHelpers.prepare(`
      SELECT ud.user_id, ud.value, u.email
      FROM user_data ud
      JOIN users u ON u.id = ud.user_id AND u.deleted_at IS NULL
      WHERE ud.key = 'release_data_all'
    `).all();

    // Collect all releases that have campaigns generated
    const allSheets = [];
    for (const row of rows) {
      let releases;
      try { releases = JSON.parse(row.value); } catch (e) { continue; }
      if (!releases || typeof releases !== 'object') continue;
      for (const [title, release] of Object.entries(releases)) {
        if (!release._campaignGenerated) continue;
        // Skip if already confirmed on SoundExchange
        const qKey = row.user_id + '::' + title + '::soundexchange';
        const qRow = dbHelpers.prepare('SELECT status FROM registration_queue WHERE user_id = ? AND release_title = ? AND platform = ?').get(row.user_id, title, 'soundexchange');
        if (qRow && qRow.status === 'confirmed') continue;
        const sheet = buildPlatformSheet(release, { email: row.email }, 'soundexchange');
        sheet._userEmail = row.email;
        sheet._releaseTitle = title;
        allSheets.push(sheet);
      }
    }

    // Build XLSX matching SoundExchange ISRC Ingest Form format
    const ExcelJS = require('exceljs');
    const wb = new ExcelJS.Workbook();
    const ws = wb.addWorksheet('Form');

    // Header rows matching official form
    ws.getCell('B1').value = 'ISRC Ingest File';
    ws.getCell('B2').value = new Date().toLocaleDateString('en-US');

    // Column headers (row 6 in official form, we use row 5)
    const headers = [
      'Artist', 'Recording Title', 'ISRC',
      'Basis of Claim', 'Percentage Claimed', 'Rights Begin Date', 'Rights End Date',
      'Non-US Territories',
      'Recording Version', 'Duration (HH:MM:SS)', 'Genre', 'Recording Date',
      'Country of Recording', 'Country of Mastering', 'Copyright Owner Nationality',
      'Date of First Release', 'Countries of First Release', '(P) Line',
      'ISWC', 'Composer(s)', 'Publisher(s)',
      'Release Artist', 'Release Title (Album)', 'Release Version', 'UPC',
      'Catalog #', 'Release Date', 'Country of Release', 'Release Label',
    ];

    // Section headers (row 4)
    ws.getCell('A4').value = 'Minimum Recording Information';
    ws.getCell('A4').font = { bold: true };
    ws.getCell('D4').value = 'Sound Recording Copyright Owner Claim';
    ws.getCell('D4').font = { bold: true };
    ws.getCell('I4').value = 'Additional Recording Information';
    ws.getCell('I4').font = { bold: true };
    ws.getCell('V4').value = 'Release Information';
    ws.getCell('V4').font = { bold: true };

    const headerRow = ws.getRow(5);
    headers.forEach((h, i) => {
      const cell = headerRow.getCell(i + 1);
      cell.value = h;
      cell.font = { bold: true, size: 10 };
      cell.fill = { type: 'pattern', pattern: 'solid', fgColor: { argb: 'FF2D4A7A' } };
      cell.font = { bold: true, size: 10, color: { argb: 'FFFFFFFF' } };
    });

    // Data rows
    for (let i = 0; i < allSheets.length; i++) {
      const s = allSheets[i];
      const row = ws.getRow(6 + i);
      const composerStr = s.composers || (s.songwriters || []).map(w => w.name).join(', ');
      const publisherStr = s.publishers && typeof s.publishers === 'string' ? s.publishers
        : (Array.isArray(s.publishers) ? s.publishers.map(p => p.name).join(', ') : (s.publisherEntity || ''));
      const durationHMS = s.duration ? '00:' + s.duration : '';
      row.values = [
        s.primaryArtist, s.songTitle, s.isrc,
        s.claimBasis || 'Copyright Owner', s.percentageClaimed || '100',
        s.rightsBegin || s.releaseDate, s.rightsEnd || 'Perpetuity',
        s.nonUsTerritories || '',
        s.version || '', durationHMS, s.genre || '', s.recordingDate || s.releaseDate || '',
        s.countryOfRecording || 'US', s.countryOfMastering || 'US', s.copyrightOwnerNationality || 'US',
        s.dateOfFirstRelease || s.releaseDate || '', s.countriesOfFirstRelease || 'US', s.pLine || '',
        s.iswc || '', composerStr, publisherStr,
        s.releaseArtist || s.primaryArtist, s.albumName || '', s.releaseVersion || '', s.upc || '',
        s.catalogNumber || '', s.releaseDate || '', s.countryOfRelease || 'US', s.label || '',
      ];
    }

    // Auto-width columns
    ws.columns.forEach(col => {
      let maxLen = 12;
      col.eachCell({ includeEmpty: false }, cell => {
        const len = cell.value ? String(cell.value).length : 0;
        if (len > maxLen) maxLen = len;
      });
      col.width = Math.min(maxLen + 2, 40);
    });

    logOperation(req, 'admin.soundexchange_xlsx_exported', null, null, { count: allSheets.length });

    res.setHeader('Content-Type', 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet');
    res.setHeader('Content-Disposition', 'attachment; filename="SoundExchange_ISRC_Ingest_' + new Date().toISOString().slice(0,10) + '.xlsx"');
    await wb.xlsx.write(res);
    res.end();
  } catch (e) {
    console.error('soundexchange-xlsx error:', e);
    res.status(500).json({ error: 'Failed to generate XLSX: ' + e.message });
  }
});

// Mark a release as queued for registration (status: pending → ready → submitted → confirmed)
app.post('/api/admin/registration/queue', requireAdmin, (req, res) => {
  const { userId, releaseTitle, platform } = req.body;
  if (!userId || !releaseTitle) return res.status(400).json({ error: 'userId and releaseTitle required' });

  dbHelpers.prepare(`
    INSERT INTO registration_queue (user_id, release_title, platform, status)
    VALUES (?, ?, ?, 'pending')
    ON CONFLICT(user_id, release_title, platform) DO UPDATE SET status = 'pending', reviewed_at = NULL, submitted_at = NULL, confirmed_at = NULL, confirmation_number = NULL
  `).run(userId, releaseTitle, platform || 'bmi');
  flushDbNow();

  logOperation(req, 'admin.registration_queued', 'user', userId, { release: releaseTitle, platform: platform || 'bmi' });
  res.json({ success: true });
});

// Update registration status (ready, submitted, confirmed) + confirmation number
app.post('/api/admin/registration/:id/update', requireAdmin, (req, res) => {
  const id = parseInt(req.params.id, 10);
  if (!Number.isInteger(id) || id <= 0) return res.status(400).json({ error: 'Invalid id' });

  const entry = dbHelpers.prepare('SELECT * FROM registration_queue WHERE id = ?').get(id);
  if (!entry) return res.status(404).json({ error: 'Queue entry not found' });

  const { status, confirmationNumber, adminNotes } = req.body;
  const validStatuses = ['pending', 'ready', 'submitted', 'confirmed', 'skipped'];
  if (status && !validStatuses.includes(status)) return res.status(400).json({ error: 'Invalid status' });

  const updates = [];
  const params = [];
  if (status) {
    updates.push('status = ?');
    params.push(status);
    if (status === 'ready') { updates.push("reviewed_at = datetime('now')"); }
    if (status === 'submitted') { updates.push("submitted_at = datetime('now')"); }
    if (status === 'confirmed') { updates.push("confirmed_at = datetime('now')"); }
  }
  if (confirmationNumber !== undefined) { updates.push('confirmation_number = ?'); params.push(confirmationNumber); }
  if (adminNotes !== undefined) { updates.push('admin_notes = ?'); params.push(adminNotes); }
  if (updates.length === 0) return res.status(400).json({ error: 'Nothing to update' });

  params.push(id);
  dbHelpers.prepare(`UPDATE registration_queue SET ${updates.join(', ')} WHERE id = ?`).run(...params);
  flushDbNow();

  logOperation(req, 'admin.registration_updated', 'registration_queue', id, {
    release: entry.release_title, user_id: entry.user_id, status, confirmationNumber
  });
  res.json({ success: true });
});

// Bulk queue multiple releases for registration
app.post('/api/admin/registration/bulk-queue', requireAdmin, (req, res) => {
  const { items } = req.body; // [{ userId, releaseTitle, platform }]
  if (!items || !Array.isArray(items) || items.length === 0) return res.status(400).json({ error: 'Items array required' });
  if (items.length > 50) return res.status(413).json({ error: 'Max 50 items per bulk queue' });

  const insert = dbHelpers.transaction(() => {
    for (const item of items) {
      dbHelpers.prepare(`
        INSERT INTO registration_queue (user_id, release_title, platform, status)
        VALUES (?, ?, ?, 'pending')
        ON CONFLICT(user_id, release_title, platform) DO NOTHING
      `).run(item.userId, item.releaseTitle, item.platform || 'bmi');
    }
  });
  insert();
  flushDbNow();

  logOperation(req, 'admin.registration_bulk_queued', 'registration_queue', null, { count: items.length });
  res.json({ success: true, queued: items.length });
});

// Admin endpoint to manually trigger "Ask in Slack" for a release
app.post('/api/admin/registration/ask-slack', requireAdmin, async (req, res) => {
  const { userId, releaseTitle } = req.body;
  if (!userId || !releaseTitle) return res.status(400).json({ error: 'userId and releaseTitle required' });
  if (!SLACK_BOT_TOKEN) return res.status(503).json({ error: 'SLACK_BOT_TOKEN not configured' });

  // Get the release data
  const row = dbHelpers.prepare('SELECT value FROM user_data WHERE user_id = ? AND key = ?').get(userId, 'release_data_all');
  if (!row) return res.status(404).json({ error: 'No release data for this user' });
  let releases;
  try { releases = JSON.parse(row.value); } catch (e) { return res.status(500).json({ error: 'Corrupt data' }); }
  const release = releases[releaseTitle];
  if (!release) return res.status(404).json({ error: 'Release not found' });

  // Gather missing fields across ALL platforms
  const allMissing = {};
  for (const platform of ALL_PLATFORMS) {
    const completeness = checkPlatformCompleteness(release, platform);
    if (completeness.missing.length > 0) allMissing[platform] = completeness.missing;
  }
  if (Object.keys(allMissing).length === 0) return res.status(400).json({ error: 'No missing fields across any platform' });

  const platformNames = Object.keys(allMissing).map(p => PLATFORM_DEFS[p].shortName).join(', ');
  const sent = await askEliteUserForMissingFields(userId, releaseTitle, release, allMissing);
  if (sent) {
    res.json({ success: true, message: 'Slack DM sent covering ' + platformNames });
  } else {
    res.status(400).json({ error: 'Could not send Slack DM — user may not be in your workspace or not Elite tier' });
  }
});

// --- Slack Interactive Endpoint ---
// Receives form submissions when Elite users fill in missing registration fields via Slack DM.
// Slack sends application/x-www-form-urlencoded with a `payload` JSON field.
app.post('/api/slack/interactions',
  express.urlencoded({ extended: true, verify: (req, _res, buf) => { req.rawBody = buf.toString(); } }),
  async (req, res) => {
  try {
    // Verify signature
    if (SLACK_SIGNING_SECRET && !verifySlackSignature(req)) {
      return res.status(401).send('Invalid signature');
    }

    const payload = JSON.parse(req.body.payload);

    // Handle block_actions (button clicks)
    if (payload.type === 'block_actions') {
      const action = payload.actions && payload.actions[0];
      // Accept both old and new action IDs for backward compat
      if (!action || (action.action_id !== 'submit_registration_fields' && action.action_id !== 'submit_bmi_fields')) {
        return res.json({ text: 'Unknown action' });
      }

      let meta;
      try { meta = JSON.parse(action.value); } catch (e) { return res.json({ text: 'Invalid action data' }); }
      const { userId, releaseTitle, platforms: submittedPlatforms, platform: legacyPlatform } = meta;
      // Support both new (platforms array) and old (single platform string)
      const affectedPlatforms = submittedPlatforms || [legacyPlatform || 'pro'];

      // Extract values from the message blocks state
      const state = payload.state && payload.state.values;
      if (!state) return res.json({ text: 'No form data received' });

      // Dynamically extract all text input fields from state
      const updates = {};
      const simpleTextFields = ['songTitle', 'primaryArtist', 'isrc', 'upc', 'iswc', 'releaseDate',
        'genrePrimary', 'genreSecondary', 'albumName', 'label', 'language', 'featArtist',
        'publisherEntity', 'durationMin', 'durationSec', 'workContent', 'proAffiliation'];
      for (const key of simpleTextFields) {
        const val = state['field_' + key]?.[key]?.value;
        if (val && val.trim()) updates[key] = val.trim();
      }
      // Lyrics (multiline)
      if (state.field_lyrics?.lyrics?.value?.trim()) updates.lyrics = state.field_lyrics.lyrics.value.trim();
      // Explicit (select)
      if (state.field_explicit?.explicit?.selected_option?.value) updates.explicit = state.field_explicit.explicit.selected_option.value;

      // Songwriter fields
      const swName = state.field_songwriter_name?.songwriter_name?.value?.trim();
      const swIpi = state.field_songwriter_ipi?.songwriter_ipi?.value?.trim();
      const swPro = state.field_songwriter_pro?.songwriter_pro?.selected_option?.value;
      // Publisher fields
      const pubName = state.field_publisher_name?.publisher_name?.value?.trim();
      const pubIpi = state.field_publisher_ipi?.publisher_ipi?.value?.trim();

      // Load existing release data
      const row = dbHelpers.prepare('SELECT value FROM user_data WHERE user_id = ? AND key = ?').get(userId, 'release_data_all');
      if (!row) return res.json({ text: 'Could not find your release data. Please update on the website.' });

      let releases;
      try { releases = JSON.parse(row.value); } catch (e) {
        return res.json({ text: 'Data error — please update on the website.' });
      }

      const release = releases[releaseTitle];
      if (!release) return res.json({ text: 'Release "' + releaseTitle + '" not found.' });

      // Apply updates
      let changed = false;
      for (const [k, v] of Object.entries(updates)) {
        if (v && (!release[k] || !String(release[k]).trim())) {
          release[k] = v;
          changed = true;
        }
      }

      // Handle songwriter
      if (swName) {
        if (!Array.isArray(release.songwriters)) release.songwriters = [];
        const existingSw = release.songwriters.find(s => s && s.name && s.name.toLowerCase() === swName.toLowerCase());
        if (existingSw) {
          if (swIpi && !existingSw.ipi) { existingSw.ipi = swIpi; changed = true; }
          if (swPro && !existingSw.pro) { existingSw.pro = swPro; changed = true; }
        } else {
          release.songwriters.push({ name: swName, role: 'Both', ipi: swIpi || '', pro: swPro || '', split: '100%' });
          changed = true;
        }
      }

      // Handle publisher
      if (pubName) {
        if (!Array.isArray(release.publishers)) release.publishers = [];
        const existingPub = release.publishers.find(p => p && p.name && p.name.toLowerCase() === pubName.toLowerCase());
        if (existingPub) {
          if (pubIpi && !existingPub.ipi) { existingPub.ipi = pubIpi; changed = true; }
        } else {
          release.publishers.push({ name: pubName, ipi: pubIpi || '' });
          changed = true;
        }
      }

      if (changed) {
        releases[releaseTitle] = release;
        const serialized = JSON.stringify(releases);
        dbHelpers.prepare(`
          INSERT INTO user_data (user_id, key, value, updated_at, version) VALUES (?, 'release_data_all', ?, datetime('now'), 1)
          ON CONFLICT(user_id, key) DO UPDATE SET value = excluded.value, updated_at = datetime('now'), version = COALESCE(version, 0) + 1
        `).run(userId, serialized);
        flushDbNow();

        // Re-check completeness for ALL affected platforms and update queue
        const statusLines = [];
        let allComplete = true;
        const stillMissing = [];
        for (const plat of affectedPlatforms) {
          const comp = checkPlatformCompleteness(release, plat);
          const platName = (PLATFORM_DEFS[plat] || {}).shortName || plat;
          dbHelpers.prepare(`
            UPDATE registration_queue SET field_completeness = ?, missing_fields = ?
            WHERE user_id = ? AND release_title = ? AND platform = ?
          `).run(String(comp.pct), JSON.stringify(comp.missing), userId, releaseTitle, plat);
          if (comp.missing.length === 0) {
            statusLines.push(':white_check_mark: ' + platName + ' — complete');
          } else {
            allComplete = false;
            statusLines.push(':warning: ' + platName + ' — still needs: ' + comp.missing.join(', '));
            stillMissing.push(...comp.missing.map(m => platName + ': ' + m));
          }
        }
        flushDbNow();

        logOperation(null, 'slack.missing_fields_received', 'user', userId, {
          release: releaseTitle, platforms: affectedPlatforms, fields: Object.keys(updates).concat(swName ? ['songwriter'] : []).concat(pubName ? ['publisher'] : [])
        });

        // Notify admin
        notifySlack(`:inbox_tray: *${releaseTitle}* — user submitted registration fields via Slack\n${statusLines.join('\n')}`);

        // Respond to the user
        let thankYou;
        if (allComplete) {
          thankYou = ':white_check_mark: Thank you! All info received — we\'ll handle the registrations for you.';
        } else {
          const unique = [...new Set(stillMissing)];
          thankYou = ':+1: Thank you! We still need:\n' + unique.map(m => '• ' + m).join('\n') + '\n\nYou can update these on the website anytime.';
        }

        // Update the original message to show confirmation
        await slackApi('chat.update', {
          channel: payload.channel.id,
          ts: payload.message.ts,
          text: thankYou,
          blocks: [{
            type: 'section',
            text: { type: 'mrkdwn', text: thankYou + '\n\n_Submitted info for *' + releaseTitle + '*_' }
          }],
        });

        return res.status(200).send('');
      }

      // Nothing changed
      await slackApi('chat.update', {
        channel: payload.channel.id,
        ts: payload.message.ts,
        text: 'No new info was provided. You can fill in the fields and click Submit again, or update on the website.',
        blocks: [{
          type: 'section',
          text: { type: 'mrkdwn', text: 'No new info was provided. You can fill in the fields and click Submit again, or update on the website.' }
        }],
      });
      return res.status(200).send('');
    }

    // Default response for unhandled interaction types
    res.status(200).send('');
  } catch (e) {
    console.error('[SLACK-INTERACT] Error:', e.message);
    res.status(200).send(''); // Always 200 to Slack so it doesn't retry
  }
});

// Static files — only serve safe static assets, NOT the entire project directory
const publicDir = path.join(__dirname, 'public');
if (!fs.existsSync(publicDir)) fs.mkdirSync(publicDir, { recursive: true });
// Serve logo.png and other safe static files from project root (only specific extensions)
app.get('/logo.png', (req, res) => {
  const logoPath = path.join(__dirname, 'logo.png');
  if (fs.existsSync(logoPath)) res.sendFile(logoPath);
  else res.sendStatus(404);
});
app.get('/redemption-logo.png', (req, res) => {
  const logoPath = path.join(__dirname, 'redemption-logo.png');
  if (fs.existsSync(logoPath)) res.sendFile(logoPath);
  else res.sendStatus(404);
});
app.use('/public', express.static(publicDir, { index: false }));

// --- Start Server ---
async function startServer() {
  console.log(`[BOOT] Starting Rollout Heaven...`);
  console.log(`[BOOT] Node ${process.version}, PORT=${PORT}, NODE_ENV=${process.env.NODE_ENV}`);
  console.log(`[BOOT] RAILWAY=${!!process.env.RAILWAY_ENVIRONMENT}`);
  console.log(`[BOOT] DB_PATH=${DB_PATH}, DATA_DIR=${DATA_DIR}`);

  // Verify data directory is writable
  try {
    const testFile = path.join(DATA_DIR, '.write-test');
    fs.writeFileSync(testFile, 'ok');
    fs.unlinkSync(testFile);
    console.log('[BOOT] DATA_DIR is writable');
  } catch (e) {
    console.error('[BOOT] WARNING: DATA_DIR not writable:', e.message);
  }

  // Acquire the single-instance lock BEFORE touching the DB. If another
  // replica is already running this exits before any reads/writes happen,
  // so we can't corrupt state just by attempting to boot.
  acquireInstanceLock();

  console.log('[BOOT] Loading sql.js...');
  const SQL = await initSqlJs();
  console.log('[BOOT] sql.js loaded');

  if (fs.existsSync(DB_PATH)) {
    console.log('[BOOT] Loading existing database...');
    try {
      const fileBuffer = fs.readFileSync(DB_PATH);
      db = new SQL.Database(fileBuffer);
      // Quick sanity check — if this throws, DB is corrupted
      db.exec("SELECT count(*) FROM sqlite_master");
      console.log('[BOOT] Existing database loaded OK');
    } catch (e) {
      console.error('[BOOT] Database corrupted:', e.message);
      // Move corrupt file aside so we can try to recover from a backup.
      try {
        fs.renameSync(DB_PATH, DB_PATH + '.corrupt.' + Date.now());
      } catch (_) {}
      // H2 restore path: try the most recent valid backup before giving up
      // and creating a fresh DB. If ANY backup loads, continue from there;
      // the operator sees a loud [RESTORE] log in Railway so they know.
      const restored = tryRestoreFromBackup(SQL);
      if (restored) {
        db = restored;
        console.error('[BOOT] Database restored from backup — continuing');
      } else {
        console.error('[BOOT] FATAL-ish: no usable backup, starting fresh DB. User data lost.');
        db = new SQL.Database();
        console.log('[BOOT] Fresh database created');
      }
    }
  } else {
    console.log('[BOOT] Creating new database...');
    db = new SQL.Database();
  }
  console.log('[BOOT] Database ready');

  initDb();
  console.log('[BOOT] Database initialized');

  // --- Data Safety: boot-time row count validation + recovery manifest ---
  const MANIFEST_PATH = path.join(DATA_DIR, 'recovery_manifest.json');
  try {
    const MANIFEST_TABLES = [
      'users', 'user_data', 'user_xp', 'user_achievements', 'xp_log',
      'support_tickets', 'api_usage', 'elite_onboarding', 'redemption_requests',
      'outreach_contacts', 'outreach_purchases', 'submission_progress',
      'stripe_events', 'operation_journal', 'deleted_users_archive'
    ];
    const rowCounts = {};
    for (const t of MANIFEST_TABLES) {
      try {
        const r = dbHelpers.prepare(`SELECT COUNT(*) AS cnt FROM ${t}`).get();
        rowCounts[t] = r ? r.cnt : 0;
      } catch (_) { rowCounts[t] = -1; } // table doesn't exist yet
    }

    // Compare against previous manifest — warn on unexpected zero drops
    if (fs.existsSync(MANIFEST_PATH)) {
      try {
        const prev = JSON.parse(fs.readFileSync(MANIFEST_PATH, 'utf8'));
        if (prev.row_counts) {
          for (const t of MANIFEST_TABLES) {
            const prevCount = prev.row_counts[t];
            if (typeof prevCount === 'number' && prevCount > 0 && rowCounts[t] === 0) {
              console.error(`[BOOT] WARNING: table "${t}" dropped from ${prevCount} rows to 0 — possible data loss`);
            }
          }
        }
      } catch (_) { /* stale/corrupt manifest — just overwrite */ }
    }

    // Find latest backups for the manifest
    let latestHourly = null, latestDaily = null, hourlyCount = 0, dailyCount = 0;
    try {
      if (fs.existsSync(BACKUP_DIR)) {
        const bfiles = fs.readdirSync(BACKUP_DIR).filter(f => f.endsWith('.bak'));
        const hourly = bfiles.filter(f => f.includes('.hourly.')).sort().reverse();
        const daily = bfiles.filter(f => f.includes('.daily.')).sort().reverse();
        latestHourly = hourly[0] || null;
        latestDaily = daily[0] || null;
        hourlyCount = hourly.length;
        dailyCount = daily.length;
      }
    } catch (_) {}

    const manifest = {
      last_boot: new Date().toISOString(),
      db_path: DB_PATH,
      backup_dir: BACKUP_DIR,
      latest_hourly: latestHourly,
      latest_daily: latestDaily,
      backup_count: { hourly: hourlyCount, daily: dailyCount },
      tables: MANIFEST_TABLES,
      row_counts: rowCounts,
      has_operation_journal: true,
      has_deleted_users_archive: true
    };
    fs.writeFileSync(MANIFEST_PATH, JSON.stringify(manifest, null, 2));

    // Log summary
    const countSummary = MANIFEST_TABLES.filter(t => rowCounts[t] > 0).map(t => `${t}=${rowCounts[t]}`).join(' ');
    console.log(`[BOOT] Row counts: ${countSummary || '(empty DB)'}`);
    console.log(`[BOOT] Recovery manifest written to ${MANIFEST_PATH}`);
  } catch (e) {
    console.error('[BOOT] Recovery manifest write failed (non-fatal):', e.message);
  }

  // Kick off backup timers now that the DB is live (H2).
  startBackupTimers();
  console.log('[BOOT] Backup timers armed (hourly + daily to', BACKUP_DIR + ')');

  const server = app.listen(PORT, '0.0.0.0', () => {
    console.log(`[BOOT] Rollout Heaven running on 0.0.0.0:${PORT}`);
  });
  server.timeout = 120000;
  server.keepAliveTimeout = 120000;
}

// Log uncaught errors loudly. Exit only on uncaughtException (state may be
// corrupted); keep running on unhandledRejection (typically a single broken
// promise chain that shouldn't take the whole process down).
process.on('uncaughtException', (err) => {
  console.error('[FATAL] Uncaught exception:', err);
  try { flushDbNow(); } catch(_) {}
  try { releaseInstanceLock(); } catch(_) {}
  process.exit(1);
});
process.on('unhandledRejection', (err) => {
  console.error('[WARN] Unhandled rejection:', err);
});

startServer().catch(err => {
  console.error('[BOOT] Failed to start server:', err);
  process.exit(1);
});
