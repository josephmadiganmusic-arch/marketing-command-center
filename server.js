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
    const tmpPath = DB_PATH + '.tmp';
    fs.writeFileSync(tmpPath, Buffer.from(data));
    fs.renameSync(tmpPath, DB_PATH);
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
  process.exit(0);
}
process.on('SIGTERM', () => shutdown('SIGTERM'));
process.on('SIGINT', () => shutdown('SIGINT'));

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
      const exists = dbHelpers.prepare('SELECT id FROM users WHERE email = ?').get(admin.email);
      if (!exists) {
        const hash = bcrypt.hashSync(ADMIN_PASSWORD, 10);
        dbHelpers.prepare('INSERT INTO users (email, password, role, subscription_status) VALUES (?, ?, ?, ?)').run(admin.email, hash, 'admin', 'active');
      }
    }
  }
}

// --- Stripe Setup ---
const stripe = process.env.STRIPE_SECRET_KEY ? new Stripe(process.env.STRIPE_SECRET_KEY) : null;
const STRIPE_PRICE_ID = process.env.STRIPE_PRICE_ID || '';
const STRIPE_WEBHOOK_SECRET = process.env.STRIPE_WEBHOOK_SECRET || '';

// --- Email Setup (Resend — HTTPS API, works on Railway) ---
const resend = process.env.RESEND_API_KEY ? new Resend(process.env.RESEND_API_KEY) : null;
const EMAIL_FROM = process.env.EMAIL_FROM || 'Rollout Heaven <onboarding@resend.dev>';

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
  const user = dbHelpers.prepare('SELECT * FROM users WHERE id = ?').get(req.session.userId);
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
  const user = dbHelpers.prepare('SELECT * FROM users WHERE id = ?').get(req.session.userId);
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
  const user = dbHelpers.prepare('SELECT * FROM users WHERE id = ?').get(req.session.userId);
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

// Pre-computed dummy hash so misses do equivalent CPU work to hits.
// Generated once at boot so we don't burn ~50ms on every cold login miss.
const DUMMY_BCRYPT_HASH = bcrypt.hashSync('dummy-password-for-timing-equalization', 10);

app.post('/api/login', rlAuth, (req, res) => {
  const { email, password, rememberMe } = req.body || {};
  if (!email || !password) return res.status(400).json({ error: 'Email and password required' });
  if (typeof email !== 'string' || typeof password !== 'string') return res.status(400).json({ error: 'Invalid input' });
  if (email.length > 254 || password.length > 200) return res.status(400).json({ error: 'Invalid input' });

  const user = dbHelpers.prepare('SELECT * FROM users WHERE email = ?').get(email.toLowerCase().trim());
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
  const { email, password } = req.body;
  if (!email || !password) return res.status(400).json({ error: 'Email and password required' });
  if (typeof email !== 'string' || typeof password !== 'string') return res.status(400).json({ error: 'Invalid input' });
  if (password.length < 8 || password.length > 200) return res.status(400).json({ error: 'Password must be 8–200 characters' });
  if (email.length > 254 || !EMAIL_RE.test(email.trim())) return res.status(400).json({ error: 'Invalid email address' });

  const cleanEmail = email.toLowerCase().trim();
  const existing = dbHelpers.prepare('SELECT id FROM users WHERE email = ?').get(cleanEmail);
  if (existing) return res.status(409).json({ error: 'Account already exists. Please log in.' });

  const hash = bcrypt.hashSync(password, 10);
  const trialEnd = new Date(Date.now() + 7 * 24 * 60 * 60 * 1000).toISOString();
  const token = crypto.randomBytes(32).toString('hex');
  const tokenExpires = new Date(Date.now() + 24 * 60 * 60 * 1000).toISOString();

  dbHelpers.prepare(
    'INSERT INTO users (email, password, subscription_status, trial_ends_at, verification_token, verification_expires) VALUES (?, ?, ?, ?, ?, ?)'
  ).run(cleanEmail, hash, 'trialing', trialEnd, token, tokenExpires);

  // Respond immediately, send email in background (don't block the request).
  // Auto-verification only happens when verification is explicitly disabled —
  // never as a silent fallback when Resend is missing (see C7).
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
    res.json({ success: true, needsVerification: false });
  }
});

// --- Email Verification Route ---
app.get('/api/verify-email', (req, res) => {
  const { token } = req.query;
  if (!token) return res.status(400).send('Invalid verification link.');

  const user = dbHelpers.prepare('SELECT * FROM users WHERE verification_token = ?').get(token);
  if (!user) return res.status(400).send('Invalid or expired verification link.');

  if (new Date(user.verification_expires) < new Date()) {
    return res.status(400).send('Verification link has expired. Please request a new one.');
  }

  dbHelpers.prepare('UPDATE users SET email_verified = 1, verification_token = NULL, verification_expires = NULL WHERE id = ?').run(user.id);

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

  const user = dbHelpers.prepare('SELECT * FROM users WHERE email = ?').get(email.toLowerCase().trim());
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
  const user = dbHelpers.prepare('SELECT id, email, role, subscription_status, trial_ends_at FROM users WHERE id = ?').get(req.session.userId);
  if (!user) return res.json({ loggedIn: false });

  let daysLeft = null;
  if (user.subscription_status === 'trialing' && user.trial_ends_at) {
    daysLeft = Math.max(0, Math.ceil((new Date(user.trial_ends_at) - new Date()) / (1000 * 60 * 60 * 24)));
  }

  res.json({ loggedIn: true, ...user, hasAccess: hasAccess(user), trialDaysLeft: daysLeft });
});

// --- User Data Save/Load (server-side persistence) ---
app.post('/api/data/save', requireAuth, (req, res) => {
  const { key, value } = req.body;
  if (!key) return res.status(400).json({ error: 'Key required' });
  dbHelpers.prepare(`
    INSERT INTO user_data (user_id, key, value, updated_at) VALUES (?, ?, ?, datetime('now'))
    ON CONFLICT(user_id, key) DO UPDATE SET value = excluded.value, updated_at = datetime('now')
  `).run(req.user.id, key, typeof value === 'string' ? value : JSON.stringify(value));
  res.json({ success: true });
});

app.post('/api/data/save-batch', requireAuth, (req, res) => {
  const { items } = req.body;
  if (!items || !Array.isArray(items)) return res.status(400).json({ error: 'Items array required' });
  const saveBatch = dbHelpers.transaction(() => {
    for (const { key, value } of items) {
      if (key) {
        dbHelpers.prepare(`
          INSERT INTO user_data (user_id, key, value, updated_at) VALUES (?, ?, ?, datetime('now'))
          ON CONFLICT(user_id, key) DO UPDATE SET value = excluded.value, updated_at = datetime('now')
        `).run(req.user.id, key, typeof value === 'string' ? value : JSON.stringify(value));
      }
    }
  });
  saveBatch();
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
  if (!stripe || !STRIPE_PRICE_ID) {
    return res.status(503).json({ error: 'Payments not configured yet' });
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
    const session = await stripe.checkout.sessions.create({
      customer: customerId,
      mode: 'subscription',
      payment_method_types: ['card'],
      line_items: [{ price: STRIPE_PRICE_ID, quantity: 1 }],
      subscription_data: {
        trial_period_days: 7
      },
      success_url: `${baseUrl}/subscribe?success=1`,
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
        dbHelpers.prepare('UPDATE users SET subscription_status = ?, stripe_subscription_id = ?, trial_ends_at = ? WHERE stripe_customer_id = ?')
          .run(status, obj.id, trialEnd, obj.customer);
        break;
      }
      case 'customer.subscription.deleted': {
        dbHelpers.prepare('UPDATE users SET subscription_status = ?, stripe_subscription_id = NULL WHERE stripe_customer_id = ?')
          .run('canceled', obj.customer);
        break;
      }
      case 'invoice.payment_failed': {
        // Mark as past_due so the UI can prompt the user to update billing.
        dbHelpers.prepare('UPDATE users SET subscription_status = ? WHERE stripe_customer_id = ?')
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
        break;
      }
    }
  } catch (err) {
    console.error('[STRIPE] Handler error for', event.type, err.message);
    // Roll back the dedupe row so Stripe will retry.
    dbHelpers.prepare('DELETE FROM stripe_events WHERE id = ?').run(event.id);
    return res.sendStatus(500);
  }

  // Flush DB to disk synchronously after a successful webhook so we don't
  // lose subscription state if the process dies inside the 100ms debounce.
  try { flushDbNow(); } catch (e) { console.error('[STRIPE] flushDbNow failed:', e.message); }

  res.sendStatus(200);
});

// --- Subscribe Page ---
app.get('/subscribe', requireAuth, (req, res) => {
  res.sendFile(path.join(__dirname, 'subscribe.html'));
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

app.post('/api/research', requireActive, rlResearch, async (req, res) => {
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
  const user = dbHelpers.prepare('SELECT * FROM users WHERE id = ?').get(req.session.userId);
  if (!user || user.role !== 'admin') return res.status(403).json({ error: 'Admin access required' });
  req.user = user;
  next();
}

// --- Admin API ---

// Analytics & user list
app.get('/api/admin/users', requireAdmin, (req, res) => {
  const users = dbHelpers.prepare(`
    SELECT id, email, role, subscription_status, trial_ends_at, created_at
    FROM users ORDER BY created_at DESC
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

// Delete user (admin only, can't delete admins)
app.delete('/api/admin/users/:id', requireAdmin, (req, res) => {
  const id = parseInt(req.params.id, 10);
  if (!Number.isInteger(id) || id <= 0) return res.status(400).json({ error: 'Invalid id' });
  const user = dbHelpers.prepare('SELECT * FROM users WHERE id = ?').get(id);
  if (!user) return res.status(404).json({ error: 'User not found' });
  if (user.role === 'admin') return res.status(403).json({ error: 'Cannot delete admin accounts' });
  dbHelpers.prepare('DELETE FROM user_data WHERE user_id = ?').run(id);
  dbHelpers.prepare('DELETE FROM user_xp WHERE user_id = ?').run(id);
  dbHelpers.prepare('DELETE FROM user_achievements WHERE user_id = ?').run(id);
  dbHelpers.prepare('DELETE FROM xp_log WHERE user_id = ?').run(id);
  dbHelpers.prepare('DELETE FROM support_tickets WHERE user_id = ?').run(id);
  dbHelpers.prepare('DELETE FROM api_usage WHERE user_id = ?').run(id);
  dbHelpers.prepare('DELETE FROM users WHERE id = ?').run(id);
  res.json({ success: true, deleted: user.email });
});

// Support tickets — list all
app.get('/api/admin/tickets', requireAdmin, (req, res) => {
  const tickets = dbHelpers.prepare(`
    SELECT * FROM support_tickets ORDER BY
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
  res.json({ success: true });
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
          model: 'claude-sonnet-4-20250514',
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

  res.json({ success: true, aiResponse, escalated });
});

// User's own tickets
app.get('/api/support/my-tickets', requireAuth, (req, res) => {
  const tickets = dbHelpers.prepare('SELECT * FROM support_tickets WHERE user_id = ? ORDER BY created_at DESC').all(req.user.id);
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
        dbHelpers.prepare('UPDATE user_xp SET total_xp = ?, level = ? WHERE user_id = ?').run(newTotal, newLevel, userId);
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
    dbHelpers.prepare('UPDATE user_xp SET last_active_date = ?, current_streak = ?, longest_streak = ?, logins_total = logins_total + 1, total_xp = ?, level = ? WHERE user_id = ?')
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
    campaign_generate: 'UPDATE user_xp SET total_xp = ?, level = ?, campaigns_generated = campaigns_generated + 1 WHERE user_id = ?',
    task_complete:     'UPDATE user_xp SET total_xp = ?, level = ?, tasks_completed = tasks_completed + 1 WHERE user_id = ?',
    email_generate:    'UPDATE user_xp SET total_xp = ?, level = ?, emails_generated = emails_generated + 1 WHERE user_id = ?',
    research_run:      'UPDATE user_xp SET total_xp = ?, level = ?, research_runs = research_runs + 1 WHERE user_id = ?',
    playlist_submit:   'UPDATE user_xp SET total_xp = ?, level = ?, playlists_submitted = playlists_submitted + 1 WHERE user_id = ?',
    content_copy:      'UPDATE user_xp SET total_xp = ?, level = ?, content_copied = content_copied + 1 WHERE user_id = ?',
    release_complete:  'UPDATE user_xp SET total_xp = ?, level = ?, releases_completed = releases_completed + 1 WHERE user_id = ?'
  };
  const sql = STAT_UPDATE_SQL[action] || 'UPDATE user_xp SET total_xp = ?, level = ? WHERE user_id = ?';
  dbHelpers.prepare(sql).run(newTotal, newLevel, req.user.id);

  dbHelpers.prepare('INSERT INTO xp_log (user_id, action, xp_amount, description) VALUES (?, ?, ?, ?)').run(req.user.id, action, amount, action.replace(/_/g, ' '));

  // Check for new achievements
  const newAchievements = checkAndUnlockAchievements(req.user.id);

  // Re-fetch for accurate totals (achievements may have added XP)
  const final = dbHelpers.prepare('SELECT total_xp, level FROM user_xp WHERE user_id = ?').get(req.user.id);
  const finalLeveledUp = final.level > xp.level;

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
// before we touch Anthropic. Add new models here as the app needs them.
const ALLOWED_CLAUDE_MODELS = new Set([
  'claude-sonnet-4-20250514',
  'claude-3-5-sonnet-20241022',
  'claude-3-5-haiku-20241022'
]);
const DEFAULT_CLAUDE_MODEL = 'claude-sonnet-4-20250514';

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

app.post('/api/claude', requireActive, rlClaude, async (req, res) => {
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
  // but cheaper to bounce here before we burn tokens.
  const bodyBytes = JSON.stringify({ system, messages }).length;
  if (bodyBytes > 200000) return res.status(413).json({ error: 'Request too large' });

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

app.get('/', requireAuth, (req, res) => {
  let trialDays = -1;
  if (req.user.role !== 'admin' && req.user.subscription_status === 'trialing' && req.user.trial_ends_at) {
    trialDays = Math.max(0, Math.ceil((new Date(req.user.trial_ends_at) - new Date()) / (1000 * 60 * 60 * 24)));
  }
  const html = APP_HTML_TEMPLATE
    .replace('%%CLAUDE_API_KEY%%', '') // API key no longer sent to client
    .replace('%%USER_ID%%', String(req.user.id))
    .replace('%%USER_ROLE%%', escHtml(req.user.role || 'user'))
    .replace('%%TRIAL_DAYS%%', String(trialDays))
    .replace('%%SUB_STATUS%%', escHtml(req.user.subscription_status || 'none'))
    .replace('%%USER_EMAIL%%', escHtml(req.user.email || ''));
  res.set('Cache-Control', 'no-store, no-cache, must-revalidate');
  res.set('Pragma', 'no-cache');
  res.type('html').send(html);
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
      console.error('[BOOT] Database corrupted, backing up and creating fresh:', e.message);
      try {
        fs.renameSync(DB_PATH, DB_PATH + '.corrupt.' + Date.now());
      } catch (_) {}
      db = new SQL.Database();
      console.log('[BOOT] Fresh database created');
    }
  } else {
    console.log('[BOOT] Creating new database...');
    db = new SQL.Database();
  }
  console.log('[BOOT] Database ready');

  initDb();
  console.log('[BOOT] Database initialized');

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
  process.exit(1);
});
process.on('unhandledRejection', (err) => {
  console.error('[WARN] Unhandled rejection:', err);
});

startServer().catch(err => {
  console.error('[BOOT] Failed to start server:', err);
  process.exit(1);
});
