const express = require('express');
const fs = require('fs');
const path = require('path');
const session = require('express-session');
const bcrypt = require('bcryptjs');
const Database = require('better-sqlite3');
const Stripe = require('stripe');

const app = express();
const PORT = process.env.PORT || 3000;

// --- Database Setup ---
const DATA_DIR = process.env.DATA_DIR || path.join(__dirname, 'data');
if (!fs.existsSync(DATA_DIR)) fs.mkdirSync(DATA_DIR, { recursive: true });

const db = new Database(path.join(DATA_DIR, 'users.db'));
db.pragma('journal_mode = WAL');

db.exec(`
  CREATE TABLE IF NOT EXISTS users (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    email TEXT UNIQUE NOT NULL,
    password TEXT NOT NULL,
    role TEXT DEFAULT 'user',
    stripe_customer_id TEXT,
    stripe_subscription_id TEXT,
    subscription_status TEXT DEFAULT 'none',
    trial_ends_at TEXT,
    created_at TEXT DEFAULT (datetime('now'))
  )
`);

// --- Seed Admin Accounts ---
const ADMINS = [
  { email: 'josephmadiganmusic@gmail.com', password: 'Bornagainbold123!' },
  { email: 'official.stevenperez@gmail.com', password: 'Bornagainbold123!' }
];

for (const admin of ADMINS) {
  const exists = db.prepare('SELECT id FROM users WHERE email = ?').get(admin.email);
  if (!exists) {
    const hash = bcrypt.hashSync(admin.password, 10);
    db.prepare('INSERT INTO users (email, password, role, subscription_status) VALUES (?, ?, ?, ?)').run(admin.email, hash, 'admin', 'admin');
  }
}

// --- Stripe Setup ---
const stripe = process.env.STRIPE_SECRET_KEY ? new Stripe(process.env.STRIPE_SECRET_KEY) : null;
const STRIPE_PRICE_ID = process.env.STRIPE_PRICE_ID || '';
const STRIPE_WEBHOOK_SECRET = process.env.STRIPE_WEBHOOK_SECRET || '';

// --- Middleware ---
app.use((req, res, next) => {
  if (req.path === '/webhook') return next(); // raw body for Stripe
  express.json()(req, res, next);
});
app.use(express.urlencoded({ extended: true }));

app.use(session({
  secret: process.env.SESSION_SECRET || 'mcc-secret-change-me-in-production',
  resave: false,
  saveUninitialized: false,
  cookie: {
    secure: process.env.NODE_ENV === 'production' && process.env.RAILWAY_ENVIRONMENT ? true : false,
    httpOnly: true,
    maxAge: 30 * 24 * 60 * 60 * 1000, // 30 days
    sameSite: 'lax'
  },
  proxy: true
}));

// Trust Railway proxy
app.set('trust proxy', 1);

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
  const user = db.prepare('SELECT * FROM users WHERE id = ?').get(req.session.userId);
  if (!user) { req.session.destroy(); return res.redirect('/login'); }
  req.user = user;
  next();
}

function requireAccess(req, res, next) {
  if (!req.session.userId) return res.redirect('/login');
  const user = db.prepare('SELECT * FROM users WHERE id = ?').get(req.session.userId);
  if (!user) { req.session.destroy(); return res.redirect('/login'); }
  req.user = user;
  if (!hasAccess(user)) return res.redirect('/subscribe');
  next();
}

// --- Auth Routes ---
app.get('/login', (req, res) => {
  if (req.session.userId) return res.redirect('/');
  res.sendFile(path.join(__dirname, 'login.html'));
});

app.post('/api/login', (req, res) => {
  const { email, password } = req.body;
  if (!email || !password) return res.status(400).json({ error: 'Email and password required' });

  const user = db.prepare('SELECT * FROM users WHERE email = ?').get(email.toLowerCase().trim());
  if (!user || !bcrypt.compareSync(password, user.password)) {
    return res.status(401).json({ error: 'Invalid email or password' });
  }

  req.session.userId = user.id;
  res.json({ success: true, hasAccess: hasAccess(user) });
});

app.post('/api/signup', (req, res) => {
  const { email, password } = req.body;
  if (!email || !password) return res.status(400).json({ error: 'Email and password required' });
  if (password.length < 8) return res.status(400).json({ error: 'Password must be at least 8 characters' });

  const cleanEmail = email.toLowerCase().trim();
  const existing = db.prepare('SELECT id FROM users WHERE email = ?').get(cleanEmail);
  if (existing) return res.status(409).json({ error: 'Account already exists. Please log in.' });

  const hash = bcrypt.hashSync(password, 10);
  const trialEnd = new Date(Date.now() + 7 * 24 * 60 * 60 * 1000).toISOString();

  const result = db.prepare(
    'INSERT INTO users (email, password, subscription_status, trial_ends_at) VALUES (?, ?, ?, ?)'
  ).run(cleanEmail, hash, 'trialing', trialEnd);

  req.session.userId = result.lastInsertRowid;
  res.json({ success: true, hasAccess: true, trial: true });
});

app.post('/api/logout', (req, res) => {
  req.session.destroy();
  res.json({ success: true });
});

app.get('/api/me', (req, res) => {
  if (!req.session.userId) return res.json({ loggedIn: false });
  const user = db.prepare('SELECT id, email, role, subscription_status, trial_ends_at FROM users WHERE id = ?').get(req.session.userId);
  if (!user) return res.json({ loggedIn: false });

  let daysLeft = null;
  if (user.subscription_status === 'trialing' && user.trial_ends_at) {
    daysLeft = Math.max(0, Math.ceil((new Date(user.trial_ends_at) - new Date()) / (1000 * 60 * 60 * 24)));
  }

  res.json({ loggedIn: true, ...user, hasAccess: hasAccess(user), trialDaysLeft: daysLeft });
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
      db.prepare('UPDATE users SET stripe_customer_id = ? WHERE id = ?').run(customerId, req.user.id);
    }

    const baseUrl = `${req.protocol}://${req.get('host')}`;
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

  const sub = event.data.object;

  switch (event.type) {
    case 'customer.subscription.created':
    case 'customer.subscription.updated': {
      const status = sub.status === 'trialing' ? 'trialing' : (sub.status === 'active' ? 'active' : sub.status);
      const trialEnd = sub.trial_end ? new Date(sub.trial_end * 1000).toISOString() : null;
      db.prepare('UPDATE users SET subscription_status = ?, stripe_subscription_id = ?, trial_ends_at = ? WHERE stripe_customer_id = ?')
        .run(status, sub.id, trialEnd, sub.customer);
      break;
    }
    case 'customer.subscription.deleted': {
      db.prepare('UPDATE users SET subscription_status = ?, stripe_subscription_id = NULL WHERE stripe_customer_id = ?')
        .run('canceled', sub.customer);
      break;
    }
  }

  res.sendStatus(200);
});

// --- Subscribe Page ---
app.get('/subscribe', requireAuth, (req, res) => {
  res.sendFile(path.join(__dirname, 'subscribe.html'));
});

// --- Research API (Spotify + Serper) ---
const SPOTIFY_CLIENT_ID = process.env.SPOTIFY_CLIENT_ID || '';
const SPOTIFY_CLIENT_SECRET = process.env.SPOTIFY_CLIENT_SECRET || '';
const SERPER_API_KEY = process.env.SERPER_API_KEY || '';

let spotifyToken = null;
let spotifyTokenExpires = 0;

async function getSpotifyToken() {
  if (spotifyToken && Date.now() < spotifyTokenExpires) return spotifyToken;
  if (!SPOTIFY_CLIENT_ID || !SPOTIFY_CLIENT_SECRET) return null;

  const resp = await fetch('https://accounts.spotify.com/api/token', {
    method: 'POST',
    headers: {
      'Authorization': 'Basic ' + Buffer.from(SPOTIFY_CLIENT_ID + ':' + SPOTIFY_CLIENT_SECRET).toString('base64'),
      'Content-Type': 'application/x-www-form-urlencoded'
    },
    body: 'grant_type=client_credentials'
  });
  if (!resp.ok) throw new Error('Spotify auth failed');
  const data = await resp.json();
  spotifyToken = data.access_token;
  spotifyTokenExpires = Date.now() + (data.expires_in - 60) * 1000;
  return spotifyToken;
}

async function spotifySearch(query, type, limit = 20) {
  const token = await getSpotifyToken();
  if (!token) throw new Error('Spotify not configured');
  const resp = await fetch(`https://api.spotify.com/v1/search?q=${encodeURIComponent(query)}&type=${type}&limit=${limit}`, {
    headers: { 'Authorization': 'Bearer ' + token }
  });
  if (!resp.ok) throw new Error('Spotify search failed');
  return resp.json();
}

async function spotifyGet(endpoint) {
  const token = await getSpotifyToken();
  if (!token) throw new Error('Spotify not configured');
  const resp = await fetch(`https://api.spotify.com/v1${endpoint}`, {
    headers: { 'Authorization': 'Bearer ' + token }
  });
  if (!resp.ok) throw new Error('Spotify request failed');
  return resp.json();
}

async function serperSearch(query, num = 10) {
  if (!SERPER_API_KEY) throw new Error('Search API not configured');
  const resp = await fetch('https://google.serper.dev/search', {
    method: 'POST',
    headers: { 'X-API-KEY': SERPER_API_KEY, 'Content-Type': 'application/json' },
    body: JSON.stringify({ q: query, num })
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
  // Evict old entries if cache grows too large
  if (researchCache.size > 200) {
    const now = Date.now();
    for (const [k, v] of researchCache) { if (now > v.expires) researchCache.delete(k); }
  }
}

app.post('/api/research', requireAccess, async (req, res) => {
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
        const queries = [genre, `${genre} independent`, `${genre} new music`];
        const allPlaylists = [];
        for (const q of queries) {
          const data = await spotifySearch(q, 'playlist', 15);
          if (data.playlists?.items) {
            for (const pl of data.playlists.items) {
              if (!pl || !pl.name) continue;
              allPlaylists.push({
                name: pl.name,
                description: (pl.description || '').substring(0, 200),
                followers: pl.tracks?.total || 0,
                trackCount: pl.tracks?.total || 0,
                owner: pl.owner?.display_name || 'Unknown',
                url: pl.external_urls?.spotify || '',
                image: pl.images?.[0]?.url || ''
              });
            }
          }
        }
        // Dedupe by URL
        const seen = new Set();
        result = { playlists: allPlaylists.filter(p => { if (seen.has(p.url)) return false; seen.add(p.url); return true; }) };
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
        const competitorData = [];
        for (const artist of similarArtists.slice(0, 5)) {
          const searchResult = await spotifySearch(artist, 'artist', 1);
          const found = searchResult.artists?.items?.[0];
          if (!found) continue;
          // Get their top tracks
          const topTracks = await spotifyGet(`/artists/${found.id}/top-tracks?market=US`);
          competitorData.push({
            name: found.name,
            followers: found.followers?.total || 0,
            popularity: found.popularity || 0,
            genres: found.genres || [],
            image: found.images?.[0]?.url || '',
            url: found.external_urls?.spotify || '',
            topTracks: (topTracks.tracks || []).slice(0, 5).map(t => ({
              name: t.name,
              popularity: t.popularity,
              album: t.album?.name || ''
            }))
          });
        }
        // Search for playlists featuring these artists
        const playlistHits = [];
        for (const artist of similarArtists.slice(0, 3)) {
          const plData = await spotifySearch(artist, 'playlist', 10);
          if (plData.playlists?.items) {
            for (const pl of plData.playlists.items) {
              if (!pl || !pl.name) continue;
              playlistHits.push({
                name: pl.name,
                owner: pl.owner?.display_name || 'Unknown',
                url: pl.external_urls?.spotify || '',
                trackCount: pl.tracks?.total || 0
              });
            }
          }
        }
        result = { competitors: competitorData, playlistsFeaturingCompetitors: playlistHits };
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
    spotify: !!(SPOTIFY_CLIENT_ID && SPOTIFY_CLIENT_SECRET),
    search: !!SERPER_API_KEY
  });
});

// --- Main App (protected) ---
app.get('/', requireAccess, (req, res) => {
  let html = fs.readFileSync(path.join(__dirname, 'MARKETING-COMMAND-CENTER.html'), 'utf8');
  html = html.replace('%%CLAUDE_API_KEY%%', process.env.CLAUDE_API_KEY || '');
  res.type('html').send(html);
});

// Static files (CSS, images, etc.)
app.use(express.static(__dirname, {
  index: false // don't serve index.html automatically
}));

app.listen(PORT, () => {
  console.log(`Rollout Heaven running on port ${PORT}`);
});
