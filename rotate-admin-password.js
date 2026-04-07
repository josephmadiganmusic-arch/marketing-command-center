// One-shot admin password rotation.
//
// Usage:
//   node rotate-admin-password.js 'new-strong-password'
//   node rotate-admin-password.js 'new-strong-password' josephmadiganmusic@gmail.com
//
// On Railway:
//   railway run node rotate-admin-password.js 'new-strong-password'
//
// With no email arg, rotates BOTH seeded admin accounts to the same password.
// Pass a specific email as the second arg to rotate just one.

const fs = require('fs');
const path = require('path');
const bcrypt = require('bcryptjs');
const initSqlJs = require('sql.js');

const ADMIN_EMAILS = [
  'josephmadiganmusic@gmail.com',
  'official.stevenperez@gmail.com'
];

async function main() {
  const newPassword = process.argv[2];
  const targetEmail = process.argv[3];

  if (!newPassword) {
    console.error('Usage: node rotate-admin-password.js <new-password> [email]');
    process.exit(1);
  }
  if (newPassword.length < 12) {
    console.error('Refusing: password must be at least 12 characters.');
    process.exit(1);
  }

  const DATA_DIR = process.env.DATA_DIR || path.join(__dirname, 'data');
  const DB_PATH = path.join(DATA_DIR, 'users.db');

  console.log('[ROTATE] DATA_DIR =', DATA_DIR);
  console.log('[ROTATE] DB_PATH  =', DB_PATH);

  if (!fs.existsSync(DB_PATH)) {
    console.error('[ROTATE] FATAL: DB file not found at', DB_PATH);
    console.error('[ROTATE] Check that DATA_DIR matches your Railway volume mount (should be /app/data).');
    process.exit(1);
  }

  console.log('[ROTATE] Loading sql.js...');
  const SQL = await initSqlJs();
  const fileBuffer = fs.readFileSync(DB_PATH);
  const db = new SQL.Database(fileBuffer);

  // Sanity check
  try {
    db.exec('SELECT count(*) FROM users');
  } catch (e) {
    console.error('[ROTATE] FATAL: users table not found. Is this the right DB?');
    process.exit(1);
  }

  const emailsToRotate = targetEmail ? [targetEmail.toLowerCase().trim()] : ADMIN_EMAILS;
  const hash = bcrypt.hashSync(newPassword, 10);

  let updated = 0;
  for (const email of emailsToRotate) {
    // Look up first so we can verify the row exists AND is an admin.
    const stmt = db.prepare('SELECT id, role FROM users WHERE email = ?');
    stmt.bind([email]);
    if (!stmt.step()) {
      stmt.free();
      console.warn('[ROTATE] SKIP — no user row for', email);
      continue;
    }
    const row = stmt.getAsObject();
    stmt.free();

    if (row.role !== 'admin') {
      console.warn('[ROTATE] SKIP —', email, 'exists but role is', row.role, '(refusing to touch non-admin)');
      continue;
    }

    db.run('UPDATE users SET password = ? WHERE id = ?', [hash, row.id]);
    console.log('[ROTATE] OK   —', email, '(id ' + row.id + ')');
    updated++;
  }

  if (updated === 0) {
    console.error('[ROTATE] No rows updated. Aborting without writing DB.');
    process.exit(1);
  }

  // Atomic write: tmp file + rename, same pattern as server.js saveDb()
  console.log('[ROTATE] Writing DB...');
  const data = db.export();
  const tmpPath = DB_PATH + '.tmp';
  fs.writeFileSync(tmpPath, Buffer.from(data));
  fs.renameSync(tmpPath, DB_PATH);
  db.close();

  console.log('[ROTATE] Done. Rotated', updated, 'admin password(s).');
  console.log('[ROTATE] Test the new password on the live site, then DELETE this script');
  console.log('[ROTATE] (or at least make sure it never gets called accidentally).');
}

main().catch(err => {
  console.error('[ROTATE] FATAL:', err);
  process.exit(1);
});
