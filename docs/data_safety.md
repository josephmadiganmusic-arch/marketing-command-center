# Data Safety — Rollout Heaven

**Last updated:** 2026-04-13

This document explains how user data is protected, persisted, recovered, and verified in the Rollout Heaven marketing command center.

---

## Persistence Model

**Database:** sql.js (pure JavaScript SQLite). The full database is held in-memory and flushed to `DATA_DIR/users.db` on every write via an atomic write pattern:

1. `db.export()` serializes the full DB to a byte buffer
2. Buffer is written to `users.db.tmp`
3. `fs.renameSync(tmp, users.db)` atomically replaces the file
4. **Post-write integrity check:** file size is verified to match the exported buffer length. On mismatch, a recovery write is attempted immediately.

**Critical writes** (signup, email verification, user data save, Stripe webhooks, admin actions) call `flushDbNow()` synchronously — they do not rely on the 100ms debounce.

**Graceful shutdown:** SIGTERM and SIGINT handlers flush pending writes before `process.exit(0)`. The `uncaughtException` handler also flushes before exiting.

---

## Backup Strategy

| Type | Frequency | Retention | Storage |
|------|-----------|-----------|---------|
| Hourly | Every 60 min (first at boot+10min) | 48 most recent | `DATA_DIR/backups/users.db.hourly.*.bak` |
| Daily | UTC midnight | 30 most recent | `DATA_DIR/backups/users.db.daily.*.bak` |

**Corrupt DB recovery at boot:** If the main database fails a sanity check (`SELECT count(*) FROM sqlite_master`), the system:
1. Moves the corrupt file aside as `users.db.corrupt.{timestamp}`
2. Tries each backup from newest to oldest until one loads
3. Only creates a fresh empty DB if ALL backups fail
4. Logs the recovery loudly so the operator sees it in Railway

---

## Soft Delete Policy

User accounts are **never hard-deleted**. When an admin deletes a user:

1. A full JSON snapshot of all user data (across all tables) is saved to the `deleted_users_archive` table
2. The user and all related rows are **soft-deleted** (`deleted_at` timestamp set)
3. Soft-deleted users cannot log in, appear in admin lists, or access any endpoints
4. **Exception:** Encrypted Elite credentials (`elite_onboarding`) are hard-deleted after archiving — retaining encrypted creds post-account-deletion is a security liability

**Tables with soft delete support:**
- `users`
- `user_data`
- `user_xp`
- `user_achievements`
- `xp_log`
- `support_tickets`
- `api_usage`

All user-facing queries include `WHERE deleted_at IS NULL`.

---

## Operation Journal

Every critical mutation is logged to the `operation_journal` table (append-only, never deleted or truncated):

| Action | What's logged |
|--------|--------------|
| `user.signup` | New account creation |
| `user.soft_delete` | Admin soft-deletes a user |
| `stripe.webhook` | Every processed Stripe event |
| `elite.onboarding_submit` | Credential form submission |
| `elite.credentials_viewed` | Admin decrypts stored creds |
| `admin.ticket_update` | Support ticket status/notes change |
| `admin.redemption_update` | Redemption request status change |
| `admin.outreach_publish` | New outreach list version published |
| `redemption.created` | New Redemption Release request |

Each entry records: timestamp, actor (user ID + email), action, entity type/ID, detail JSON, IP address.

Admin can view the journal at `GET /api/admin/journal?limit=100&offset=0`.

---

## Recovery Manifest

At every boot, a `recovery_manifest.json` is written to `DATA_DIR` containing:
- Row counts for all tables
- Latest hourly and daily backup filenames
- Backup counts
- Boot timestamp

On subsequent boots, the manifest is compared against the previous one. If any table unexpectedly drops to 0 rows (when it previously had data), a `[BOOT] WARNING` is logged — this is the early warning system for silent data loss.

---

## Version Tracking

- `user_data` rows have a `version` field that auto-increments on every save (enables future conflict detection for multi-tab writes)
- `users`, `user_xp`, `redemption_requests`, `submission_progress` have `updated_at` timestamps set on every UPDATE

---

## What Survives Each Failure Mode

| Failure | Data preserved? | How |
|---------|----------------|-----|
| App restart | Yes | DB flushed to disk on shutdown |
| Server crash (SIGKILL/OOM) | Up to 100ms of non-critical writes may be lost | Critical writes flush immediately; backups cover the rest |
| Deployment interruption | Yes | Railway zero-downtime deploy + single-instance lock |
| Partial writes | Yes | Atomic tmp+rename pattern |
| Duplicate Stripe webhooks | Yes | `stripe_events` idempotency table |
| Accidental admin user delete | Recoverable | Full snapshot in `deleted_users_archive` + soft delete |
| Database corruption | Recoverable | Auto-restore from newest valid backup at boot |

---

## Recovery Procedures

### Restoring a soft-deleted user
Query `deleted_users_archive` for the user's snapshot, then re-insert the data. The snapshot contains the full state of every related table at deletion time.

### Restoring from backup
1. Stop the server
2. Copy the desired backup from `DATA_DIR/backups/` to `DATA_DIR/users.db`
3. Restart — the server will load the restored file

### Investigating data loss
1. Check `recovery_manifest.json` for row count anomalies
2. Check `operation_journal` for recent destructive actions
3. Check Railway logs for `[FLUSH] CRITICAL`, `[BOOT] WARNING`, or `[RESTORE]` messages
4. Compare backups using `sqlite3 users.db.*.bak "SELECT count(*) FROM users"` (or equivalent sql.js script)

---

## Stripe Idempotency

Every processed Stripe webhook event ID is stored in `stripe_events`. On duplicate delivery, the handler returns early without re-processing. If the handler crashes mid-processing, the event row is rolled back so Stripe's retry succeeds.

---

## Single-Instance Enforcement

sql.js and the session file store assume a single writer. A file lock at `DATA_DIR/.instance.lock` prevents multi-replica boot. The lock includes a heartbeat and stale detection (90s) to handle Railway zero-downtime deploys where old and new instances briefly overlap.
