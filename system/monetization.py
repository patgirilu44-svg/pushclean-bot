"""
monetization.py — PushClean SaaS Layer
======================================
Thread-safe user tracking and plan enforcement.

Key design decisions vs naive implementation:
- check_and_increment() is ATOMIC — one DB transaction checks + increments.
  Naive: enforce_limit() + increment_usage() separate → two parallel requests
  both pass the limit before either increments (classic TOCTOU race).
- WAL mode + busy_timeout — same pattern as rest of PushClean stack.
- Monthly reset runs in a background thread — no external cron needed.
- plan_type stored in DB — upgrade_user() is the single write path.

Plans:
  free  → FREE_LIMIT  files/month
  pro   → PRO_LIMIT   files/month
  team  → TEAM_LIMIT  files/month (future)
"""

from __future__ import annotations

import logging
import threading
import time
from contextlib import contextmanager
from datetime import datetime, timezone, timedelta
from typing import Optional
import sqlite3
import os

logger = logging.getLogger("monetization")

# ── Constants ─────────────────────────────────────────────────────────────────
FREE_LIMIT = int(os.getenv("PUSHCLEAN_FREE_LIMIT",  "20"))
PRO_LIMIT  = int(os.getenv("PUSHCLEAN_PRO_LIMIT",  "200"))
TEAM_LIMIT = int(os.getenv("PUSHCLEAN_TEAM_LIMIT", "1000"))

PLAN_LIMITS: dict[str, int] = {
    "free": FREE_LIMIT,
    "pro":  PRO_LIMIT,
    "team": TEAM_LIMIT,
}

# DB path — single source of truth via db_paths.py (respects PUSHCLEAN_DATA_DIR for Render persistence)
try:
    from db_paths import MONETIZATION_DB
except ImportError:
    # Fallback: resolve inline if db_paths.py not present
    _MONETIZATION_DB_OVERRIDE = os.getenv("MONETIZATION_DB", "").strip()
    if _MONETIZATION_DB_OVERRIDE:
        MONETIZATION_DB: str = _MONETIZATION_DB_OVERRIDE
    else:
        _d = os.getenv("PUSHCLEAN_DATA_DIR", "").strip()
        MONETIZATION_DB = os.path.join(_d, "monetization.db") if _d else "monetization.db"

_SCHEMA_LOCK    = threading.Lock()
_SCHEMA_INITED  = False


# ── DB context manager ────────────────────────────────────────────────────────

@contextmanager
def _get_db():
    conn = sqlite3.connect(MONETIZATION_DB, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA busy_timeout=5000")
    try:
        yield conn
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


# ── Schema ────────────────────────────────────────────────────────────────────

def _ensure_schema() -> None:
    global _SCHEMA_INITED
    with _SCHEMA_LOCK:
        if _SCHEMA_INITED:
            return
        with _get_db() as conn:
            conn.execute("""
                CREATE TABLE IF NOT EXISTS users (
                    installation_id  TEXT PRIMARY KEY,
                    owner            TEXT NOT NULL DEFAULT '',
                    repo_name        TEXT NOT NULL DEFAULT '',
                    plan             TEXT NOT NULL DEFAULT 'free',
                    usage            INTEGER NOT NULL DEFAULT 0,
                    usage_reset_at   TEXT NOT NULL,
                    installed_at     TEXT NOT NULL,
                    uninstalled_at   TEXT,
                    active           INTEGER NOT NULL DEFAULT 1
                )
            """)
            conn.execute("""
                CREATE INDEX IF NOT EXISTS idx_users_active
                ON users (active, plan)
            """)
            # Audit log — every limit decision recorded
            conn.execute("""
                CREATE TABLE IF NOT EXISTS usage_log (
                    id              INTEGER PRIMARY KEY AUTOINCREMENT,
                    installation_id TEXT NOT NULL,
                    repo_name       TEXT NOT NULL DEFAULT '',
                    file_count      INTEGER NOT NULL DEFAULT 1,
                    plan            TEXT NOT NULL,
                    usage_before    INTEGER NOT NULL,
                    allowed         INTEGER NOT NULL,
                    recorded_at     TEXT NOT NULL
                )
            """)
            conn.execute("""
                CREATE INDEX IF NOT EXISTS idx_usage_log_inst
                ON usage_log (installation_id, recorded_at DESC)
            """)
        _SCHEMA_INITED = True
        logger.info("Monetization DB initialised at %s", MONETIZATION_DB)


# ── Core: atomic check + increment ───────────────────────────────────────────

class LimitReached(Exception):
    """Raised when a user has hit their plan file limit."""
    def __init__(self, plan: str, usage: int, limit: int) -> None:
        self.plan  = plan
        self.usage = usage
        self.limit = limit
        super().__init__(
            f"Plan '{plan}' limit reached ({usage}/{limit} files this month). "
            f"Upgrade at https://github.com/marketplace/pushclean"
        )


def check_and_increment(
    installation_id: str,
    repo_name:       str = "",
    file_count:      int = 1,
) -> tuple[str, int]:
    """
    ATOMIC: check plan limit + increment usage in a single transaction.

    Returns (plan, new_usage) on success.
    Raises LimitReached if the user is at or over their limit.

    WHY ATOMIC:
    Two concurrent webhook deliveries for the same installation would both
    read usage=19 (below limit=20), both increment to 20 — total 21 files
    processed for a 20-file limit. Atomic transaction prevents this.
    """
    _ensure_schema()
    now = datetime.now(timezone.utc).isoformat()

    try:
        # Use BEGIN IMMEDIATE so no two threads can race on the same row
        conn = sqlite3.connect(MONETIZATION_DB, check_same_thread=False)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA busy_timeout=5000")
        try:
            conn.execute("BEGIN IMMEDIATE")

            row = conn.execute(
                "SELECT plan, usage, usage_reset_at FROM users WHERE installation_id=?",
                (installation_id,),
            ).fetchone()

            if not row:
                # First-time user — auto-register
                conn.execute("""
                    INSERT INTO users
                        (installation_id, repo_name, plan, usage, usage_reset_at, installed_at, active)
                    VALUES (?, ?, 'free', 0, ?, ?, 1)
                """, (installation_id, repo_name, now, now))
                plan, usage = "free", 0
            else:
                plan  = row["plan"]
                usage = row["usage"]

                # Auto-reset usage monthly
                # PATCH: fromisoformat() on Python < 3.11 rejects "+00:00" timezone
                # suffix produced by datetime.now(timezone.utc).isoformat().
                # Normalize by stripping the suffix before parsing.
                raw_reset = row["usage_reset_at"]
                try:
                    reset_at = datetime.fromisoformat(raw_reset)
                except ValueError:
                    # Strip timezone suffix and re-attach UTC explicitly
                    reset_at = datetime.fromisoformat(
                        raw_reset[:19]
                    ).replace(tzinfo=timezone.utc)
                if datetime.now(timezone.utc) - reset_at >= timedelta(days=30):
                    conn.execute(
                        "UPDATE users SET usage=0, usage_reset_at=? WHERE installation_id=?",
                        (now, installation_id),
                    )
                    usage = 0
                    logger.info(
                        "Usage auto-reset for installation %s (monthly)",
                        installation_id[:12],
                    )

            limit   = PLAN_LIMITS.get(plan, FREE_LIMIT)
            allowed = (usage + file_count) <= limit

            # Audit log entry
            conn.execute("""
                INSERT INTO usage_log
                    (installation_id, repo_name, file_count, plan, usage_before, allowed, recorded_at)
                VALUES (?, ?, ?, ?, ?, ?, ?)
            """, (installation_id, repo_name, file_count, plan, usage, 1 if allowed else 0, now))

            if not allowed:
                # Commit audit log, then raise — do NOT close before raise
                # so the inner except-LimitReached can re-raise cleanly.
                conn.commit()
                conn.close()
                raise LimitReached(plan, usage, limit)

            new_usage = usage + file_count
            conn.execute(
                "UPDATE users SET usage=? WHERE installation_id=?",
                (new_usage, installation_id),
            )
            conn.commit()
            conn.close()

            logger.info(
                "Usage: installation=%s plan=%s %d/%d files this month",
                installation_id[:12], plan, new_usage, limit,
            )
            return plan, new_usage

        except LimitReached:
            # Connection already committed and closed above — just re-raise.
            raise
        except Exception:
            # Real DB error — attempt cleanup, never crash caller.
            try:
                conn.rollback()
            except Exception:
                pass
            try:
                conn.close()
            except Exception:
                pass
            raise

    except LimitReached:
        raise
    except Exception:
        logger.exception("check_and_increment failed for %s", installation_id[:12])
        # Fail open on DB errors — don't block users because our DB has a problem
        return "free", 0


# ── Installation lifecycle ────────────────────────────────────────────────────

def register_installation(
    installation_id: str,
    owner:           str,
    repo_name:       str = "",
) -> None:
    """Called when GitHub App is installed on an account/repo."""
    _ensure_schema()
    now = datetime.now(timezone.utc).isoformat()
    try:
        with _get_db() as conn:
            conn.execute("""
                INSERT INTO users
                    (installation_id, owner, repo_name, plan, usage,
                     usage_reset_at, installed_at, active)
                VALUES (?, ?, ?, 'free', 0, ?, ?, 1)
                ON CONFLICT(installation_id) DO UPDATE SET
                    owner          = excluded.owner,
                    repo_name      = excluded.repo_name,
                    active         = 1,
                    uninstalled_at = NULL
            """, (installation_id, owner, repo_name, now, now))
        logger.info(
            "Installation registered: %s (owner=%s)", installation_id[:12], owner
        )
    except Exception:
        logger.exception("register_installation failed")


def unregister_installation(installation_id: str) -> None:
    """Called when GitHub App is uninstalled."""
    _ensure_schema()
    try:
        with _get_db() as conn:
            conn.execute("""
                UPDATE users SET active=0, uninstalled_at=?
                WHERE installation_id=?
            """, (datetime.now(timezone.utc).isoformat(), installation_id))
        logger.info("Installation unregistered: %s", installation_id[:12])
    except Exception:
        logger.exception("unregister_installation failed")


def upgrade_user(installation_id: str, plan: str = "pro") -> bool:
    """
    Upgrade a user's plan. Called from Stripe webhook or manual admin action.
    Returns True if user was found and upgraded.
    """
    _ensure_schema()
    if plan not in PLAN_LIMITS:
        logger.warning("upgrade_user: unknown plan %r — rejected", plan)
        return False
    try:
        with _get_db() as conn:
            result = conn.execute(
                "UPDATE users SET plan=? WHERE installation_id=?",
                (plan, installation_id),
            )
            # FIX: check rowcount INSIDE the with block — cursor is still live here.
            # Checking after context exit works in practice (SQLite retains rowcount)
            # but is fragile and misleading.
            if result.rowcount == 0:
                logger.warning("upgrade_user: installation %s not found", installation_id[:12])
                return False
        logger.info(
            "User upgraded: installation=%s → plan=%s", installation_id[:12], plan
        )
        return True
    except Exception:
        logger.exception("upgrade_user failed")
        return False


# ── Stats ─────────────────────────────────────────────────────────────────────

def get_all_users(active_only: bool = True) -> list[dict]:
    """Return user list for admin dashboard."""
    _ensure_schema()
    try:
        with _get_db() as conn:
            # FIX: f-string SQL replaced with explicit queries — no SQL injection
            # risk here (where came from hardcoded conditional, not user input),
            # but explicit queries are clearer and easier to audit.
            if active_only:
                rows = conn.execute("""
                    SELECT installation_id, owner, repo_name, plan,
                           usage, usage_reset_at, installed_at, active
                    FROM users
                    WHERE active=1
                    ORDER BY installed_at DESC
                """).fetchall()
            else:
                rows = conn.execute("""
                    SELECT installation_id, owner, repo_name, plan,
                           usage, usage_reset_at, installed_at, active
                    FROM users
                    ORDER BY installed_at DESC
                """).fetchall()
        return [dict(r) for r in rows]
    except Exception:
        logger.exception("get_all_users failed")
        return []


def get_monetization_stats() -> dict:
    """Aggregate stats for admin /stats endpoint."""
    _ensure_schema()
    try:
        with _get_db() as conn:
            totals = conn.execute("""
                SELECT
                    COUNT(*)                                  as total_installs,
                    SUM(CASE WHEN active=1 THEN 1 ELSE 0 END) as active_installs,
                    SUM(CASE WHEN plan='pro'  AND active=1 THEN 1 ELSE 0 END) as pro_users,
                    SUM(CASE WHEN plan='free' AND active=1 THEN 1 ELSE 0 END) as free_users,
                    SUM(usage)                                as total_files_processed
                FROM users
            """).fetchone()
            recent = conn.execute("""
                SELECT COUNT(*) as calls,
                       SUM(CASE WHEN allowed=1 THEN 1 ELSE 0 END) as allowed,
                       SUM(CASE WHEN allowed=0 THEN 1 ELSE 0 END) as blocked
                FROM usage_log
                WHERE recorded_at >= datetime('now', '-30 days')
            """).fetchone()
        return {
            "total_installs":      totals["total_installs"] or 0,
            "active_installs":     totals["active_installs"] or 0,
            "pro_users":           totals["pro_users"] or 0,
            "free_users":          totals["free_users"] or 0,
            "total_files_ever":    totals["total_files_processed"] or 0,
            "last_30d": {
                "api_calls": recent["calls"] or 0,
                "allowed":   recent["allowed"] or 0,
                "blocked":   recent["blocked"] or 0,
            },
            "plans": {k: f"{v} files/month" for k, v in PLAN_LIMITS.items()},
        }
    except Exception:
        logger.exception("get_monetization_stats failed")
        return {}


# ── Monthly reset background thread ──────────────────────────────────────────
# No external cron needed. Each request auto-resets if >30 days elapsed.
# This thread is a belt-and-suspenders cleanup for inactive users.

def _monthly_reset_worker() -> None:
    """Background thread: reset usage for users whose 30-day window has elapsed."""
    while True:
        try:
            time.sleep(86_400)  # run daily
            cutoff = (datetime.now(timezone.utc) - timedelta(days=30)).isoformat()
            # FIX: was usage_reset_at=CURRENT_TIMESTAMP (SQLite format: "YYYY-MM-DD HH:MM:SS")
            # Rest of codebase stores ISO 8601: "2024-01-01T00:00:00+00:00"
            # Format mismatch causes datetime.fromisoformat() to return wrong timezone info.
            # Now using Python datetime consistently.
            now_iso = datetime.now(timezone.utc).isoformat()
            with _get_db() as conn:
                result = conn.execute("""
                    UPDATE users SET usage=0, usage_reset_at=?
                    WHERE usage_reset_at < ? AND active=1
                """, (now_iso, cutoff))
                if result.rowcount:  # FIX: check inside with block — cursor live here
                    logger.info(
                        "Monthly reset: cleared usage for %d installation(s)",
                        result.rowcount,
                    )
        except Exception:
            logger.exception("Monthly reset worker error — will retry tomorrow")


def start_monthly_reset_thread() -> None:
    """Start background monthly-reset thread. Call once at startup."""
    t = threading.Thread(target=_monthly_reset_worker, daemon=True, name="monthly_reset")
    t.start()
    logger.debug("Monthly reset background thread started")
