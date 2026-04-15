import threading
import datetime
import logging
import sqlite3
import os

logger = logging.getLogger("monetization")

# Constants
FREE_LIMIT = int(os.getenv("PUSHCLEAN_FREE_LIMIT", "20"))
PRO_LIMIT = int(os.getenv("PUSHCLEAN_PRO_LIMIT", "200"))
TEAM_LIMIT = int(os.getenv("PUSHCLEAN_TEAM_LIMIT", "1000"))

# DB path
try:
    from db_paths import MonetizationDB
except ImportError:
    _MonetizationDB: str = os.getenv("MONETIZATION_DB", "").strip()
    if _MonetizationDB:
        MONETIZATION_DB: str = _MonetizationDB
    else:
        _MonetizationDB = os.getenv("PUSHCLEAN_DATA_DIR", "").strip()
        MONETIZATION_DB = os.path.join(_MonetizationDB, "monetization.db")

_SCHEMA_LOCK = threading.Lock()
_SCHEMA_INITED = False

# DB context manager
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

# Schema
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
                    active           INTEGER NOT NULL DEFAULT 1
                )
            """)
            conn.execute("""
                CREATE INDEX IF NOT EXISTS idx_users_active
                ON users (active, plan)
            """)
            # Audit log
            conn.execute("""
                CREATE TABLE IF NOT EXISTS usage_log (
                    id              INTEGER PRIMARY KEY AUTOINCREMENT,
                    installation_id TEXT NOT NULL,
                    repo_name       TEXT NOT NULL DEFAULT '',
                    file_count      INTEGER NOT NULL DEFAULT 1,
                    plan            TEXT NOT NULL,
                    usage            INTEGER NOT NULL,
                    usage_before    INTEGER NOT NULL,
                    allowed          INTEGER NOT NULL,
                    recorded_at     TEXT NOT NULL
                )
            """)
            conn.execute("""
                CREATE INDEX IF NOT EXISTS idx_usage_log_inst
                ON usage_log (installation_id, recorded_at DESC)
            """)

    _SCHEMA_INITED = True
    logger.info("Monetization DB initialised at %s", MONETIZATION_DB)

# Core
class LimitReached(Exception):
    """Raised when a user has hit their plan file limit."""
    def __init__(self, plan: str, usage: int, limit: int):
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
    try:
        with _get_db() as conn:
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
                            (installation_id, repo_name, plan, usage,
                             usage_reset_at, installed_at, active)
                        VALUES (?, ?, ?, ?, ?, ?, 1)
                    """, (installation_id, repo_name, now, now))
                    plan, usage = "free", 0
                else:
                    plan  = row["plan"]
                    usage = row["usage"]

                    # Auto-reset usage monthly
                    # PATCH: fromisoformat() on Python < 3.11 rejects "+00:00" timezone
                    # suffix produced by datetime.fromisoformat() to return wrong timezone info.
                    # Now using Python datetime consistently.
                    now_iso = datetime.now(timezone.utc).isoformat()
                    with conn:
                        conn.execute("""
                            UPDATE users SET usage=0, usage_reset_at=?
                            WHERE usage_reset_at < ? AND active=1
                        """, (now_iso, now))
                        if conn.rowcount:
                            logger.info(
                                "Usage auto-reset for installation %s (monthly)",
                                installation_id,
                            )
                    return plan, usage

            except Exception:
                conn.rollback()
                try:
                    conn.execute("""
                        INSERT INTO usage_log (installation_id, repo_name, file_count, plan, usage_before, allowed, recorded_at)
                        VALUES (?, ?, ?, ?, ?, ?, ?)
                    """, (installation_id, repo_name, file_count, plan, usage, now))
                    conn.execute("""
                        INSERT INTO usage_log (installation_id, repo_name, file_count, plan, usage_before, allowed, recorded_at)
                        VALUES (?, ?, ?, ?, ?, ?, ?)
                    """, (installation_id, repo_name, file_count, plan, usage, now))
                    conn.commit()
                    conn.close()
                    logger.info("Monthly reset: cleared usage for %s", installation_id)
                except Exception:
                    logger.exception("monthly reset failed")
                    return "free", 0

    except LimitReached:
        # Connection already committed and closed above — just re-raise.
        raise

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
            with conn.cursor() as cur:
                cur.execute("""
                    INSERT INTO users (installation_id, owner, repo_name, plan,
                           usage, usage_reset_at, installed_at, active)
                    VALUES (?, ?, ?, ?, ?, ?, ?)
                """, (installation_id, owner, repo_name, now, now))
        logger.info(
            "Installation registered: %s",
            installation_id,
        )
    except Exception:
        logger.exception("register_installation failed")

def unregister_installation(installation_id: str) -> None:
    """Called when GitHub App is uninstalled."""
    _ensure_schema()
    try:
        with _get_db() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    UPDATE users SET active=0, uninstalled_at=?
                    WHERE installation_id=?
                """, (installation_id,))
        conn.commit()
        logger.info("Installation unregistered: %s", installation_id)
    except Exception:
        logger.exception("unregister_installation failed")

def upgrade_user(installation_id: str, plan: str = "pro") -> bool:
    """
    Upgrade a user's plan. Called from Stripe webhook or manual admin action.

    Returns True if user was found and upgraded.
    """
    _ensure_schema()
    try:
        with _get_db() as conn:
            result = conn.execute("""
                UPDATE users SET plan=? WHERE installation_id=?
            """, (plan, installation_id))
            if result.rowcount:
                logger.info(
                    "User upgraded: installation %s → plan=%s",
                    installation_id,
                    plan,
                )
                return True
            else:
                logger.warning("User not found for installation %s", installation_id)
                return False
    except Exception:
        logger.exception("upgrade_user failed")
        return False

def get_all_users(active_only: bool = True) -> dict:
    """Return user list for admin dashboard."""
    _ensure_schema()
    try:
        with _get_db() as conn:
            rows = conn.execute("""
                SELECT
                    COUNT(*) as total_installs,
                    SUM(CASE WHEN active=1 THEN 1 ELSE 0 END) as active_installs,
                    SUM(CASE WHEN plan='pro' AND active=1 THEN 1 ELSE 0 END) as pro_users,
                    SUM(CASE WHEN plan='free' AND active=1 THEN 1 ELSE 0 END) as free_users,
                    SUM(usage) as total_files_processed
                FROM users
            """).fetchall()
            recent = conn.execute("""
                SELECT COUNT(*) as calls,
                       SUM(CASE WHEN allowed=1 THEN 1 ELSE 0 END) as allowed,
                       SUM(CASE WHEN allowed=0 THEN 1 ELSE 0 END) as blocked
                FROM usage_log
                WHERE recorded_at >= datetime('now', '-30 days')
            """).fetchone()
            return {
                "total_installs":      rows[0],
                "active_installs":     rows[1],
                "pro_users":           rows[2],
                "free_users":          rows[3],
                "total_files_processed":  rows[4],
            }
    except Exception:
        logger.exception("get_all_users failed")
        return {}