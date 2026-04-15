"""
Webhook Server — GitHub Push Event Receiver — HARDENED v2
=========================================================
HARDENING CHANGES vs v1:
- _claim_inflight_db: removed redundant CREATE TABLE on every call
  (table guaranteed by init_delivery_db at startup — was a schema check
   on every single hash operation, potentially hundreds per second)
- recover_stale_jobs: now actually calls _cleanup_stale_inflight()
  (docstring promised this; code never did — stale hashes leaked on crash)
- _worker_run_webhook: marks job as 'running' BEFORE starting orchestrator
  (v1: direct-submit jobs stayed 'pending' during execution — server crash
   caused duplicate processing since recover_stale_jobs only reset 'running')
- /webhook: body size cap added (10MB) — closes DoS vector
- cleanup_old_deliveries: dual-trigger (counter AND time-based)
  (v1 counter-only trigger resets on restart → old records accumulate
   indefinitely under low traffic or frequent rolling deploys)
- _get_cost_optimizer_stats: escalation HAVING raised to MIN_SAMPLES=10
  (consistent with cost_optimizer v8 — v1 used 2, inflating dashboard count)
- STALE_JOB_HOURS: now configurable via env var (was hardcoded 2h —
  legitimate long jobs incorrectly recovered and re-run)
- Rate limiting: in-memory enforcement explicitly documented with comment
  explaining trade-off vs DB enforcement

FIX v2.1 (import chain):
- Cost optimizer import now tries cost_optimizer_v9 before v6
  (v2 tried 'optimized_cost' then 'cost_optimizer_v6' — neither file exists;
   HAS_COST_OPTIMIZER was always False, silently disabling all cost routing)
- db_paths.py now used for DELIVERY_DB (single source of truth)

Environment variables:
    GITHUB_WEBHOOK_SECRET   — Shared secret from GitHub (REQUIRED in production)
    ADMIN_API_KEY           — Protects the /run and /stats endpoints
    WATCHED_BRANCH          — Only pushes to this branch trigger a run (default: main)
    PORT                    — Server port (default: 8000)
    WEBHOOK_WORKERS         — Number of parallel worker threads (default: 3, max: 10)
    RATE_LIMIT_WINDOW_SEC   — Rate limit window in seconds (default: 60)
    RATE_LIMIT_MAX_CALLS    — Max webhook calls per repo per window (default: 5)
    MAX_JOB_RETRIES         — Max retry attempts per failed job (default: 3)
    STALE_JOB_HOURS         — Hours before running job is considered stale (default: 2)
    MAX_WEBHOOK_BODY_BYTES  — Max webhook payload size in bytes (default: 10485760 = 10MB)
    PROD_MODE               — Set to "1" to enforce strict secret validation
"""

from __future__ import annotations

import atexit
import hmac
import hashlib
import json
import logging
import os
import sqlite3
import threading
import time
import uuid
from concurrent.futures import ThreadPoolExecutor, Future
from contextlib import asynccontextmanager, contextmanager
from datetime import datetime, timezone, timedelta
from typing import Optional

# ── Logging ───────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger("webhook_server")

# ── FastAPI import guard ──────────────────────────────────────────────────────
try:
    from fastapi import FastAPI, Request, HTTPException, BackgroundTasks, Depends
    from fastapi.responses import JSONResponse
    from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
    import uvicorn
    HAS_FASTAPI = True
except ImportError:
    HAS_FASTAPI = False
    log.error("FastAPI not installed. Run: pip install fastapi uvicorn")

# ── Orchestrator import guard ─────────────────────────────────────────────────
try:
    from orchestrator import (
        run_webhook, MasterOrchestrator, SmartAPICaller,
        SELF_LEARNING_DB, COST_OPTIMIZER_DB, get_db,
    )
    from pushclean_brain import PushCleanBrain, BRAIN_DB
except ImportError as _exc:
    log.critical("Failed to import orchestrator: %s", _exc)
    raise SystemExit(1) from _exc

# ── Cost Optimizer import (optional) ─────────────────────────────────────────
# FIX v2.1: Import chain now correctly tries cost_optimizer_v9.
# v2 tried 'optimized_cost' then 'cost_optimizer_v6' — neither file exists.
# HAS_COST_OPTIMIZER was always False, silently disabling all cost routing logic.
try:
    from optimized_cost import LearningLog as CostLearningLog
    HAS_COST_OPTIMIZER = True
except ImportError:
    try:
        from cost_optimizer_v9 import LearningLog as CostLearningLog  # ← FIXED
        HAS_COST_OPTIMIZER = True
        log.info("Cost optimizer loaded: cost_optimizer_v9")
    except ImportError:
        try:
            from cost_optimizer_v6 import LearningLog as CostLearningLog
            HAS_COST_OPTIMIZER = True
            log.info("Cost optimizer loaded: cost_optimizer_v6 (legacy)")
        except ImportError:
            HAS_COST_OPTIMIZER = False
            log.warning("No cost optimizer found — routing intelligence disabled")

# ── Monetization + GitHub commenter ──────────────────────────────────────────
try:
    from monetization import (
        check_and_increment, register_installation, unregister_installation,
        upgrade_user, get_all_users, get_monetization_stats,
        start_monthly_reset_thread, LimitReached,
    )
    from github_commenter import post_refinement_comment, create_refinement_pr
    HAS_MONETIZATION = True
except ImportError:
    HAS_MONETIZATION = False
    log.warning("monetization.py not found — usage limits disabled")

# ── DB paths (single source of truth via db_paths.py) ────────────────────────
# FIX v2.1: DELIVERY_DB now sourced from db_paths.py instead of inline os.getenv.
# All 5 DB paths are now controlled from one place — change db_paths.py to move them all.
try:
    from db_paths import DELIVERY_DB, validate_data_dir
    _data_dir_ok = validate_data_dir()
    if not _data_dir_ok:
        log.warning("db_paths.validate_data_dir() failed — check PUSHCLEAN_DATA_DIR")
except ImportError:
    # Fallback: inline resolution if db_paths.py is not present
    log.warning("db_paths.py not found — falling back to inline DELIVERY_DB resolution")
    DELIVERY_DB = (
        os.path.join(os.getenv("PUSHCLEAN_DATA_DIR", ""), "webhook_deliveries.db")
        if os.getenv("PUSHCLEAN_DATA_DIR", "")
        else "webhook_deliveries.db"
    )

# ── Configuration ─────────────────────────────────────────────────────────────
WEBHOOK_SECRET:         str  = os.getenv("GITHUB_WEBHOOK_SECRET", "")
ADMIN_API_KEY:          str  = os.getenv("ADMIN_API_KEY", "")
WATCHED_BRANCH:         str  = os.getenv("WATCHED_BRANCH", "main")
PORT:                   int  = int(os.getenv("PORT") or "8000")
WEBHOOK_WORKERS:        int  = max(1, min(10, int(os.getenv("WEBHOOK_WORKERS") or "3")))
RATE_LIMIT_WINDOW_SEC:  int  = int(os.getenv("RATE_LIMIT_WINDOW_SEC") or "60")
RATE_LIMIT_MAX_CALLS:   int  = int(os.getenv("RATE_LIMIT_MAX_CALLS") or "5")
MAX_JOB_RETRIES:        int  = int(os.getenv("MAX_JOB_RETRIES") or "3")
PROD_MODE:              bool = os.getenv("PROD_MODE", "0") == "1"

# v2: configurable stale job window (was hardcoded 2h — long jobs incorrectly recovered)
STALE_JOB_HOURS:        int  = max(1, int(os.getenv("STALE_JOB_HOURS") or "2"))

# v2: configurable body size cap — DoS protection on /webhook
MAX_WEBHOOK_BODY_BYTES: int  = int(os.getenv("MAX_WEBHOOK_BODY_BYTES") or str(10 * 1024 * 1024))

# Escalation min samples — must match LearningLog.MIN_SAMPLES in cost_optimizer v9
_ESCALATION_MIN_SAMPLES: int = 10

# HARDENED: In production mode, missing secret is a fatal error at startup
if PROD_MODE and not WEBHOOK_SECRET:
    log.critical(
        "PROD_MODE=1 but GITHUB_WEBHOOK_SECRET is not set — refusing to start. "
        "Set GITHUB_WEBHOOK_SECRET or disable PROD_MODE for development."
    )
    raise SystemExit(1)

if not WEBHOOK_SECRET:
    log.warning("⚠️  GITHUB_WEBHOOK_SECRET not set — signature verification DISABLED (dev mode)")
if not ADMIN_API_KEY:
    log.warning("⚠️  ADMIN_API_KEY not set — /run and /stats are UNPROTECTED")

# ── Startup check ────────────────────────────────────────────────────────────
try:
    from startup_check import run_all_checks as _startup_check
    _startup_check()
except ImportError:
    log.warning("startup_check.py not found — skipping pre-flight validation")

# ── App ───────────────────────────────────────────────────────────────────────
@asynccontextmanager
async def lifespan(app_: FastAPI):
    log.info("Worker pool active with %d threads", WEBHOOK_WORKERS)
    yield
    # Shutdown — wait for in-flight jobs to finish (up to 30s)
    log.info("Shutting down worker pool — waiting for in-flight jobs...")
    _worker_pool.shutdown(wait=True, cancel_futures=False)
    log.info("Worker pool shut down cleanly.")

app = FastAPI(
    title="PushClean Webhook",
    version="3.1",
    lifespan=lifespan,
) if HAS_FASTAPI else None

# Fallback for non-uvicorn exits (e.g. kill signal).
# wait=False is intentional here: jobs are persisted in DB and will be
# recovered on next startup — no data loss, just potential partial processing.
atexit.register(lambda: _worker_pool.shutdown(wait=False))

# ── Worker pool ───────────────────────────────────────────────────────────────
# Capped at 10 threads: SQLite WAL supports concurrent reads but serialises
# writes. Beyond ~10 threads the DB becomes the bottleneck, adding threads
# only increases lock contention without improving throughput.
_worker_pool = ThreadPoolExecutor(
    max_workers=WEBHOOK_WORKERS,
    thread_name_prefix="webhook_worker",
)
log.info("Worker pool started with %d threads", WEBHOOK_WORKERS)

# ── Rate limiter state (per-repo, sliding window, IN-MEMORY ONLY) ─────────────
# Enforcement is intentionally memory-only for sub-millisecond performance.
# DB rate_limit_log is written for observability/analytics — not for enforcement.
# Trade-off: a server restart resets the window, allowing a brief bypass.
# For strict cross-restart enforcement, switch to DB reads here (higher latency).
_rate_limit_lock  = threading.Lock()
_rate_limit_calls: dict[str, list[float]] = {}  # repo -> [timestamps]

# ── Delivery counter + time-based cleanup dual trigger ───────────────────────
_delivery_counter_lock = threading.Lock()
_delivery_count:   int   = 0
_last_cleanup_lock = threading.Lock()
_last_cleanup_time: float = 0.0   # monotonic time of last cleanup run


# ─────────────────────────────────────────────
# DB HELPERS
# ─────────────────────────────────────────────

@contextmanager
def _delivery_db():
    """WAL-mode context manager for the delivery DB."""
    conn = sqlite3.connect(DELIVERY_DB, check_same_thread=False)
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


def init_delivery_db() -> None:
    """
    Create webhook_deliveries, job_queue, rate_limit_log, and
    inflight_hashes tables on startup.
    HARDENED: persistent queue survives server restart.
    """
    with _delivery_db() as conn:
        conn.execute("""
            CREATE TABLE IF NOT EXISTS webhook_deliveries (
                delivery_id  TEXT PRIMARY KEY,
                received_at  TEXT NOT NULL,
                processed    INTEGER NOT NULL DEFAULT 0,
                processed_at TEXT
            )
        """)

        conn.execute("""
            CREATE TABLE IF NOT EXISTS job_queue (
                id           INTEGER PRIMARY KEY AUTOINCREMENT,
                job_id       TEXT UNIQUE NOT NULL,
                delivery_id  TEXT NOT NULL DEFAULT '',
                payload_json TEXT NOT NULL,
                status       TEXT NOT NULL DEFAULT 'pending',
                retries      INTEGER NOT NULL DEFAULT 0,
                max_retries  INTEGER NOT NULL DEFAULT 3,
                enqueued_at  TEXT NOT NULL,
                last_attempt TEXT,
                error_msg    TEXT DEFAULT ''
            )
        """)
        conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_jq_status
            ON job_queue (status, enqueued_at)
        """)

        conn.execute("""
            CREATE TABLE IF NOT EXISTS rate_limit_log (
                id          INTEGER PRIMARY KEY AUTOINCREMENT,
                repo_name   TEXT NOT NULL,
                called_at   TEXT NOT NULL
            )
        """)
        conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_rl_repo_time
            ON rate_limit_log (repo_name, called_at)
        """)

        # DB-backed in-flight deduplication (cross-process safe)
        # v2 FIX: this is the ONLY place this table is created.
        # _claim_inflight_db no longer re-creates it on every call.
        conn.execute("""
            CREATE TABLE IF NOT EXISTS inflight_hashes (
                content_hash TEXT PRIMARY KEY,
                claimed_at   TEXT NOT NULL
            )
        """)

    _cleanup_stale_inflight()


def is_duplicate_delivery(delivery_id: str) -> bool:
    """
    Atomically insert delivery_id.
    Returns True if already seen (duplicate), False if new.
    """
    try:
        with _delivery_db() as conn:
            conn.execute(
                "INSERT OR FAIL INTO webhook_deliveries (delivery_id, received_at) VALUES (?, ?)",
                (delivery_id, datetime.now(timezone.utc).isoformat()),
            )
        return False
    except sqlite3.IntegrityError:
        return True
    except Exception:
        log.exception("is_duplicate_delivery failed — treating as non-duplicate")
        return False


def mark_delivery_processed(delivery_id: str) -> None:
    """Mark a delivery as fully processed."""
    if not delivery_id:
        return
    try:
        with _delivery_db() as conn:
            conn.execute(
                "UPDATE webhook_deliveries SET processed=1, processed_at=? WHERE delivery_id=?",
                (datetime.now(timezone.utc).isoformat(), delivery_id),
            )
    except Exception:
        log.exception("mark_delivery_processed failed for %s", delivery_id)


def _claim_inflight_db(content_hash: str) -> bool:
    """
    Atomically claim a content_hash as in-flight in the DB.
    Returns True if successfully claimed (not a duplicate).
    Returns False if already in-flight (another worker has it).

    v2 FIX: removed redundant CREATE TABLE IF NOT EXISTS.
    v1 recreated the schema on every single call — potentially hundreds of
    times per second under load. Table is guaranteed by init_delivery_db()
    which runs once at startup.
    """
    if not content_hash:
        return True  # no hash = can't deduplicate, allow through
    try:
        with _delivery_db() as conn:
            conn.execute(
                "INSERT OR FAIL INTO inflight_hashes (content_hash, claimed_at) VALUES (?, ?)",
                (content_hash, datetime.now(timezone.utc).isoformat()),
            )
        return True
    except sqlite3.IntegrityError:
        return False  # already in-flight
    except Exception:
        log.exception("_claim_inflight_db failed — allowing through")
        return True


def _release_inflight_db(content_hash: str) -> None:
    """Release the in-flight DB claim for a content_hash."""
    if not content_hash:
        return
    try:
        with _delivery_db() as conn:
            conn.execute(
                "DELETE FROM inflight_hashes WHERE content_hash=?", (content_hash,)
            )
    except Exception:
        log.exception("_release_inflight_db failed for hash %s", content_hash[:16])


def _cleanup_stale_inflight() -> None:
    """
    Remove inflight_hashes older than 10 minutes (crash recovery).
    Called by init_delivery_db() on startup AND by recover_stale_jobs()
    so that both job queue and hash table stay consistent after a crash.
    """
    try:
        cutoff = (datetime.now(timezone.utc) - timedelta(minutes=10)).isoformat()
        with _delivery_db() as conn:
            conn.execute(
                "DELETE FROM inflight_hashes WHERE claimed_at < ?", (cutoff,)
            )
    except Exception:
        log.exception("_cleanup_stale_inflight failed")


def cleanup_old_deliveries() -> None:
    """
    Delete processed deliveries older than 7 days and old rate-limit logs.

    v2 FIX: dual-trigger — counter AND time-based.
    v1 used counter-only (every 100 deliveries). Under low traffic or
    frequent restarts (rolling deploys reset the counter), cleanup never ran
    and old records accumulated indefinitely.
    New: also triggers if >1 hour has elapsed since last cleanup — guaranteed
    to run eventually regardless of traffic volume or restart frequency.
    """
    global _delivery_count, _last_cleanup_time

    now = time.monotonic()
    should_clean = False

    with _delivery_counter_lock:
        _delivery_count += 1
        count_triggered = (_delivery_count % 100 == 0)

    with _last_cleanup_lock:
        time_triggered = (now - _last_cleanup_time) > 3600  # 1 hour
        if count_triggered or time_triggered:
            _last_cleanup_time = now
            should_clean = True

    if not should_clean:
        return

    cutoff_7d = (datetime.now(timezone.utc) - timedelta(days=7)).isoformat()
    cutoff_1h = (datetime.now(timezone.utc) - timedelta(hours=1)).isoformat()
    try:
        with _delivery_db() as conn:
            r1 = conn.execute(
                "DELETE FROM webhook_deliveries WHERE processed=1 AND received_at < ?",
                (cutoff_7d,),
            )
            r2 = conn.execute(
                "DELETE FROM rate_limit_log WHERE called_at < ?",
                (cutoff_1h,),
            )
            r3 = conn.execute("""
                DELETE FROM job_queue
                WHERE status IN ('done', 'dead')
                AND enqueued_at < ?
            """, (cutoff_7d,))
        if any(r.rowcount for r in [r1, r2, r3]):
            log.info(
                "Cleanup: %d delivery records, %d rate limit rows, %d jobs pruned",
                r1.rowcount, r2.rowcount, r3.rowcount,
            )
    except Exception:
        log.exception("cleanup_old_deliveries failed")


# ─────────────────────────────────────────────
# PERSISTENT JOB QUEUE
# ─────────────────────────────────────────────

def enqueue_job(payload: dict, delivery_id: str = "") -> str:
    """
    HARDENED: Persist a job to the DB queue (survives server restart).
    Returns job_id.
    """
    job_id = str(uuid.uuid4())
    try:
        with _delivery_db() as conn:
            conn.execute("""
                INSERT INTO job_queue
                    (job_id, delivery_id, payload_json, status, retries,
                     max_retries, enqueued_at)
                VALUES (?, ?, ?, 'pending', 0, ?, ?)
            """, (
                job_id, delivery_id or "",
                json.dumps(payload, ensure_ascii=False),
                MAX_JOB_RETRIES,
                datetime.now(timezone.utc).isoformat(),
            ))
        log.info(
            "Job %s enqueued (delivery=%s)",
            job_id[:8], (delivery_id or "")[:12] or "none",
        )
    except Exception:
        log.exception("enqueue_job failed — job may be lost")
    return job_id


def claim_next_job() -> Optional[dict]:
    """
    Atomically claim the next pending job using BEGIN IMMEDIATE so no two
    threads can claim the same row.
    Thread-safe via SQLite WAL + exclusive write lock.
    """
    try:
        conn = sqlite3.connect(DELIVERY_DB, check_same_thread=False)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA busy_timeout=5000")
        try:
            conn.execute("BEGIN IMMEDIATE")
            row = conn.execute("""
                SELECT id, job_id, delivery_id, payload_json, retries, max_retries
                FROM job_queue
                WHERE status = 'pending'
                ORDER BY enqueued_at ASC
                LIMIT 1
            """).fetchone()

            if not row:
                conn.rollback()
                return None

            conn.execute(
                "UPDATE job_queue SET status='running', last_attempt=? WHERE id=?",
                (datetime.now(timezone.utc).isoformat(), row["id"]),
            )
            conn.commit()
            return dict(row)
        except Exception:
            conn.rollback()
            raise
        finally:
            conn.close()
    except Exception:
        log.exception("claim_next_job failed")
        return None


def _mark_job_running(job_id: str) -> None:
    """
    Mark a job as 'running' with a fresh last_attempt timestamp.

    v2 NEW: called at the START of _worker_run_webhook for all jobs,
    including those submitted directly via _submit_job (which bypass
    claim_next_job and were never transitioned to 'running' in v1).

    Without this, a server crash during direct-submit job execution left
    the job in 'pending' state. recover_stale_jobs() only resets 'running'
    jobs, so the job would be re-executed on every startup — duplicate
    processing risk.
    """
    try:
        with _delivery_db() as conn:
            conn.execute(
                "UPDATE job_queue SET status='running', last_attempt=? WHERE job_id=?",
                (datetime.now(timezone.utc).isoformat(), job_id),
            )
    except Exception:
        log.warning(
            "Could not mark job %s as running — crash recovery for this job unreliable",
            job_id[:8],
        )


def mark_job_done(job_id: str, delivery_id: str) -> None:
    """Mark a job as successfully completed."""
    try:
        with _delivery_db() as conn:
            conn.execute(
                "UPDATE job_queue SET status='done' WHERE job_id=?", (job_id,)
            )
        mark_delivery_processed(delivery_id)
    except Exception:
        log.exception("mark_job_done failed for job %s", job_id[:8])


def mark_job_failed(job_id: str, error_msg: str) -> None:
    """
    HARDENED: Retry logic — increment retries.
    If retries >= max_retries, mark as 'dead' (no more retries).
    Otherwise reset to 'pending' for next worker cycle.
    Both SELECT and UPDATE run in the same transaction — atomic.
    """
    try:
        with _delivery_db() as conn:
            row = conn.execute(
                "SELECT retries, max_retries FROM job_queue WHERE job_id=?",
                (job_id,),
            ).fetchone()
            if not row:
                return

            new_retries = row["retries"] + 1
            if new_retries >= row["max_retries"]:
                conn.execute("""
                    UPDATE job_queue
                    SET status='dead', retries=?, error_msg=?
                    WHERE job_id=?
                """, (new_retries, str(error_msg)[:500], job_id))
                log.error(
                    "Job %s permanently failed after %d retries",
                    job_id[:8], new_retries,
                )
            else:
                conn.execute("""
                    UPDATE job_queue
                    SET status='pending', retries=?, error_msg=?
                    WHERE job_id=?
                """, (new_retries, str(error_msg)[:500], job_id))
                log.warning(
                    "Job %s failed (attempt %d/%d) — will retry",
                    job_id[:8], new_retries, row["max_retries"],
                )
    except Exception:
        log.exception("mark_job_failed failed for job %s", job_id[:8])


def recover_stale_jobs() -> int:
    """
    HARDENED: On startup, reset any jobs stuck in 'running' state (from crash).
    Also cleans up stale inflight_hashes entries.
    Returns count of recovered jobs.

    v2 FIX: now actually calls _cleanup_stale_inflight().
    v1 docstring promised this — code never did it. Stale hashes from crashed
    workers blocked future processing of the same content.

    v2 FIX: stale window now controlled by STALE_JOB_HOURS env var (default 2h).
    Previously hardcoded — legitimate jobs longer than 2h were incorrectly reset.
    """
    count = 0
    try:
        cutoff = (
            datetime.now(timezone.utc) - timedelta(hours=STALE_JOB_HOURS)
        ).isoformat()
        with _delivery_db() as conn:
            result = conn.execute("""
                UPDATE job_queue
                SET status='pending'
                WHERE status='running' AND last_attempt < ?
            """, (cutoff,))
        count = result.rowcount
        if count:
            log.warning(
                "Recovered %d stale running jobs on startup (older than %dh)",
                count, STALE_JOB_HOURS,
            )
    except Exception:
        log.exception("recover_stale_jobs failed")

    # v2 FIX: clean stale inflight hashes from crashed workers
    _cleanup_stale_inflight()

    return count


# ─────────────────────────────────────────────
# RATE LIMITER (per repo, sliding window)
# ─────────────────────────────────────────────

def _check_rate_limit(repo_name: str) -> bool:
    """
    HARDENED: Per-repo sliding window rate limiter.
    Returns True if allowed, False if rate-limited.

    Enforcement is in-memory only (fast, sub-millisecond).
    DB rate_limit_log is written for observability — NOT read for enforcement.
    See module docstring for trade-off discussion.
    """
    now = time.monotonic()
    window_start = now - RATE_LIMIT_WINDOW_SEC

    with _rate_limit_lock:
        calls = _rate_limit_calls.get(repo_name, [])
        calls = [t for t in calls if t > window_start]

        if len(calls) >= RATE_LIMIT_MAX_CALLS:
            log.warning(
                "Rate limit exceeded for repo '%s': %d calls in %ds window",
                repo_name, len(calls), RATE_LIMIT_WINDOW_SEC,
            )
            return False

        calls.append(now)
        _rate_limit_calls[repo_name] = calls

    # Non-blocking DB log for observability (best-effort — failure must not block)
    try:
        with _delivery_db() as conn:
            conn.execute(
                "INSERT INTO rate_limit_log (repo_name, called_at) VALUES (?, ?)",
                (repo_name, datetime.now(timezone.utc).isoformat()),
            )
    except Exception:
        pass

    return True


# ─────────────────────────────────────────────
# SECURITY HELPERS
# ─────────────────────────────────────────────

def verify_signature(payload_body: bytes, signature: str) -> bool:
    """
    HARDENED: Verify GitHub HMAC-SHA256 webhook signature.

    In PROD_MODE, missing secret causes startup failure (enforced at module load).
    In dev mode (no secret), allow through with a warning.
    In all other cases, ALWAYS verify the signature — no bypass.
    """
    if not WEBHOOK_SECRET:
        # Dev mode only — PROD_MODE with no secret is rejected at startup
        return True

    if not signature:
        log.warning("verify_signature: no X-Hub-Signature-256 header present")
        return False

    if not signature.startswith("sha256="):
        log.warning("verify_signature: signature does not start with 'sha256='")
        return False

    try:
        expected = "sha256=" + hmac.new(
            WEBHOOK_SECRET.encode("utf-8"),
            payload_body,
            hashlib.sha256,
        ).hexdigest()
        return hmac.compare_digest(expected, signature)
    except Exception:
        log.exception("verify_signature: HMAC computation failed")
        return False


if HAS_FASTAPI:
    def verify_admin(
        credentials: HTTPAuthorizationCredentials = Depends(HTTPBearer(auto_error=False)),
    ) -> None:
        """
        HARDENED: Constant-time comparison to prevent timing attacks.
        Returns 503 (not 401) when ADMIN_API_KEY is unconfigured — signals
        misconfiguration rather than auth failure.
        """
        if not ADMIN_API_KEY:
            raise HTTPException(status_code=503, detail="Admin auth not configured")
        if credentials is None:
            raise HTTPException(status_code=401, detail="Authorization header required")
        if not hmac.compare_digest(credentials.credentials, ADMIN_API_KEY):
            raise HTTPException(status_code=403, detail="Invalid admin key")


# ─────────────────────────────────────────────
# COST OPTIMIZER STATS HELPER
# ─────────────────────────────────────────────

def _get_cost_optimizer_stats() -> dict:
    """
    Read CostOptimizer LearningLog stats from cost_optimizer.db.

    v2 FIX: escalation_candidates HAVING now uses _ESCALATION_MIN_SAMPLES=10,
    consistent with LearningLog.MIN_SAMPLES in cost_optimizer v8.
    v1 used HAVING total >= 2 — inflated dashboard count by counting files
    with too few samples to be statistically meaningful.
    """
    if not HAS_COST_OPTIMIZER:
        return {"available": False}
    try:
        with get_db(COST_OPTIMIZER_DB) as db:
            overall = db.execute("""
                SELECT COUNT(*) as total,
                       SUM(success) as successes,
                       AVG(score)   as avg_score
                FROM learning_log
            """).fetchone()

            by_mode = db.execute("""
                SELECT balance_mode,
                       COUNT(*)        as total,
                       SUM(success)    as successes,
                       AVG(score)      as avg_score
                FROM learning_log
                WHERE balance_mode IS NOT NULL
                GROUP BY balance_mode
                ORDER BY total DESC
            """).fetchall()

            # v2 FIX: consistent with cost_optimizer v8 MIN_SAMPLES=10
            escalation_candidates = db.execute(f"""
                SELECT COUNT(DISTINCT file_hash) as cnt
                FROM (
                    SELECT file_hash,
                           SUM(success) * 1.0 / COUNT(*) as rate,
                           COUNT(*) as total
                    FROM learning_log
                    WHERE stage = 'refine'
                    GROUP BY file_hash
                    HAVING rate < 0.5 AND total >= {_ESCALATION_MIN_SAMPLES}
                )
            """).fetchone()

        total     = overall[0] or 0
        successes = overall[1] or 0
        return {
            "available":           True,
            "total_outcomes":      total,
            "success_rate":        round(successes / total, 3) if total else None,
            "avg_score":           round(float(overall[2] or 0.0), 3),
            "by_balance_mode": [
                {
                    "mode":         r[0],
                    "total":        r[1],
                    "success_rate": round((r[2] or 0) / r[1], 3) if r[1] else None,
                    "avg_score":    round(float(r[3] or 0.0), 3),
                }
                for r in by_mode
            ],
            "escalation_candidates": escalation_candidates[0] if escalation_candidates else 0,
            "escalation_min_samples": _ESCALATION_MIN_SAMPLES,
            "db":                  COST_OPTIMIZER_DB,
        }
    except Exception as exc:
        log.warning("_get_cost_optimizer_stats failed: %s", exc)
        return {"available": True, "error": str(exc)}


# ─────────────────────────────────────────────
# WORKER FUNCTIONS
# ─────────────────────────────────────────────

def _worker_run_webhook(job: dict) -> None:
    """
    HARDENED worker: runs in ThreadPoolExecutor thread.
    Handles retries internally via mark_job_failed/mark_job_done.
    All errors are caught and logged — no silent failures.

    v2 FIX: marks job as 'running' BEFORE calling the orchestrator.
    v1 jobs submitted via _submit_job (direct pool submission) were never
    transitioned to 'running' status — they went from 'pending' directly
    to 'done' or 'failed'. A server crash during execution left the job
    'pending', causing re-execution on every startup (duplicate processing).
    Now all jobs pass through 'running' → recover_stale_jobs() can detect
    and reset them correctly.
    """
    job_id      = job.get("job_id", "unknown")
    delivery_id = job.get("delivery_id", "")
    payload_raw = job.get("payload_json", "{}")

    try:
        payload = json.loads(payload_raw)
    except json.JSONDecodeError as exc:
        log.error("Worker[%s]: invalid JSON payload — %s", job_id[:8], exc)
        mark_job_failed(job_id, f"invalid_json: {exc}")
        return

    # v2 FIX: mark running FIRST — crash recovery now reliable for all job paths
    _mark_job_running(job_id)

    try:
        log.info(
            "Worker[%s]: starting orchestrator (delivery=%s)",
            job_id[:8], (delivery_id or "")[:12] or "none",
        )
        run_webhook(payload)
        mark_job_done(job_id, delivery_id)
        log.info("Worker[%s]: completed successfully", job_id[:8])
    except Exception as exc:
        log.exception("Worker[%s]: orchestrator crashed — %s", job_id[:8], exc)
        mark_job_failed(job_id, str(exc))


def _submit_job(payload: dict, delivery_id: str = "") -> None:
    """
    Enqueue a job to the persistent queue and submit it to the worker pool.
    HARDENED: if pool is at capacity, job stays in DB and will be picked up
    by the next available worker via _drain_queue().
    """
    job_id = enqueue_job(payload, delivery_id)

    try:
        job = {
            "job_id":       job_id,
            "delivery_id":  delivery_id,
            "payload_json": json.dumps(payload, ensure_ascii=False),
            "retries":      0,
            "max_retries":  MAX_JOB_RETRIES,
        }
        future: Future = _worker_pool.submit(_worker_run_webhook, job)

        def _on_done(f: Future) -> None:
            if f.exception():
                log.error("Worker future raised: %s", f.exception())
            # After each job completes, drain the next pending DB job
            _drain_queue_once()

        future.add_done_callback(_on_done)
    except RuntimeError:
        # Pool shutdown — job stays in DB for recovery on next startup
        log.warning(
            "Worker pool shut down — job %s will be recovered on restart",
            job_id[:8],
        )


def _drain_queue_once() -> None:
    """
    HARDENED: Claim and submit the next pending job from the DB queue.
    Called after each worker completes to ensure continuous processing.
    """
    job = claim_next_job()
    if job:
        try:
            _worker_pool.submit(_worker_run_webhook, {
                "job_id":       job["job_id"],
                "delivery_id":  job["delivery_id"],
                "payload_json": job["payload_json"],
                "retries":      job["retries"],
                "max_retries":  job["max_retries"],
            })
        except RuntimeError:
            pass  # pool shut down


def _safe_manual_run() -> None:
    """
    Run a manual full-scan orchestration.
    HARDENED: errors are logged; no silent failures.
    """
    try:
        log.info("Manual full scan starting")
        MasterOrchestrator().run(trigger="manual")
        log.info("Manual full scan completed")
    except Exception:
        log.exception("Manual orchestrator run crashed")


def _run_batch_learning() -> None:
    """
    Background task — trigger PushCleanBrain batch learning.
    HARDENED: errors logged; no silent failures.
    """
    try:
        api   = SmartAPICaller()
        brain = PushCleanBrain(
            api_for_analysis     = api.for_analysis,
            api_for_verification = api.for_verification,
            api_general          = api.general,
        )
        total = 0
        for lang in ["Python", "JavaScript", "TypeScript", "Go", "Rust"]:
            new = brain.run_batch_learning(lang)
            total += new
            if new:
                log.info("Batch learn: %d patterns from %s", new, lang)
        log.info("Batch learning complete — %d total new patterns", total)
    except Exception:
        log.exception("Batch learning crashed")


# ─────────────────────────────────────────────
# ROUTES
# ─────────────────────────────────────────────

if HAS_FASTAPI:

    @app.get("/health")
    async def health():
        """Liveness probe — returns service status, queue depth, brain stats."""
        orchestrator_running = False
        try:
            with get_db() as conn:
                row = conn.execute("SELECT 1 FROM run_locks LIMIT 1").fetchone()
                orchestrator_running = row is not None
        except Exception:
            log.exception("Health check: run_locks query failed")

        queue_depth = 0
        dead_jobs   = 0
        try:
            with _delivery_db() as conn:
                row = conn.execute(
                    "SELECT COUNT(*) FROM job_queue WHERE status='pending'"
                ).fetchone()
                queue_depth = row[0] if row else 0
                row2 = conn.execute(
                    "SELECT COUNT(*) FROM job_queue WHERE status='dead'"
                ).fetchone()
                dead_jobs = row2[0] if row2 else 0
        except Exception:
            log.exception("Health check: job_queue query failed")

        deliveries_today = 0
        try:
            with _delivery_db() as conn:
                today_start = datetime.now(timezone.utc).replace(
                    hour=0, minute=0, second=0, microsecond=0,
                ).isoformat()
                row = conn.execute(
                    "SELECT COUNT(*) FROM webhook_deliveries WHERE received_at >= ?",
                    (today_start,),
                ).fetchone()
                deliveries_today = row[0] if row else 0
        except Exception:
            log.exception("Health check: deliveries count failed")

        brain_stats = {}
        try:
            with get_db(BRAIN_DB) as conn:
                pairs    = conn.execute(
                    "SELECT COUNT(*) FROM refinement_pairs WHERE accepted = 1"
                ).fetchone()
                patterns = conn.execute(
                    "SELECT COUNT(*) FROM learned_patterns"
                ).fetchone()
                brain_stats = {
                    "accepted_pairs":   pairs[0] if pairs else 0,
                    "learned_patterns": patterns[0] if patterns else 0,
                }
        except Exception:
            brain_stats = {"accepted_pairs": 0, "learned_patterns": 0}

        cost_stats: dict = {}
        if HAS_COST_OPTIMIZER:
            try:
                with get_db(COST_OPTIMIZER_DB) as _cdb:
                    _row = _cdb.execute(
                        "SELECT COUNT(*), SUM(success), AVG(score) FROM learning_log"
                    ).fetchone()
                    total     = _row[0] or 0
                    successes = _row[1] or 0
                    avg_sc    = round(float(_row[2] or 0.0), 3)
                cost_stats = {
                    "total_outcomes": total,
                    "success_rate":   round(successes / total, 3) if total else None,
                    "avg_score":      avg_sc,
                }
            except Exception:
                cost_stats = {"error": "could not read cost_optimizer.db"}

        return {
            "status":               "running",
            "service":              "PushClean Bot",
            "watched_branch":       WATCHED_BRANCH,
            "time":                 datetime.now(timezone.utc).isoformat(),
            "orchestrator_running": orchestrator_running,
            "queue_depth":          queue_depth,
            "dead_jobs":            dead_jobs,
            "worker_threads":       WEBHOOK_WORKERS,
            "deliveries_today":     deliveries_today,
            "brain":                brain_stats,
            "cost_optimizer":       cost_stats,
            "prod_mode":            PROD_MODE,
            "signature_check":      bool(WEBHOOK_SECRET),
        }

    @app.post("/webhook")
    async def github_webhook(request: Request, background_tasks: BackgroundTasks):
        """
        Receive GitHub push webhook events.
        HARDENED v2:
        - Body size cap (MAX_WEBHOOK_BODY_BYTES) — DoS protection
        - Strict signature validation (no bypass in prod)
        - Per-repo rate limiting
        - Persistent job queue (no data loss on server restart)
        - Parallel worker pool (no single-thread bottleneck)
        """
        # 1. Signature verification — read body first
        signature = request.headers.get("X-Hub-Signature-256", "")
        body      = await request.body()

        # v2 FIX: body size cap — prevents DoS via giant payloads
        if len(body) > MAX_WEBHOOK_BODY_BYTES:
            log.warning(
                "Webhook rejected: body size %d bytes exceeds cap %d bytes (from %s)",
                len(body), MAX_WEBHOOK_BODY_BYTES,
                request.client.host if request.client else "unknown",
            )
            raise HTTPException(
                status_code=413,
                detail=f"Payload too large (max {MAX_WEBHOOK_BODY_BYTES // 1024}KB)",
            )

        if not verify_signature(body, signature):
            log.warning(
                "Webhook signature mismatch from %s — request rejected",
                request.client.host if request.client else "unknown",
            )
            raise HTTPException(status_code=401, detail="Invalid signature")

        # 2. Filter by event type
        event_type = request.headers.get("X-GitHub-Event", "")

        # 2a. GitHub Marketplace purchase events — billing automation
        if event_type == "marketplace_purchase" and HAS_MONETIZATION:
            try:
                data    = json.loads(body)
                action  = data.get("action", "")
                mp      = data.get("marketplace_purchase", {})
                plan_nm = mp.get("plan", {}).get("name", "free").lower()
                inst_id = str(data.get("installation", {}).get("id", "")
                            or mp.get("account", {}).get("id", ""))
                owner   = mp.get("account", {}).get("login", "")

                plan = "pro" if "pro" in plan_nm or "paid" in plan_nm else "free"

                if action in ("purchased", "changed"):
                    upgrade_user(inst_id, plan=plan)
                    log.info("Marketplace %s: %s → plan=%s", action, owner, plan)
                elif action in ("cancelled", "pending_change"):
                    upgrade_user(inst_id, plan="free")
                    log.info("Marketplace %s: %s → downgraded to free", action, owner)
            except Exception:
                log.exception("marketplace_purchase handling failed")
            return JSONResponse({"status": "ok", "event": event_type})

        # 2b. GitHub App installation events — track users
        if event_type == "installation" and HAS_MONETIZATION:
            try:
                data   = json.loads(body)
                action = data.get("action", "")
                inst   = data.get("installation", {})
                inst_id = str(inst.get("id", ""))
                owner   = inst.get("account", {}).get("login", "")
                repos   = [r.get("full_name", "") for r in data.get("repositories", [])]
                repo_name = repos[0] if repos else ""
                if action == "created" and inst_id:
                    register_installation(inst_id, owner, repo_name)
                    log.info("App installed: %s by %s", inst_id[:12], owner)
                elif action in ("deleted", "suspend") and inst_id:
                    unregister_installation(inst_id)
                    log.info("App uninstalled: %s by %s", inst_id[:12], owner)
            except Exception:
                log.exception("Installation event handling failed")
            return JSONResponse({"status": "ok", "event": event_type})

        if event_type != "push":
            log.info("Ignoring event: %s", event_type)
            return JSONResponse({"status": "ignored", "event": event_type})

        # 3. Parse payload
        try:
            payload = json.loads(body)
        except json.JSONDecodeError:
            log.error("Malformed JSON payload received")
            raise HTTPException(status_code=400, detail="Invalid JSON payload")

        repo_name = payload.get("repository", {}).get("full_name", "unknown")

        # 3a. Plan limit check — before queuing any work
        if HAS_MONETIZATION:
            installation_id = str(
                payload.get("installation", {}).get("id", "")
                or payload.get("sender", {}).get("id", "")
            )
            # FIX: guard against "0" and "" — both bypass monetization.
            # GitHub sends id=0 on some bot/test events; str("0") is truthy
            # so the old check would pass it through to a wrong user row.
            if installation_id and installation_id != "0":
                try:
                    check_and_increment(installation_id, repo_name=repo_name)
                except LimitReached as exc:
                    log.info(
                        "Plan limit reached for installation %s (%s)",
                        installation_id[:12], repo_name,
                    )
                    return JSONResponse(
                        status_code=402,
                        content={
                            "status":  "limit_reached",
                            "message": str(exc),
                            "upgrade": "https://github.com/marketplace/pushclean",
                        },
                    )

        # 4. Per-repo rate limiting
        if not _check_rate_limit(repo_name):
            return JSONResponse(
                status_code=429,
                content={
                    "status": "rate_limited",
                    "repo":   repo_name,
                    "message": (
                        f"Rate limit exceeded: max {RATE_LIMIT_MAX_CALLS} calls "
                        f"per {RATE_LIMIT_WINDOW_SEC}s per repo"
                    ),
                },
            )

        # 5. Branch filter
        pushed_ref    = payload.get("ref", "")
        pushed_branch = pushed_ref.removeprefix("refs/heads/")
        if pushed_branch != WATCHED_BRANCH:
            log.info(
                "Ignoring push to '%s' (watching '%s')",
                pushed_branch, WATCHED_BRANCH,
            )
            return JSONResponse({
                "status": "ignored",
                "reason": f"Branch '{pushed_branch}' not watched",
            })

        # 6. Idempotency check
        delivery_id = request.headers.get("X-GitHub-Delivery", "")
        if delivery_id and is_duplicate_delivery(delivery_id):
            log.warning("Duplicate delivery %s — ignoring", delivery_id)
            return JSONResponse({
                "status":      "ignored",
                "reason":      "duplicate delivery",
                "delivery_id": delivery_id,
            })

        # 7. Periodic cleanup
        cleanup_old_deliveries()

        # 8. Enqueue to persistent queue + submit to worker pool
        log.info(
            "Enqueuing orchestrator job for %s [%s]",
            repo_name, (delivery_id or "")[:12] or "none",
        )
        background_tasks.add_task(_submit_job, payload, delivery_id)

        return JSONResponse({
            "status":  "accepted",
            "event":   event_type,
            "repo":    repo_name,
            "branch":  WATCHED_BRANCH,
            "message": "Job enqueued to persistent queue",
        })

    @app.get("/stats", dependencies=[Depends(verify_admin)])
    async def get_stats():
        """Return full bot intelligence statistics (admin only)."""
        try:
            with get_db(SELF_LEARNING_DB) as conn:
                c = conn.cursor()
                c.execute("SELECT COUNT(*) FROM finetune_memory")
                rules = c.fetchone()[0]
                c.execute("SELECT AVG(self_score), COUNT(*) FROM retrospect_log")
                row        = c.fetchone()
                avg_score  = round(float(row[0] or 0.0), 2)
                total_runs = row[1]

            with get_db(BRAIN_DB) as conn:
                pairs    = conn.execute(
                    "SELECT COUNT(*), AVG(self_score) FROM refinement_pairs WHERE accepted = 1"
                ).fetchone()
                rejected = conn.execute(
                    "SELECT COUNT(*) FROM refinement_pairs WHERE accepted = 0"
                ).fetchone()
                patterns = conn.execute(
                    "SELECT COUNT(*) FROM learned_patterns"
                ).fetchone()
                failures = conn.execute(
                    "SELECT language, error_type, occurrences "
                    "FROM validation_failures "
                    "ORDER BY occurrences DESC LIMIT 5"
                ).fetchall()
                by_lang = conn.execute(
                    "SELECT language, COUNT(*), AVG(self_score) "
                    "FROM refinement_pairs WHERE accepted = 1 "
                    "GROUP BY language ORDER BY COUNT(*) DESC"
                ).fetchall()

            with _delivery_db() as conn:
                jq = conn.execute("""
                    SELECT status, COUNT(*) as cnt
                    FROM job_queue GROUP BY status
                """).fetchall()
                jq_stats = {r[0]: r[1] for r in jq}

            return {
                "status":            "healthy",
                "self_learning": {
                    "rules_learned":         rules,
                    "avg_self_score":        avg_score,
                    "total_files_processed": total_runs,
                },
                "brain": {
                    "accepted_pairs":   pairs[0] if pairs else 0,
                    "avg_pair_score":   round(float(pairs[1] or 0.0), 2) if pairs else 0,
                    "rejected_pairs":   rejected[0] if rejected else 0,
                    "learned_patterns": patterns[0] if patterns else 0,
                    "top_errors":       [
                        {"language": r[0], "type": r[1], "count": r[2]}
                        for r in failures
                    ],
                    "by_language": [
                        {"language": r[0], "pairs": r[1], "avg_score": round(float(r[2] or 0), 2)}
                        for r in by_lang
                    ],
                },
                "job_queue":         jq_stats,
                "cost_optimizer":    _get_cost_optimizer_stats(),
            }
        except Exception as exc:
            log.exception("Stats fetch failed")
            raise HTTPException(status_code=500, detail=str(exc))

    @app.get("/brain", dependencies=[Depends(verify_admin)])
    async def brain_report():
        """Full PushCleanBrain report — training data progress (admin only)."""
        try:
            with get_db(BRAIN_DB) as conn:
                total_pairs = conn.execute(
                    "SELECT COUNT(*), AVG(self_score) FROM refinement_pairs"
                ).fetchone()
                accepted = conn.execute(
                    "SELECT COUNT(*) FROM refinement_pairs WHERE accepted = 1"
                ).fetchone()
                rejected = conn.execute(
                    "SELECT COUNT(*) FROM refinement_pairs WHERE accepted = 0"
                ).fetchone()
                patterns_total = conn.execute(
                    "SELECT COUNT(*) FROM learned_patterns"
                ).fetchone()
                top_patterns = conn.execute(
                    "SELECT language, pattern, confidence, used_count "
                    "FROM learned_patterns "
                    "ORDER BY confidence DESC, used_count DESC LIMIT 10"
                ).fetchall()
                top_errors = conn.execute(
                    "SELECT language, error_type, occurrences, last_seen "
                    "FROM validation_failures "
                    "ORDER BY occurrences DESC LIMIT 10"
                ).fetchall()
                score_dist = conn.execute("""
                    SELECT
                        CASE
                            WHEN self_score >= 9.0 THEN '9-10'
                            WHEN self_score >= 8.0 THEN '8-9'
                            WHEN self_score >= 7.0 THEN '7-8'
                            ELSE 'below-7'
                        END as bucket,
                        COUNT(*) as count
                    FROM refinement_pairs
                    WHERE accepted = 1
                    GROUP BY bucket
                    ORDER BY bucket DESC
                """).fetchall()

            return {
                "summary": {
                    "total_pairs":    total_pairs[0] if total_pairs else 0,
                    "avg_score":      round(float(total_pairs[1] or 0.0), 2) if total_pairs else 0,
                    "accepted":       accepted[0] if accepted else 0,
                    "rejected":       rejected[0] if rejected else 0,
                    "total_patterns": patterns_total[0] if patterns_total else 0,
                },
                "score_distribution": {r[0]: r[1] for r in score_dist},
                "top_patterns": [
                    {"language": r[0], "pattern": r[1], "confidence": r[2], "used": r[3]}
                    for r in top_patterns
                ],
                "recurring_errors": [
                    {"language": r[0], "type": r[1], "count": r[2], "last_seen": r[3]}
                    for r in top_errors
                ],
                "message": (
                    "Collecting data for future fine-tuning. "
                    f"Need ~10,000 accepted pairs for training. "
                    f"Current: {accepted[0] if accepted else 0}"
                ),
            }
        except Exception as exc:
            log.exception("Brain report failed")
            raise HTTPException(status_code=500, detail=str(exc))

    @app.post("/brain/learn", dependencies=[Depends(verify_admin)])
    async def trigger_batch_learning(background_tasks: BackgroundTasks):
        """Manually trigger batch pattern learning (admin only)."""
        log.info("Manual batch learning triggered via /brain/learn")
        background_tasks.add_task(_run_batch_learning)
        return {
            "status":  "started",
            "message": "Batch learning running in background — check /brain for results",
        }

    @app.get("/users", dependencies=[Depends(verify_admin)])
    async def list_users():
        """List all active installations + usage (admin only)."""
        if not HAS_MONETIZATION:
            raise HTTPException(status_code=501, detail="Monetization not installed")
        users = get_all_users(active_only=False)
        stats = get_monetization_stats()
        return {"stats": stats, "users": users}

    @app.post("/upgrade", dependencies=[Depends(verify_admin)])
    async def manual_upgrade(request: Request):
        """
        Manually upgrade a user's plan (admin only).
        Body: {"installation_id": "...", "plan": "pro"}
        Use this to apply Stripe webhook upgrades or comp accounts.
        """
        if not HAS_MONETIZATION:
            raise HTTPException(status_code=501, detail="Monetization not installed")
        try:
            data = await request.json()
        except Exception:
            raise HTTPException(status_code=400, detail="Invalid JSON body")
        installation_id = data.get("installation_id", "").strip()
        plan            = data.get("plan", "pro").strip()
        if not installation_id:
            raise HTTPException(status_code=400, detail="installation_id required")
        ok = upgrade_user(installation_id, plan=plan)
        if not ok:
            raise HTTPException(status_code=404, detail="Installation not found")
        return {"status": "upgraded", "installation_id": installation_id, "plan": plan}

    @app.post("/run", dependencies=[Depends(verify_admin)])
    async def manual_run(background_tasks: BackgroundTasks):
        """Trigger a full repository scan manually (admin only)."""
        log.info("Manual full scan triggered via /run")
        background_tasks.add_task(_safe_manual_run)
        return {"status": "started", "message": "Full scan triggered"}

    @app.get("/queue", dependencies=[Depends(verify_admin)])
    async def queue_status():
        """Return persistent job queue state (admin only)."""
        try:
            with _delivery_db() as conn:
                rows = conn.execute("""
                    SELECT job_id, delivery_id, status, retries, max_retries,
                           enqueued_at, last_attempt, error_msg
                    FROM job_queue
                    ORDER BY enqueued_at DESC
                    LIMIT 50
                """).fetchall()
            return {
                "jobs": [
                    {
                        "job_id":       r["job_id"][:8],
                        "delivery_id":  (r["delivery_id"] or "")[:12],
                        "status":       r["status"],
                        "retries":      f"{r['retries']}/{r['max_retries']}",
                        "enqueued_at":  r["enqueued_at"],
                        "last_attempt": r["last_attempt"],
                        "error_msg":    r["error_msg"],
                    }
                    for r in rows
                ]
            }
        except Exception as exc:
            log.exception("Queue status failed")
            raise HTTPException(status_code=500, detail=str(exc))


# ── Start monetization background thread ─────────────────────────────────────
if HAS_MONETIZATION:
    start_monthly_reset_thread()

# ── Initialise DB + recover stale jobs at module load ────────────────────────
init_delivery_db()
_recovered = recover_stale_jobs()
if _recovered:
    log.info("Startup recovery: %d stale jobs reset to pending", _recovered)

# Drain any pending jobs from previous server session
_drain_queue_once()


# ── Entry point ───────────────────────────────────────────────────────────────
if __name__ == "__main__":
    if not HAS_FASTAPI:
        print("Install FastAPI first: pip install fastapi uvicorn")
    else:
        log.info("🚀 Starting webhook server on port %d...", PORT)
        log.info("   Watching branch : %s", WATCHED_BRANCH)
        log.info("   Signature check : %s", "enabled" if WEBHOOK_SECRET else "DISABLED")
        log.info("   Admin auth      : %s", "enabled" if ADMIN_API_KEY  else "DISABLED")
        log.info("   Worker threads  : %d (max 10 — SQLite write bottleneck above this)", WEBHOOK_WORKERS)
        log.info("   Rate limit      : %d calls / %ds per repo (in-memory)", RATE_LIMIT_MAX_CALLS, RATE_LIMIT_WINDOW_SEC)
        log.info("   Max retries     : %d", MAX_JOB_RETRIES)
        log.info("   Stale job hours : %d", STALE_JOB_HOURS)
        log.info("   Max body size   : %dKB", MAX_WEBHOOK_BODY_BYTES // 1024)
        log.info("   Prod mode       : %s", PROD_MODE)
        log.info("   Idempotency DB  : %s", DELIVERY_DB)
        log.info("   Health  : http://localhost:%d/health",          PORT)
        log.info("   Webhook : http://localhost:%d/webhook",         PORT)
        log.info("   Stats   : http://localhost:%d/stats   (admin)", PORT)
        log.info("   Queue   : http://localhost:%d/queue   (admin)", PORT)
        log.info("   Brain   : http://localhost:%d/brain   (admin)", PORT)
        log.info("   Learn   : http://localhost:%d/brain/learn (admin)", PORT)
        log.info("   Run     : http://localhost:%d/run     (admin)", PORT)
        uvicorn.run(app, host="0.0.0.0", port=PORT, log_level="info")
