"""
╔══════════════════════════════════════════════════════════════════╗
║          MASTER ORCHESTRATOR — PushClean Bot                     ║
║                         FIXED v1.1                               ║
║                                                                  ║
║  Combines:                                                       ║
║  1. cleaner_bot.py       → Multi-API fusion (DeepSeek+Claude+Gemini) ║
║  2. fine_tuning_layer.py → Style + Standards + Context + Feedback  ║
║  3. self_learning.py     → Autonomous Retrospect + Self-Improve  ║
║                                                                  ║
║  Flow:                                                           ║
║  GitHub Push → Orchestrator → Analyze → Refine → Learn → Commit ║
║                                                                  ║
║  FIX v1.1:                                                       ║
║  - DB paths now from db_paths.py (PUSHCLEAN_DATA_DIR respected)    ║
║    v1.0 hardcoded "orchestrator.db" etc → wrote to CWD always,  ║
║    losing ALL data on every Render redeploy.                     ║
║  - Cost optimizer import now tries cost_optimizer_v9 before v6   ║
║    v1.0 only tried v6 → HAS_COST_OPTIMIZER=False always.         ║
║  - hashlib imported at module level (was inside method body)     ║
║  - co_decision default: None instead of () (wrong type)          ║
╚══════════════════════════════════════════════════════════════════╝
"""

from __future__ import annotations

import hashlib  # FIX v1.1: module-level (was imported inside _run_file_pipeline)
import os
import sys
import time
import random
import sqlite3
import logging
import threading
from contextlib import contextmanager
from datetime import datetime, timezone, timedelta
from pathlib import Path
from typing import Optional, Tuple

# ── Import all 3 modules ─────────────────────────────────────────────────────
from cleaner_bot import (
    GitHubClient, call_claude, call_deepseek, call_gemini,
    verify_refinement, CONFIG,
)
from fine_tuning_layer import FineTuningLayer
from self_learning import SelfLearningEngine

# ── Cost Optimizer — pre-processing intelligence layer ───────────────────────
# FIX v1.1: import chain now tries cost_optimizer_v9 before v6.
# v1.0 only tried cost_optimizer_v6 — the actual file is cost_optimizer_v9.py.
# HAS_COST_OPTIMIZER was always False, silently disabling all routing logic.
try:
    from optimized_cost import (
        CostOptimizer,
        LearningLog as CostLearningLog,
    )
    HAS_COST_OPTIMIZER = True
except ImportError:
    try:
        from cost_optimizer_v9 import (          # FIX: try v9 before v6
            CostOptimizer,
            LearningLog as CostLearningLog,
        )
        HAS_COST_OPTIMIZER = True
    except ImportError:
        try:
            from cost_optimizer_v6 import (
                CostOptimizer,
                LearningLog as CostLearningLog,
            )
            HAS_COST_OPTIMIZER = True
        except ImportError:
            HAS_COST_OPTIMIZER = False
            logging.getLogger("orchestrator").warning(
                "No cost_optimizer found — pre-flight routing disabled. "
                "All files will enter the full API pipeline."
            )

# ── Logging ──────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("orchestrator")


# ─────────────────────────────────────────────
# ORCHESTRATOR CONFIG
# ─────────────────────────────────────────────

def _parse_budget() -> float:
    """Safe parse of MAX_BUDGET_USD — float('abc') would crash at import time."""
    raw = os.getenv("MAX_BUDGET_USD") or "5.0"
    try:
        return float(raw)
    except ValueError:
        logger.warning("Invalid MAX_BUDGET_USD=%r — defaulting to 5.0", raw)
        return 5.0


ORCHESTRATOR_CONFIG: dict = {
    # Self-learning thresholds
    # NOTE: retry attempts are controlled by PUSHCLEAN_MAX_ATTEMPTS env var
    # in self_learning.py — not here. Tune that var, not this config.
    "min_score_to_commit":        6.5,
    "min_confidence_rule":        0.65,

    # Fine-tuning
    "style_sample_files":         8,
    "enable_standards_auto_load": True,

    # Processing
    "delay_between_files":        1,     # base seconds; jitter added at call site
    "max_files_per_run":          50,

    # Cost guard
    "max_budget_usd":             _parse_budget(),

    # Skip files smaller than this (bytes)
    "min_file_bytes":             80,

    # Run lock TTL — stale locks older than this are evicted
    "lock_ttl_seconds":           600,

    # DB pruning — file_runs / api_spend older than this are deleted on init
    "db_retention_days":          7,

    # Output
    "verbose":    True,
}

# FIX v1.1: DB paths now sourced from db_paths.py so PUSHCLEAN_DATA_DIR is
# respected on Render. v1.0 hardcoded plain filenames → DBs created in CWD
# (ephemeral on Render) → ALL data lost on every redeploy.
try:
    from db_paths import (
        ORCHESTRATOR_DB,
        SELF_LEARNING_DB,
        COST_OPTIMIZER_DB,
        validate_data_dir as _validate_data_dir,
    )
    _validate_data_dir()
except ImportError:
    # Fallback: manual PUSHCLEAN_DATA_DIR resolution if db_paths.py not present
    _data_dir = os.getenv("PUSHCLEAN_DATA_DIR", "").strip()
    def _mk(name: str) -> str:
        return os.path.join(_data_dir, name) if _data_dir else name
    ORCHESTRATOR_DB   = _mk("orchestrator.db")
    SELF_LEARNING_DB  = _mk("self_learning.db")
    COST_OPTIMIZER_DB = _mk("cost_optimizer.db")
COMMIT_MSG_LIMIT = 72

_DB_MIGRATIONS: list[tuple[str, str, str]] = [
    ("runs",      "commit_branch", "TEXT DEFAULT ''"),
    ("file_runs", "error_msg",     "TEXT DEFAULT ''"),
]

# Guards schema creation — prevents redundant PRAGMA calls on every request
_DB_INITIALIZED: bool = False
_DB_LOCK = threading.Lock()


# ─────────────────────────────────────────────
# HELPERS
# ─────────────────────────────────────────────

def _vlog(msg: str, end: str = "\n") -> None:
    """Verbose-aware logger. Uses logger for full lines, print for inline progress."""
    if not ORCHESTRATOR_CONFIG["verbose"]:
        return
    if end == "\n":
        logger.info(msg.strip())
    else:
        # Inline progress (e.g. "[1/50] ") — print preserves the end char
        print(msg, end=end, flush=True)


def _detect_language(path: str) -> str:
    """
    Detect the programming language from a file extension.
    Standalone helper — no external class instantiation needed.
    """
    ext_map = {
        ".py":   "Python",     ".js":   "JavaScript", ".ts":  "TypeScript",
        ".jsx":  "JavaScript", ".tsx":  "TypeScript",  ".java": "Java",
        ".go":   "Go",         ".rs":   "Rust",        ".cpp": "C++",
        ".c":    "C",          ".cs":   "C#",          ".rb":  "Ruby",
        ".php":  "PHP",        ".swift":"Swift",        ".kt":  "Kotlin",
        ".sh":   "Shell",      ".sql":  "SQL",          ".html":"HTML",
        ".css":  "CSS",        ".md":   "Markdown",     ".vue": "Vue",
        ".svelte":"Svelte",    ".yaml": "YAML",         ".yml": "YAML",
        ".toml": "TOML",       ".json": "JSON",         ".env": "Shell",
        ".scss": "CSS",        ".sass": "CSS",          ".less": "CSS",
    }
    return ext_map.get(Path(path).suffix.lower(), "Unknown")


def _truncate_commit_msg(msg: str, limit: int = COMMIT_MSG_LIMIT) -> str:
    """Truncate a commit message to the recommended subject-line length."""
    return msg if len(msg) <= limit else msg[: limit - 3] + "..."


def _sleep_between_files() -> None:
    """Sleep with jitter — avoids bursty rate-limit hits on busy repos."""
    base = ORCHESTRATOR_CONFIG["delay_between_files"]
    time.sleep(base + random.uniform(0.0, 0.5))


# ─────────────────────────────────────────────
# DATABASE HELPERS
# ─────────────────────────────────────────────

@contextmanager
def get_db(path: str = ORCHESTRATOR_DB):
    """
    Thread-safe SQLite context manager with WAL mode, auto-commit and close.

    FIX 1 (Minimax P0): Added check_same_thread=False and WAL journal mode.
    WAL allows concurrent readers while a writer is active — critical for
    the webhook server and background orchestrator running in the same process.
    """
    conn = sqlite3.connect(path, check_same_thread=False)
    conn.row_factory = sqlite3.Row   # allows col access by name AND index
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA busy_timeout=5000")   # wait up to 5 s on locked DB
    try:
        yield conn
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def _prune_old_rows(conn: sqlite3.Connection) -> None:
    """Delete rows older than db_retention_days. Called inside init."""
    cutoff = (
        datetime.now(timezone.utc)
        - timedelta(days=ORCHESTRATOR_CONFIG["db_retention_days"])
    ).isoformat()
    # Prune api_spend — grows unbounded otherwise
    conn.execute("DELETE FROM api_spend WHERE recorded_at < ?", (cutoff,))
    # Prune non-committed file_runs — keep committed rows for audit trail
    conn.execute(
        "DELETE FROM file_runs WHERE created_at < ? AND committed = 0",
        (cutoff,),
    )


def init_orchestrator_db() -> None:
    """
    Create all required tables if they do not already exist, then migrate.
    _DB_INITIALIZED guard prevents redundant schema work on every API call.
    """
    global _DB_INITIALIZED
    with _DB_LOCK:
        if _DB_INITIALIZED:
            return

        with get_db() as conn:
            c = conn.cursor()

            # Full-run summary log
            c.execute("""
                CREATE TABLE IF NOT EXISTS runs (
                    id                  INTEGER PRIMARY KEY AUTOINCREMENT,
                    repo_name           TEXT,
                    trigger             TEXT,
                    files_total         INTEGER,
                    files_refined       INTEGER,
                    files_skipped       INTEGER,
                    files_failed        INTEGER,
                    avg_self_score      REAL,
                    rules_applied       INTEGER,
                    rules_learned       INTEGER,
                    total_duration_sec  REAL,
                    started_at          TEXT,
                    finished_at         TEXT,
                    commit_branch       TEXT DEFAULT ''
                )
            """)

            # Per-file detailed log
            c.execute("""
                CREATE TABLE IF NOT EXISTS file_runs (
                    id              INTEGER PRIMARY KEY AUTOINCREMENT,
                    run_id          INTEGER,
                    repo_name       TEXT,
                    file_path       TEXT,
                    language        TEXT,
                    self_score      REAL,
                    iterations      INTEGER,
                    rules_applied   INTEGER,
                    committed       INTEGER,
                    commit_sha      TEXT,
                    duration_sec    REAL,
                    created_at      TEXT,
                    error_msg       TEXT DEFAULT '',
                    FOREIGN KEY (run_id) REFERENCES runs(id)
                )
            """)

            # Intelligence growth tracker
            c.execute("""
                CREATE TABLE IF NOT EXISTS intelligence_growth (
                    id              INTEGER PRIMARY KEY AUTOINCREMENT,
                    run_number      INTEGER,
                    avg_score       REAL,
                    total_rules     INTEGER,
                    total_patterns  INTEGER,
                    recorded_at     TEXT
                )
            """)

            # Run lock — one row per named lock
            c.execute("""
                CREATE TABLE IF NOT EXISTS run_locks (
                    id          INTEGER PRIMARY KEY AUTOINCREMENT,
                    lock_name   TEXT UNIQUE,
                    acquired_at TEXT,
                    pid         INTEGER
                )
            """)

            # API spend tracking — pruned on a rolling window
            c.execute("""
                CREATE TABLE IF NOT EXISTS api_spend (
                    id          INTEGER PRIMARY KEY AUTOINCREMENT,
                    model       TEXT NOT NULL,
                    cost        REAL NOT NULL DEFAULT 0.0,
                    recorded_at TEXT NOT NULL
                )
            """)

            _prune_old_rows(conn)

        # Safe column-level migrations for existing deployments
        _migrate_db()

        # Performance indexes
        with get_db() as conn:
            conn.execute("""
                CREATE INDEX IF NOT EXISTS idx_file_runs_path
                ON file_runs (file_path, committed, created_at DESC)
            """)
            conn.execute("""
                CREATE INDEX IF NOT EXISTS idx_runs_started
                ON runs (started_at DESC)
            """)

        _DB_INITIALIZED = True
        logger.info("Orchestrator DB initialised at %s", ORCHESTRATOR_DB)


def _migrate_db() -> None:
    """
    Add missing columns to existing tables without destroying data.

    Column names and types come from the hardcoded _DB_MIGRATIONS list —
    never from user input — so f-string SQL construction is safe here.
    """
    with get_db() as conn:
        existing_cols: dict[str, set] = {}
        for table, col, col_type in _DB_MIGRATIONS:
            if table not in existing_cols:
                rows = conn.execute(f"PRAGMA table_info({table})").fetchall()
                existing_cols[table] = {r[1] for r in rows}
            if col not in existing_cols[table]:
                conn.execute(f"ALTER TABLE {table} ADD COLUMN {col} {col_type}")
                logger.info("DB migration: added %s.%s (%s)", table, col, col_type)
                existing_cols[table].add(col)


# ─────────────────────────────────────────────
# RUN LOCK  (FIX 2 — Minimax P0)
# ─────────────────────────────────────────────

def acquire_run_lock(lock_name: str = "main") -> Tuple[bool, Optional[int]]:
    """
    Attempt to acquire a named run lock to prevent concurrent orchestration.

    TTL eviction: locks older than lock_ttl_seconds are considered stale
    and evicted before the INSERT attempt — handles server crash recovery
    without the 30-min manual wait.

    Returns:
        (True,  lock_id) — lock acquired, safe to proceed.
        (False, None)    — another active run is holding the lock.
    """
    ttl     = ORCHESTRATOR_CONFIG["lock_ttl_seconds"]
    expiry  = (datetime.now(timezone.utc) - timedelta(seconds=ttl)).isoformat()
    now_iso = datetime.now(timezone.utc).isoformat()

    try:
        with get_db() as conn:
            # Evict stale locks before attempting INSERT
            conn.execute(
                "DELETE FROM run_locks WHERE lock_name = ? AND acquired_at < ?",
                (lock_name, expiry),
            )
            try:
                conn.execute("""
                    INSERT OR FAIL INTO run_locks (lock_name, acquired_at, pid)
                    VALUES (?, ?, ?)
                """, (lock_name, now_iso, os.getpid()))
            except sqlite3.IntegrityError:
                row = conn.execute(
                    "SELECT acquired_at FROM run_locks WHERE lock_name = ?",
                    (lock_name,),
                ).fetchone()
                held_since = row[0] if row else "unknown"
                logger.warning(
                    "Run lock '%s' held since %s — aborting.", lock_name, held_since
                )
                return False, None

            lock_id: int = conn.execute(
                "SELECT id FROM run_locks WHERE lock_name = ?", (lock_name,)
            ).fetchone()[0]
            return True, lock_id
    except Exception:
        logger.exception("acquire_run_lock failed for '%s'", lock_name)
        return False, None


def release_run_lock(lock_name: str = "main") -> None:
    """Release a previously acquired run lock."""
    try:
        with get_db() as conn:
            conn.execute("DELETE FROM run_locks WHERE lock_name = ?", (lock_name,))
    except Exception:
        logger.exception("release_run_lock failed — lock may be stale")


def refresh_run_lock(lock_name: str = "main") -> None:
    """
    Extend an existing lock's TTL by updating acquired_at to now.
    Called every 10 files during a long run so the TTL-based eviction
    doesn't reclaim a legitimately active lock.
    """
    try:
        with get_db() as conn:
            cur = conn.execute(
                "UPDATE run_locks SET acquired_at = ? WHERE lock_name = ?",
                (datetime.now(timezone.utc).isoformat(), lock_name),
            )
            if cur.rowcount == 0:  # FIX: check inside with block — consistent with monetization.py pattern
                logger.warning(
                    "refresh_run_lock: no active lock for '%s' — possible crash?",
                    lock_name,
                )
    except Exception:
        logger.exception("refresh_run_lock failed for '%s'", lock_name)


# ─────────────────────────────────────────────
# API CALLER FACTORY
# ─────────────────────────────────────────────

class SmartAPICaller:
    """
    Routes each task to the most appropriate API backend:

    - Planning / Analysis  → Claude    (best reasoning)
    - Code Refining        → DeepSeek  (cost-efficient and powerful)
    - Verification         → Gemini Flash (fast and free)
    - Self-retrospect      → Claude    (honest self-evaluation)
    - Rule Extraction      → DeepSeek  (cheapest option)

    COST OPTIMIZATION: a budget cap is enforced. When estimated spend
    reaches MAX_BUDGET_USD the caller raises BudgetExceeded so the
    orchestrator can commit what it has and stop cleanly.
    """

    # Cost constants per call (USD) — update when provider pricing changes
    _COST: dict[str, float] = {
        "claude":            0.25,
        "deepseek":          0.08,
        "gemini":            0.00,
        "deepseek_cheap":    0.03,
        "deepseek_verify":   0.02,
        "claude_analysis":   0.15,
        "deepseek_analysis": 0.05,
    }

    class BudgetExceeded(RuntimeError):
        """Raised when the configured API budget cap is hit."""

    def __init__(self) -> None:
        self.call_counts:   dict[str, int] = {"claude": 0, "deepseek": 0, "gemini": 0}
        self.cost_estimate: float = 0.0
        self._budget: float = ORCHESTRATOR_CONFIG["max_budget_usd"]

    def _track(self, provider: str, cost_key: str) -> None:
        cost = self._COST.get(cost_key, 0.0)
        self.call_counts[provider] += 1
        self.cost_estimate += cost
        if self._budget > 0 and self.cost_estimate >= self._budget:
            raise SmartAPICaller.BudgetExceeded(
                f"Budget cap ${self._budget:.2f} reached "
                f"(spent ${self.cost_estimate:.4f})"
            )
        # Persist spend to DB for cross-run cost visibility
        try:
            with get_db() as conn:
                conn.execute(
                    "INSERT INTO api_spend (model, cost, recorded_at) VALUES (?, ?, ?)",
                    (cost_key, cost, datetime.now(timezone.utc).isoformat()),
                )
        except Exception:
            logger.warning("Failed to record api_spend for model '%s'", cost_key)

    def for_refining(self, prompt: str, max_tokens: int = 4000) -> Optional[str]:
        """DeepSeek primary, Claude fallback."""
        result = call_deepseek(prompt, max_tokens=max_tokens)
        if result:
            self._track("deepseek", "deepseek")
            return result
        result = call_claude(prompt, max_tokens=max_tokens)
        if result:
            self._track("claude", "claude")
        return result

    def for_analysis(self, prompt: str, max_tokens: int = 1000) -> Optional[str]:
        """Claude primary, DeepSeek fallback."""
        result = call_claude(prompt, max_tokens=max_tokens)
        if result:
            self._track("claude", "claude_analysis")
            return result
        result = call_deepseek(prompt, max_tokens=max_tokens)
        if result:
            self._track("deepseek", "deepseek_analysis")
        return result

    def for_verification(self, prompt: str, max_tokens: int = 500) -> Optional[str]:
        """Gemini primary, DeepSeek fallback."""
        result = call_gemini(prompt, max_tokens=max_tokens)
        if result:
            self._track("gemini", "gemini")
            return result
        result = call_deepseek(prompt, max_tokens=max_tokens)
        if result:
            self._track("deepseek", "deepseek_verify")
        return result

    def general(self, prompt: str, max_tokens: int = 800) -> Optional[str]:
        """DeepSeek primary, Claude fallback (cheapest path)."""
        result = call_deepseek(prompt, max_tokens=max_tokens)
        if result:
            self._track("deepseek", "deepseek_cheap")
            return result
        result = call_claude(prompt, max_tokens=max_tokens)
        if result:
            self._track("claude", "claude")
        return result

    def get_usage_report(self) -> dict:
        return {
            "calls":              self.call_counts,
            "total_calls":        sum(self.call_counts.values()),
            "estimated_cost_usd": round(self.cost_estimate, 4),
            "budget_usd":         self._budget if self._budget > 0 else "unlimited",
        }


# ─────────────────────────────────────────────
# MASTER ORCHESTRATOR
# ─────────────────────────────────────────────

class MasterOrchestrator:
    """
    Coordinates the three sub-systems for every file in the pipeline:

    ┌─────────────────────────────────────────┐
    │ 1. Fine Tuning Layer                    │
    │    ├─ Load style profile                │
    │    ├─ Load company standards            │
    │    ├─ Load project context              │
    │    └─ Build enhanced prompt             │
    │                                         │
    │ 2. Self-Learning Engine                 │
    │    ├─ Inject learned rules              │
    │    ├─ Refine code (DeepSeek)            │
    │    ├─ Self-retrospect (Claude)          │
    │    ├─ Re-refine if score < 6.5          │
    │    └─ Persist new patterns to memory    │
    │                                         │
    │ 3. Final Verification (Gemini)          │
    │    └─ Safe to commit?                   │
    │                                         │
    │ 4. GitHub Commit                        │
    └─────────────────────────────────────────┘
    """

    def __init__(self) -> None:
        init_orchestrator_db()
        self.api       = SmartAPICaller()
        github_token = CONFIG.get("GITHUB_TOKEN", "")
        github_repo  = CONFIG.get("GITHUB_REPO", "")
        if not github_token or not github_repo:
            raise RuntimeError(
                "CONFIG missing GITHUB_TOKEN or GITHUB_REPO — check cleaner_bot.py"
            )
        self.gh        = GitHubClient(github_token, github_repo)
        self.repo_name = github_repo

        # Contract guard — fail fast at startup, not mid-run when budget is spent
        _required_gh_methods = [
            "get_repo_files", "get_file_content", "commit_file",
            "branch_exists", "create_branch",
        ]
        missing = [m for m in _required_gh_methods if not hasattr(self.gh, m)]
        if missing:
            raise RuntimeError(
                f"GitHubClient is missing required methods: {missing}. "
                "Check your cleaner_bot.py version."
            )

        self.fine_tuner   = FineTuningLayer(self.api.for_analysis, self.repo_name)
        self.self_learner = SelfLearningEngine(self.api.general)

        # ── Cost Optimizer — pre-flight routing + adaptive learning ──────────
        # caller=None: CostOptimizer still runs full local analysis (syntax,
        # bug patterns, nesting, duplicates). Budget enforcement is handled by
        # the orchestrator's own SmartAPICaller.BudgetExceeded — not duplicated.
        # LearningLog persists outcomes to cost_optimizer.db so adaptive routing
        # (files that historically fail → auto-escalate to HIGH mode) survives
        # server restarts on Render.
        if HAS_COST_OPTIMIZER:
            self._cost_log = CostLearningLog(db_path=COST_OPTIMIZER_DB)
            self.cost_opt  = CostOptimizer(
                caller=None,
                learning_log=self._cost_log,
                convergence_threshold=0.90,
            )
            logger.info("CostOptimizer initialised — pre-flight routing active")
        else:
            self._cost_log = None
            self.cost_opt  = None

        self._run_id:    Optional[int] = None
        self._run_stats: dict          = self._fresh_stats()

    # ── STATS ────────────────────────────────────────

    def _fresh_stats(self) -> dict:
        return {
            "files_total":    0,
            "files_refined":  0,
            "files_skipped":  0,
            "files_failed":   0,
            "scores":         [],
            "rules_applied":  0,
            "rules_learned":  0,
            "co_skipped_low": 0,   # files skipped by CostOptimizer LOW gate
            "co_issues_found":0,   # total local issues surfaced pre-flight
            "start_time":     time.time(),
        }

    # ── PHASE 0: SETUP ───────────────────────────────

    def setup(self, files: list, files_content: dict) -> None:
        """Initialize all sub-systems once before the per-file processing loop."""
        _vlog("🔧 Setting up orchestrator...")
        sample_files = [
            {"path": k, "content": v}
            for k, v in list(files_content.items())[: ORCHESTRATOR_CONFIG["style_sample_files"]]
        ]
        self.fine_tuner.setup(
            sample_files=sample_files,
            all_files_summary=files,
            repo_files_content=files_content,
        )
        _vlog("✅ All systems initialized\n")

    # ── PHASE 1: ORCHESTRATE ONE FILE ────────────────

    def orchestrate_file(
        self,
        file_info: dict,
        original_code: str,
        sha: str,
        all_files: list,
        commit_branch: str,
    ) -> tuple[bool, str, float]:
        """
        Run the full pipeline for a single file.

        Returns:
            (success, final_code, score)

        Pre-flight: CostOptimizer.decide() runs BEFORE any API call.
        - LOCAL analysis (syntax, bug patterns, nesting) at zero API cost.
        - balance_mode="low" → file is clean enough to skip the API pipeline.
        - local_issues → injected into the refine prompt so the LLM knows
          exactly what to fix.
        - LearningLog records outcomes for adaptive routing over time.
        """
        path       = file_info["path"]
        language   = _detect_language(path)
        file_start = time.time()

        _vlog(f"\n{'─' * 55}")
        _vlog(f"📄 {path} ({language})")

        # COST OPTIMIZATION: skip files too small to be worth an API call
        byte_len = len(original_code.encode())
        if byte_len < ORCHESTRATOR_CONFIG["min_file_bytes"]:
            _vlog(f"  ⏭️  File too small ({byte_len} bytes) — skipping")
            self._run_stats["files_skipped"] += 1
            return False, original_code, 10.0

        # ── PRE-FLIGHT: CostOptimizer routing decision ────────────────────────
        co_decision: dict = {}
        if self.cost_opt is not None:
            co_decision = self.cost_opt.decide(path, original_code)
            balance     = co_decision.get("balance_mode", "medium")
            run_id      = co_decision.get("run_id", "")
            co_issues   = co_decision.get("local_issues", [])
            co_fixes    = co_decision.get("suggested_fixes", [])

            if co_issues:
                self._run_stats["co_issues_found"] += len(co_issues)
                _vlog(
                    f"  🔍 Pre-flight [{run_id}]: {len(co_issues)} local issue(s) "
                    f"— {co_issues[0][:60]}"
                )

            # LOW mode: file is already clean — API pipeline not worth the cost.
            # Record as skip so the run log is honest.
            if balance == "low":
                _vlog(
                    f"  ✅ CostOpt: LOW — clean file, skipping API pipeline "
                    f"[{run_id}]"
                )
                self._run_stats["files_skipped"]  += 1
                self._run_stats["co_skipped_low"] += 1
                duration = time.time() - file_start
                self._log_file_run(path, language, duration, False, 10.0, 0)
                return False, original_code, 10.0

            _vlog(f"  📊 CostOpt: {balance.upper()} mode → entering pipeline")

        try:
            return self._run_file_pipeline(
                file_info, path, language, original_code, sha,
                all_files, commit_branch, file_start, co_decision,
            )
        except SmartAPICaller.BudgetExceeded as exc:
            _vlog(f"  💸 {exc}")
            logger.warning("Budget cap hit — halting file processing")
            raise  # propagate to run() so the loop can break cleanly
        except Exception as exc:
            _vlog(f"  💥 Unexpected error for {path}: {exc}")
            logger.exception("orchestrate_file crashed")
            self._run_stats["files_failed"] += 1
            duration = time.time() - file_start
            self._log_file_run(path, language, duration, False, 0.0, 0, error_msg=str(exc))
            return False, original_code, 0.0

    def _run_file_pipeline(
        self,
        file_info: dict,        # ← now passed through for SHA re-fetch (FIX 4)
        path: str,
        language: str,
        original_code: str,
        sha: str,
        all_files: list,
        commit_branch: str,
        file_start: float,
        co_decision: Optional[dict] = None,  # FIX v1.1: was dict=() (wrong type)
    ) -> tuple[bool, str, float]:
        """
        Inner pipeline logic — separated so orchestrate_file can wrap it cleanly
        with a single top-level exception handler.

        co_decision: result of CostOptimizer.decide() — carries local_issues,
        suggested_fixes, balance_mode, and run_id for structured logging.
        """
        # FIX v1.1: normalise to empty dict so .get() is always safe
        co_decision = co_decision or {}

        # Extract CostOptimizer findings (empty dict if optimizer unavailable)
        co_issues  = co_decision.get("local_issues",    [])
        co_fixes   = co_decision.get("suggested_fixes", [])
        co_balance = co_decision.get("balance_mode",    "medium")

        # FIX v1.1: use module-level hashlib (was: import hashlib as _hashlib here)
        _content_hash = hashlib.sha256(
            original_code.encode("utf-8", errors="replace")
        ).hexdigest()

        # ── Step 1: Build enhanced context ──────────────────────────────────
        _vlog("  [1/4] Building enhanced context...")
        enhanced_ctx = self.fine_tuner.build_enhanced_prompt(
            path, language, original_code, all_files
        )

        rule_markers = ["RULE:", "STANDARD:", "PATTERN:"]
        rules_in_ctx = sum(enhanced_ctx.count(m) for m in rule_markers)
        if rules_in_ctx:
            _vlog(f"        → {rules_in_ctx} context rules loaded")
            self._run_stats["rules_applied"] += rules_in_ctx

        # ── Inject CostOptimizer pre-flight findings ─────────────────────────
        # The LLM now knows EXACTLY what the local static analysis found,
        # so it can focus its refine effort on real issues rather than guessing.
        if co_issues:
            co_ctx  = "\n\n=== PRE-FLIGHT LOCAL ANALYSIS ===\n"
            co_ctx += "Static analysis found these issues — prioritize fixing them:\n"
            for issue, fix in zip(co_issues[:6], co_fixes[:6]):
                co_ctx += f"ISSUE: {issue}\nFIX:   {fix}\n"
            co_ctx += "=== END PRE-FLIGHT ===\n"
            enhanced_ctx += co_ctx
            _vlog(f"        → {len(co_issues)} pre-flight issue(s) injected into context")

        # ── Step 2: Self-learning refine ────────────────────────────────────
        _vlog("  [2/4] Self-learning refine...")
        full_input = f"{enhanced_ctx}\n\nCODE TO REFINE:\n{original_code}"

        refine_result = self.self_learner.refine(
            full_input, path, language, self.repo_name
        )

        # refine() may return a plain string or a (code, score, iterations) tuple
        if isinstance(refine_result, tuple):
            refined_code, self_score, iterations = refine_result
        else:
            refined_code = refine_result
            self_score   = 0.0
            iterations   = 1

        # Sanitize: guard against NaN, negative, or out-of-range scores
        try:
            self_score = float(self_score)
            if self_score != self_score or self_score < 0:  # NaN check
                self_score = 0.0
            self_score = min(self_score, 10.0)
        except (TypeError, ValueError):
            self_score = 0.0

        session_rules = getattr(self.self_learner, "session_rules_learned", 0)
        if session_rules:
            self._run_stats["rules_learned"] += session_rules

        if not refined_code or refined_code.strip() == original_code.strip():
            _vlog("  ✨ Already optimal — skipping")
            self._run_stats["files_skipped"] += 1
            duration = time.time() - file_start
            self._log_file_run(path, language, duration, False, 10.0, iterations)
            # Record skip as a pass (no refine needed) in CostLearningLog
            if self._cost_log is not None:
                self._cost_log.record(
                    _content_hash, "refine", success=True, score=1.0,
                    balance_mode=co_balance,
                )
            return False, original_code, 10.0

        refined_code = self._clean_code(refined_code, original_code)

        # ── Step 3: Final verification ───────────────────────────────────────
        # co_decision["should_verify"] gates whether the expensive Gemini
        # verification is worth running. HIGH mode always verifies; MEDIUM only
        # verifies when substantive issues were found.
        _should_verify = co_decision.get("should_verify", True) if co_decision else True
        _vlog(f"  [3/4] Final verification {'(running)' if _should_verify else '(skipped — CostOpt MEDIUM/clean)'}...")

        verification: dict = {}
        if _should_verify:
            try:
                verification = verify_refinement(original_code, refined_code, path)
            except Exception as exc:
                logger.warning("verify_refinement raised %s — treating as unsafe", exc)
                verification = {}
        else:
            # MEDIUM mode with no substantive issues: trust the refiner output
            # and skip the Gemini verification API call.
            verification = {"safe_to_commit": True, "reason": "skipped by CostOpt MEDIUM"}

        # Default False: if verify_refinement returns None/{} or crashes,
        # we treat that as "not safe" — fail-closed is correct for a security gate.
        if not verification or not verification.get("safe_to_commit", False):
            reason = (
                verification.get("reason", "verifier returned no result")
                if verification else "verifier raised exception"
            )
            _vlog(f"  ⛔ Verification failed: {reason}")
            self._run_stats["files_failed"] += 1
            duration = time.time() - file_start
            self._log_file_run(path, language, duration, False, self_score, iterations,
                               error_msg=f"verification: {reason}")
            # Record failure in CostLearningLog — adaptive routing will
            # escalate this file to HIGH mode on the next run.
            if self._cost_log is not None:
                self._cost_log.record(
                    _content_hash, "refine", success=False, score=None,
                    balance_mode=co_balance,
                )
            return False, original_code, 0.0

        improvements = verification.get("improvements_made", [])
        if improvements:
            _vlog(f"  ✅ Verified: {improvements[0]}")

        # ── Step 4: SHA re-fetch before commit (FIX 4 — Minimax P1) ─────────
        # Re-fetch the SHA so we don't send a stale sha= and get a 409 conflict.
        _vlog("  [4/4] Re-fetching SHA then committing...")
        try:
            _, fresh_sha = self.gh.get_file_content(file_info)
            if fresh_sha and fresh_sha != sha:
                _vlog(f"        → SHA changed mid-run — using fresh SHA")
                sha = fresh_sha
        except Exception as exc:
            logger.warning("SHA re-fetch failed (%s) — using cached SHA", exc)

        improvements_str = ", ".join(improvements[:2]) if improvements else "AI refinement"
        raw_msg    = f"🤖 [{language}] {Path(path).name}: {improvements_str}"
        commit_msg = _truncate_commit_msg(raw_msg)

        commit_sha = self.gh.commit_file(
            path, refined_code, sha, commit_branch, commit_msg
        )
        success  = bool(commit_sha)
        duration = time.time() - file_start

        if success:
            _vlog(f"  ✅ Committed! ({duration:.1f}s) — score {self_score:.1f}/10")
            self._run_stats["files_refined"] += 1
            self.fine_tuner.record_success(path, language, improvements, "orchestrator")
            self._log_file_run(
                path, language, duration, True,
                self_score, iterations, commit_sha,
            )
            # Record success in CostLearningLog.
            # Normalize score 0–10 → 0.0–1.0 for LearningLog's [0,1] contract.
            if self._cost_log is not None:
                self._cost_log.record(
                    _content_hash, "refine",
                    success=True,
                    score=round(min(self_score, 10.0) / 10.0, 3),
                    balance_mode=co_balance,
                )
            return True, refined_code, self_score
        else:
            _vlog(f"  ❌ Commit failed ({duration:.1f}s)")
            self._run_stats["files_failed"] += 1
            self._log_file_run(path, language, duration, False, self_score, iterations,
                               error_msg="commit returned no SHA")
            # Record commit failure — adaptive routing learns this file is risky.
            if self._cost_log is not None:
                self._cost_log.record(
                    _content_hash, "refine", success=False, score=None,
                    balance_mode=co_balance,
                )
            return False, original_code, 0.0

    # ── MAIN RUN ─────────────────────────────────────

    def run(
        self,
        trigger: str = "manual",
        changed_files: Optional[list] = None,
    ) -> None:
        """
        Execute a full orchestration run.

        Args:
            trigger:       Label for the run source (e.g. "manual", "push").
            changed_files: Specific paths to process (webhook mode), or None
                           for a full repository scan.
        """
        # FIX 2 (Minimax P0): prevent concurrent runs at the DB level
        acquired, _lock_id = acquire_run_lock("main")
        if not acquired:
            logger.warning("Another orchestration run is already active — aborting")
            return

        self._run_stats = self._fresh_stats()
        started_at = datetime.now(timezone.utc).isoformat()

        try:
            self._execute_run(trigger, changed_files, started_at)
        finally:
            release_run_lock("main")

    def _execute_run(
        self,
        trigger: str,
        changed_files: Optional[list],
        started_at: str,
    ) -> None:
        """Core run logic — separated so run() keeps the lock/unlock framing clean."""
        _print_banner()
        _vlog(f"🚀 Trigger : {trigger.upper()}")
        _vlog(f"⏰ Started : {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S UTC')}")
        _vlog(f"📁 Repo   : {self.repo_name}")

        _vlog("\n📥 Fetching repo files...")
        target_branch = CONFIG.get("TARGET_BRANCH", "main")
        all_files = self.gh.get_repo_files(target_branch)

        if changed_files:
            # Convert to set for O(1) lookup — list `in` is O(n) per file
            changed_set = set(changed_files)
            all_files   = [f for f in all_files if f["path"] in changed_set]
            _vlog(f"   🎯 Webhook mode: {len(all_files)} changed files")
        else:
            _vlog(f"   📂 Full scan: {len(all_files)} code files")

        if not all_files:
            _vlog("❌ No files to process")
            self._finalize_run(started_at, "")
            return

        # Apply safety cap before fetching file content
        all_files = all_files[: ORCHESTRATOR_CONFIG["max_files_per_run"]]
        self._run_stats["files_total"] = len(all_files)

        _vlog("\n📖 Loading file contents...")
        files_content: dict[str, str] = {}
        file_shas:     dict[str, str] = {}

        for f in all_files:
            content, sha = self.gh.get_file_content(f)
            if content:
                files_content[f["path"]] = content
                file_shas[f["path"]]     = sha

        _vlog(f"   {len(files_content)}/{len(all_files)} files loaded")

        self.setup(all_files, files_content)

        commit_branch = CONFIG.get("COMMIT_BRANCH", "").strip()
        if not commit_branch:
            logger.error("CONFIG['COMMIT_BRANCH'] is empty — aborting run")
            self._finalize_run(started_at, "")
            return
        if not self.gh.branch_exists(commit_branch):
            self.gh.create_branch(commit_branch, target_branch)
            _vlog(f"🌿 Created branch: {commit_branch}")

        self._run_id = self._create_run_record(started_at, trigger)

        _vlog(f"\n{'=' * 55}")
        _vlog(f"🔄 Processing {len(all_files)} files...")

        for i, file_info in enumerate(all_files, 1):
            path = file_info["path"]
            _vlog(f"\n[{i}/{len(all_files)}]", end=" ")

            original_code = files_content.get(path)
            sha           = file_shas.get(path)

            # Guard: skip files whose content or SHA could not be fetched.
            # `not sha` catches both None (key missing) and "" (empty string on error).
            if not original_code or not sha:
                _vlog(f"❌ Content/SHA unavailable: {path}")
                self._run_stats["files_failed"] += 1
                continue

            try:
                _, _, score = self.orchestrate_file(
                    file_info, original_code, sha, all_files, commit_branch
                )
            except SmartAPICaller.BudgetExceeded:
                _vlog("\n💸 Budget cap reached — stopping early and saving results")
                break

            if score > 0 and score < 10.0:
                # Exclude 10.0 — that's the sentinel for "already optimal / too small"
                # Including it would inflate avg_score in the final report
                self._run_stats["scores"].append(score)

            # Jitter sleep — avoids bursty rate-limit hits
            _sleep_between_files()

            # Refresh lock every 10 files — prevents TTL expiry on long runs
            if i % 10 == 0:
                refresh_run_lock()

        self._finalize_run(started_at, commit_branch)

    # ── WEBHOOK HANDLER ──────────────────────────────

    # Prefix used by _run_file_pipeline when building commit messages (line 583).
    # Must match exactly — this is what we filter on in the webhook handler.
    _BOT_COMMIT_PREFIX = "\U0001f916"  # 🤖

    def handle_webhook(self, payload: dict) -> None:
        """
        Entry point for a GitHub push webhook payload.

        Bot-loop guard: every commit made by this bot starts with _BOT_COMMIT_PREFIX.
        GitHub fires a push webhook for those commits too, which would re-trigger
        a full refine run. self_learner.refine() is called *before* the
        already-optimal check, so every unwanted loop spends real API budget
        (DeepSeek x N files + Claude style analysis) before terminating.

        We filter those commits here — before acquiring the run lock or touching
        any API — so bot commits cost exactly zero.
        """
        repo    = payload.get("repository", {}).get("full_name", "")
        commits = payload.get("commits", [])

        changed:    set[str] = set()
        skipped_bot = 0

        for commit in commits:
            msg = commit.get("message", "")
            if msg.startswith(self._BOT_COMMIT_PREFIX):
                # This commit was made by the bot — skip entirely.
                # DEBUG not INFO to avoid log noise on busy repos.
                logger.debug("handle_webhook: skipping bot commit — %.60s", msg)
                skipped_bot += 1
                continue
            changed.update(commit.get("modified", []))
            changed.update(commit.get("added",    []))

        if skipped_bot:
            logger.info(
                "handle_webhook: skipped %d bot commit(s) — no run triggered",
                skipped_bot,
            )

        if not changed:
            _vlog("\U0001f514 Webhook received — no human-authored changes to process")
            return

        _vlog(f"\n\U0001f514 Webhook: {repo} — {len(changed)} files changed")
        self.run(trigger="push", changed_files=list(changed))

    # ── HELPERS ──────────────────────────────────────

    def _clean_code(self, refined: str, original: str) -> str:
        """
        Strip markdown code fences and sanity-check the output length.

        Handles opening fences: ```python, ```js, ``` (plain).
        Handles closing fences: ``` with or without trailing language tag.
        If the cleaned result is less than 30% of the original length,
        the original is returned to prevent accidental data loss.
        """
        lines = refined.strip().split("\n")

        # Remove opening fence line (e.g. ```python, ```js, ```)
        if lines and lines[0].startswith("```"):
            lines = lines[1:]

        # Remove closing fence — last non-empty line starting with ```
        if lines and lines[-1].strip().startswith("```"):
            lines = lines[:-1]

        cleaned = "\n".join(lines).strip()

        if not cleaned:
            _vlog("  ⚠️  Cleaned code is empty — keeping original")
            return original

        if len(cleaned) < len(original) * 0.3:
            _vlog("  ⚠️  Cleaned code too short — keeping original")
            return original

        return cleaned

    # ── DATABASE OPERATIONS ──────────────────────────

    def _create_run_record(self, started_at: str, trigger: str) -> int:
        with get_db() as conn:
            c = conn.cursor()
            c.execute("""
                INSERT INTO runs
                    (repo_name, trigger, started_at,
                     files_total, files_refined, files_skipped,
                     files_failed, avg_self_score, rules_applied,
                     rules_learned, total_duration_sec, finished_at,
                     commit_branch)
                VALUES (?, ?, ?, 0, 0, 0, 0, 0, 0, 0, 0, '', ?)
            """, (self.repo_name, trigger, started_at, CONFIG.get("COMMIT_BRANCH", "")))
            return c.lastrowid

    def _log_file_run(
        self,
        path: str,
        language: str,
        duration: float,
        committed: bool,
        self_score: float = 0.0,
        iterations: int   = 1,
        commit_sha: str   = "",
        error_msg: str    = "",
    ) -> None:
        if not self._run_id:
            return
        with get_db() as conn:
            c = conn.cursor()
            c.execute("""
                INSERT INTO file_runs
                    (run_id, repo_name, file_path, language,
                     self_score, iterations, rules_applied,
                     committed, commit_sha, duration_sec, created_at, error_msg)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                self._run_id, self.repo_name, path, language,
                self_score, iterations, self._run_stats["rules_applied"],
                1 if committed else 0, commit_sha or "",
                duration, datetime.now(timezone.utc).isoformat(), error_msg,
            ))

    def _finalize_run(self, started_at: str, commit_branch: str) -> None:
        """Persist final run statistics and print the summary report."""
        duration  = time.time() - self._run_stats["start_time"]
        scores    = self._run_stats["scores"]
        avg_score = sum(scores) / len(scores) if scores else 0.0

        self._snapshot_intelligence()

        if self._run_id:
            with get_db() as conn:
                c = conn.cursor()
                c.execute("""
                    UPDATE runs SET
                        files_total        = ?,
                        files_refined      = ?,
                        files_skipped      = ?,
                        files_failed       = ?,
                        avg_self_score     = ?,
                        rules_applied      = ?,
                        rules_learned      = ?,
                        total_duration_sec = ?,
                        finished_at        = ?,
                        commit_branch      = ?
                    WHERE id = ?
                """, (
                    self._run_stats["files_total"],
                    self._run_stats["files_refined"],
                    self._run_stats["files_skipped"],
                    self._run_stats["files_failed"],
                    avg_score,
                    self._run_stats["rules_applied"],
                    self._run_stats["rules_learned"],
                    duration,
                    datetime.now(timezone.utc).isoformat(),
                    commit_branch,
                    self._run_id,
                ))

        usage = self.api.get_usage_report()

        _vlog(f"\n{'=' * 55}")
        _vlog("🏁 ORCHESTRATION COMPLETE")
        _vlog(f"{'=' * 55}")
        _vlog(f"  ✅ Refined & committed : {self._run_stats['files_refined']}")
        _vlog(f"  ✨ Already optimal     : {self._run_stats['files_skipped']}")
        _vlog(f"  🔒 CostOpt LOW skipped : {self._run_stats['co_skipped_low']}")
        _vlog(f"  🔍 Pre-flight issues   : {self._run_stats['co_issues_found']}")
        _vlog(f"  ❌ Failed              : {self._run_stats['files_failed']}")
        _vlog(f"  📊 Avg self-score      : {avg_score:.1f}/10")
        _vlog(f"  🧠 Rules applied       : {self._run_stats['rules_applied']}")
        _vlog(f"  📚 Rules learned       : {self._run_stats['rules_learned']}")
        _vlog(f"  ⏱️  Duration            : {duration:.1f}s")
        _vlog(f"{'─' * 55}")
        _vlog(
            f"  API calls — Claude: {usage['calls']['claude']} | "
            f"DeepSeek: {usage['calls']['deepseek']} | "
            f"Gemini: {usage['calls']['gemini']}"
        )
        _vlog(f"  💰 Est. cost           : ${usage['estimated_cost_usd']}")
        _vlog(f"  🏦 Budget cap          : ${usage['budget_usd']}")
        _vlog(f"  🌿 Branch              : {commit_branch}")
        _vlog(f"  🔗 github.com/{self.repo_name}/compare/{commit_branch}")
        _vlog(f"{'=' * 55}\n")

        self.self_learner.get_session_report()

        # Close CostLearningLog :memory: connection if present.
        # File-based LearningLog (COST_OPTIMIZER_DB) does not need explicit close.
        if self._cost_log is not None:
            self._cost_log.close()

    def _snapshot_intelligence(self) -> None:
        """Record a snapshot of the bot's intelligence growth to the database."""
        try:
            # FIX consistent: always use get_db() — never raw sqlite3.connect()
            with get_db(SELF_LEARNING_DB) as sl:
                c = sl.cursor()
                c.execute("SELECT COUNT(*) FROM finetune_memory")
                rules = c.fetchone()[0]
                c.execute("SELECT COUNT(*) FROM learned_patterns")
                patterns = c.fetchone()[0]
                c.execute("SELECT COUNT(*), AVG(self_score) FROM retrospect_log")
                row     = c.fetchone()
                run_num = row[0]
                avg     = row[1] or 0.0

            with get_db() as conn:
                c = conn.cursor()
                c.execute("""
                    INSERT INTO intelligence_growth
                        (run_number, avg_score, total_rules, total_patterns, recorded_at)
                    VALUES (?, ?, ?, ?, ?)
                """, (run_num, avg, rules, patterns, datetime.now(timezone.utc).isoformat()))
        except Exception:
            logger.exception("_snapshot_intelligence failed — skipping")

    @staticmethod
    def show_intelligence_growth() -> None:
        """Print a table showing how the bot's intelligence has grown over time."""
        try:
            with get_db() as conn:
                rows = conn.cursor().execute("""
                    SELECT run_number, avg_score, total_rules, total_patterns
                    FROM intelligence_growth
                    ORDER BY run_number
                """).fetchall()
        except Exception as exc:
            logger.error("Could not read intelligence_growth: %s", exc)
            return

        if not rows:
            _vlog("No intelligence data yet — run first!")
            return

        _vlog("\n📈 INTELLIGENCE GROWTH OVER TIME")
        _vlog(f"{'Run':>5} | {'Avg Score':>10} | {'Rules':>6} | {'Patterns':>9}")
        _vlog("─" * 40)
        for run, score, rules, patterns in rows:
            safe_score = max(0.0, min(float(score or 0), 10.0))
            bar = "█" * int(safe_score)
            _vlog(f"{run:>5} | {safe_score:>8.1f}/10 | {rules:>6} | {patterns:>9}  {bar}")


# ─────────────────────────────────────────────
# MODULE-LEVEL HELPERS
# ─────────────────────────────────────────────

def _print_banner() -> None:
    _vlog("""
╔══════════════════════════════════════════════════════╗
║      🤖 MASTER ORCHESTRATOR — PushClean Bot          ║
║   Fine Tuning + Self-Learning + Multi-API Fusion     ║
╚══════════════════════════════════════════════════════╝""")


# ─────────────────────────────────────────────
# ENTRY POINTS
# ─────────────────────────────────────────────

def run_manual() -> None:
    """Trigger a full repository scan manually."""
    MasterOrchestrator().run(trigger="manual")


def run_webhook(payload: dict) -> None:
    """Trigger an orchestration run from a GitHub webhook payload."""
    MasterOrchestrator().handle_webhook(payload)


def show_growth() -> None:
    """
    Display intelligence growth without triggering a full orchestration run.

    Reads only local databases — no GitHub API calls are made.
    """
    init_orchestrator_db()
    learner = SelfLearningEngine(SmartAPICaller().general)
    MasterOrchestrator.show_intelligence_growth()  # static — no GitHub init needed
    learner.show_learned_rules()


if __name__ == "__main__":
    commands = {
        "growth": show_growth,
        "manual": run_manual,
    }
    cmd  = sys.argv[1] if len(sys.argv) > 1 else "manual"
    func = commands.get(cmd)
    if func:
        func()
    else:
        print(f"Unknown command '{cmd}'. Available: {', '.join(commands)}")
