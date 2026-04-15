"""
╔══════════════════════════════════════════════════════════════════╗
║          MASTER ORCHESTRATOR — PushClean Bot                     ║
║                         FIXED v1.3                               ║
╚══════════════════════════════════════════════════════════════════╝
"""

from __future__ import annotations

import hashlib
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

from cleaner_bot import (
    GitHubClient, verify_refinement, CONFIG, _gemma_call
)
from fine_tuning_layer import FineTuningLayer
from self_learning import SelfLearningEngine

try:
    from cost_optimizer_v9 import (
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

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("orchestrator")

def _parse_budget() -> float:
    raw = os.getenv("MAX_BUDGET_USD") or "5.0"
    try:
        return float(raw)
    except ValueError:
        logger.warning("Invalid MAX_BUDGET_USD=%r — defaulting to 5.0", raw)
        return 5.0

ORCHESTRATOR_CONFIG: dict = {
    "min_score_to_commit":        6.5,
    "min_confidence_rule":        0.65,
    "style_sample_files":         8,
    "enable_standards_auto_load": True,
    "delay_between_files":        15,
    "max_files_per_run":          50,
    "max_budget_usd":             _parse_budget(),
    "min_file_bytes":             80,
    "lock_ttl_seconds":           600,
    "db_retention_days":          7,
    "verbose":    True,
}

try:
    from db_paths import (
        ORCHESTRATOR_DB,
        SELF_LEARNING_DB,
        COST_OPTIMIZER_DB,
        validate_data_dir as _validate_data_dir,
    )
    _validate_data_dir()
except ImportError:
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

_DB_INITIALIZED: bool = False
_DB_LOCK = threading.Lock()

def _vlog(msg: str, end: str = "\n") -> None:
    if not ORCHESTRATOR_CONFIG["verbose"]:
        return
    if end == "\n":
        logger.info(msg.strip())
    else:
        print(msg, end=end, flush=True)

def _detect_language(path: str) -> str:
    ext_map = {
        ".py": "Python", ".js": "JavaScript", ".ts": "TypeScript",
        ".jsx": "JavaScript", ".tsx": "TypeScript", ".java": "Java",
        ".go": "Go", ".rs": "Rust", ".cpp": "C++", ".c": "C", ".cs": "C#",
        ".rb": "Ruby", ".php": "PHP", ".swift": "Swift", ".kt": "Kotlin",
        ".sh": "Shell", ".sql": "SQL", ".html": "HTML", ".css": "CSS",
        ".md": "Markdown", ".vue": "Vue", ".svelte": "Svelte",
        ".yaml": "YAML", ".yml": "YAML", ".toml": "TOML", ".json": "JSON",
        ".env": "Shell", ".scss": "CSS", ".sass": "CSS", ".less": "CSS",
    }
    return ext_map.get(Path(path).suffix.lower(), "Unknown")

def _truncate_commit_msg(msg: str, limit: int = COMMIT_MSG_LIMIT) -> str:
    return msg if len(msg) <= limit else msg[: limit - 3] + "..."

def _sleep_between_files() -> None:
    base = ORCHESTRATOR_CONFIG["delay_between_files"]
    time.sleep(base + random.uniform(0.0, 0.5))

@contextmanager
def get_db(path: str = ORCHESTRATOR_DB):
    conn = sqlite3.connect(path, check_same_thread=False)
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

def _prune_old_rows(conn: sqlite3.Connection) -> None:
    cutoff = (
        datetime.now(timezone.utc)
        - timedelta(days=ORCHESTRATOR_CONFIG["db_retention_days"])
    ).isoformat()
    conn.execute("DELETE FROM api_spend WHERE recorded_at < ?", (cutoff,))
    conn.execute(
        "DELETE FROM file_runs WHERE created_at < ? AND committed = 0",
        (cutoff,),
    )

def init_orchestrator_db() -> None:
    global _DB_INITIALIZED
    with _DB_LOCK:
        if _DB_INITIALIZED:
            return
        with get_db() as conn:
            c = conn.cursor()
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
            c.execute("""
                CREATE TABLE IF NOT EXISTS run_locks (
                    id          INTEGER PRIMARY KEY AUTOINCREMENT,
                    lock_name   TEXT UNIQUE,
                    acquired_at TEXT,
                    pid         INTEGER
                )
            """)
            c.execute("""
                CREATE TABLE IF NOT EXISTS api_spend (
                    id          INTEGER PRIMARY KEY AUTOINCREMENT,
                    model       TEXT NOT NULL,
                    cost        REAL NOT NULL DEFAULT 0.0,
                    recorded_at TEXT NOT NULL
                )
            """)
            _prune_old_rows(conn)
        _migrate_db()
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

def acquire_run_lock(lock_name: str = "main") -> Tuple[bool, Optional[int]]:
    ttl     = ORCHESTRATOR_CONFIG["lock_ttl_seconds"]
    expiry  = (datetime.now(timezone.utc) - timedelta(seconds=ttl)).isoformat()
    now_iso = datetime.now(timezone.utc).isoformat()
    try:
        with get_db() as conn:
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
    try:
        with get_db() as conn:
            conn.execute("DELETE FROM run_locks WHERE lock_name = ?", (lock_name,))
    except Exception:
        logger.exception("release_run_lock failed — lock may be stale")

def refresh_run_lock(lock_name: str = "main") -> None:
    try:
        with get_db() as conn:
            cur = conn.execute(
                "UPDATE run_locks SET acquired_at = ? WHERE lock_name = ?",
                (datetime.now(timezone.utc).isoformat(), lock_name),
            )
            if cur.rowcount == 0:
                logger.warning(
                    "refresh_run_lock: no active lock for '%s' — possible crash?",
                    lock_name,
                )
    except Exception:
        logger.exception("refresh_run_lock failed for '%s'", lock_name)

class SmartAPICaller:
    _COST: dict[str, float] = {
        "claude": 0.25, "deepseek": 0.08, "gemini": 0.00,
        "deepseek_cheap": 0.03, "deepseek_verify": 0.02,
        "claude_analysis": 0.15, "deepseek_analysis": 0.05,
    }
    class BudgetExceeded(RuntimeError):
        pass

    def __init__(self) -> None:
        self.call_counts: dict[str, int] = {"claude": 0, "deepseek": 0, "gemini": 0}
        self.cost_estimate: float = 0.0
        self._budget: float = ORCHESTRATOR_CONFIG["max_budget_usd"]

    def _track(self, provider: str, cost_key: str) -> None:
        cost = self._COST.get(cost_key, 0.0)
        self.call_counts[provider] += 1
        self.cost_estimate += cost
        if self._budget > 0 and self.cost_estimate >= self._budget:
            raise SmartAPICaller.BudgetExceeded(
                f"Budget cap ${self._budget:.2f} reached (spent ${self.cost_estimate:.4f})"
            )
        try:
            with get_db() as conn:
                conn.execute(
                    "INSERT INTO api_spend (model, cost, recorded_at) VALUES (?, ?, ?)",
                    (cost_key, cost, datetime.now(timezone.utc).isoformat()),
                )
        except Exception:
            logger.warning("Failed to record api_spend for model '%s'", cost_key)

    def for_refining(self, prompt: str, max_tokens: int = 4000) -> Optional[str]:
        return _gemma_call(prompt, max_tokens)

    def for_analysis(self, prompt: str, max_tokens: int = 1000) -> Optional[str]:
        return _gemma_call(prompt, max_tokens)

    def for_verification(self, prompt: str, max_tokens: int = 500) -> Optional[str]:
        return _gemma_call(prompt, max_tokens)

    def general(self, prompt: str, max_tokens: int = 800) -> Optional[str]:
        return _gemma_call(prompt, max_tokens)

    def get_usage_report(self) -> dict:
        return {
            "calls": self.call_counts,
            "total_calls": sum(self.call_counts.values()),
            "estimated_cost_usd": round(self.cost_estimate, 4),
            "budget_usd": self._budget if self._budget > 0 else "unlimited",
    }class MasterOrchestrator:
    _BOT_COMMIT_PREFIX = "\U0001f916"

    def __init__(self) -> None:
        init_orchestrator_db()
        self.api = SmartAPICaller()
        github_token = CONFIG.get("GITHUB_TOKEN", "")
        github_repo  = CONFIG.get("GITHUB_REPO", "")
        if not github_token or not github_repo:
            raise RuntimeError("CONFIG missing GITHUB_TOKEN or GITHUB_REPO — check cleaner_bot.py")
        self.gh = GitHubClient(github_token, github_repo)
        self.repo_name = github_repo
        self.fine_tuner = FineTuningLayer(self.api.for_analysis, self.repo_name)
        self.self_learner = SelfLearningEngine(self.api.general)
        if HAS_COST_OPTIMIZER:
            self._cost_log = CostLearningLog(db_path=COST_OPTIMIZER_DB)
            self.cost_opt = CostOptimizer(
                caller=None,
                learning_log=self._cost_log,
                convergence_threshold=0.90,
            )
            logger.info("CostOptimizer initialised — pre-flight routing active")
        else:
            self._cost_log = None
            self.cost_opt = None
        self._run_id: Optional[int] = None
        self._run_stats: dict = self._fresh_stats()

    def _fresh_stats(self) -> dict:
        return {
            "files_total": 0, "files_refined": 0, "files_skipped": 0,
            "files_failed": 0, "scores": [], "rules_applied": 0,
            "rules_learned": 0, "co_skipped_low": 0, "co_issues_found": 0,
            "start_time": time.time(),
        }

    def setup(self, files: list, files_content: dict) -> None:
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

    def orchestrate_file(
        self,
        file_info: dict,
        original_code: str,
        sha: str,
        all_files: list,
        commit_branch: str,
    ) -> tuple[bool, str, float]:
        path = file_info["path"]
        language = _detect_language(path)
        file_start = time.time()
        _vlog(f"\n{'─' * 55}")
        _vlog(f"📄 {path} ({language})")
        byte_len = len(original_code.encode())
        if byte_len < ORCHESTRATOR_CONFIG["min_file_bytes"]:
            _vlog(f"  ⏭️  File too small ({byte_len} bytes) — skipping")
            self._run_stats["files_skipped"] += 1
            return False, original_code, 10.0
        co_decision: dict = {}
        if self.cost_opt is not None:
            co_decision = self.cost_opt.decide(path, original_code)
            balance = co_decision.get("balance_mode", "medium")
            run_id = co_decision.get("run_id", "")
            co_issues = co_decision.get("local_issues", [])
            if co_issues:
                self._run_stats["co_issues_found"] += len(co_issues)
                _vlog(f"  🔍 Pre-flight [{run_id}]: {len(co_issues)} local issue(s) — {co_issues[0][:60]}")
            if balance == "low":
                _vlog(f"  ✅ CostOpt: LOW — clean file, skipping API pipeline [{run_id}]")
                self._run_stats["files_skipped"] += 1
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
            raise
        except Exception as exc:
            _vlog(f"  💥 Unexpected error for {path}: {exc}")
            logger.exception("orchestrate_file crashed")
            self._run_stats["files_failed"] += 1
            duration = time.time() - file_start
            self._log_file_run(path, language, duration, False, 0.0, 0, error_msg=str(exc))
            return False, original_code, 0.0    def _run_file_pipeline(
        self,
        file_info: dict,
        path: str,
        language: str,
        original_code: str,
        sha: str,
        all_files: list,
        commit_branch: str,
        file_start: float,
        co_decision: Optional[dict] = None,
    ) -> tuple[bool, str, float]:
        import time
        import random
        import hashlib
        from pathlib import Path

        def _fetch_fresh_sha(attempts: int = 2) -> str:
            for attempt in range(attempts):
                try:
                    _, fresh = self.gh.get_file_content(file_info)
                    if fresh:
                        return fresh
                except Exception:
                    pass
                if attempt < attempts - 1:
                    time.sleep(1)
            return ""

        co_decision = co_decision or {}
        co_issues = co_decision.get("local_issues", [])
        co_fixes = co_decision.get("suggested_fixes", [])
        co_balance = co_decision.get("balance_mode", "medium")

        _content_hash = hashlib.sha256(
            original_code.encode("utf-8", errors="replace")
        ).hexdigest()

        _vlog("  [1/4] Building enhanced context...")
        enhanced_ctx = self.fine_tuner.build_enhanced_prompt(
            path, language, original_code, all_files
        )

        rule_markers = ["RULE:", "STANDARD:", "PATTERN:"]
        rules_in_ctx = sum(enhanced_ctx.count(m) for m in rule_markers)
        if rules_in_ctx:
            _vlog(f"        -> {rules_in_ctx} context rules loaded")
            self._run_stats["rules_applied"] += rules_in_ctx

        if co_issues:
            co_ctx = "\n\n=== PRE-FLIGHT LOCAL ANALYSIS ===\n"
            co_ctx += "Static analysis found these issues - prioritize fixing them:\n"
            for issue, fix in zip(co_issues[:6], co_fixes[:6]):
                co_ctx += f"ISSUE: {issue}\nFIX:   {fix}\n"
            co_ctx += "=== END PRE-FLIGHT ===\n"
            enhanced_ctx += co_ctx
            _vlog(f"        -> {len(co_issues)} pre-flight issue(s) injected into context")

        _vlog("  [2/4] Self-learning refine...")
        full_input = f"{enhanced_ctx}\n\nCODE TO REFINE:\n{original_code}"

        refine_result = self.self_learner.refine(
            full_input, path, language, self.repo_name
        )

        if isinstance(refine_result, tuple):
            refined_code, self_score, iterations = refine_result
        else:
            refined_code = refine_result
            self_score = 0.0
            iterations = 1

        try:
            self_score = float(self_score)
            if self_score != self_score or self_score < 0:
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
            if self._cost_log is not None:
                self._cost_log.record(
                    _content_hash, "refine", success=True, score=1.0,
                    balance_mode=co_balance,
                )
            return False, original_code, 10.0

        refined_code = self._clean_code(refined_code, original_code)

        _should_verify = co_decision.get("should_verify", True) if co_decision else True
        _vlog(f"  [3/4] Final verification {'(running)' if _should_verify else '(skipped — CostOpt MEDIUM/clean)'}...")

        verification: dict = {}
        if _should_verify:
            try:
                verification = verify_refinement(original_code, refined_code, path)
            except Exception as exc:
                logger.warning("verify_refinement raised %s - treating as unsafe", exc)
                verification = {}
        else:
            verification = {"safe_to_commit": True, "reason": "skipped by CostOpt MEDIUM"}

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
            if self._cost_log is not None:
                self._cost_log.record(
                    _content_hash, "refine", success=False, score=None,
                    balance_mode=co_balance,
                )
            return False, original_code, 0.0

        improvements = verification.get("improvements_made", [])
        if improvements:
            _vlog(f"  ✅ Verified: {improvements[0]}")

        improvements_str = ", ".join(improvements[:2]) if improvements else "AI refinement"
        raw_msg = f"🤖 [{language}] {Path(path).name}: {improvements_str}"
        commit_msg = _truncate_commit_msg(raw_msg)

        _vlog("  [4/4] Re-fetching SHA then committing...")

        max_commit_retries = 3
        base_delay = 1.0
        commit_success = False
        commit_sha = ""
        last_error = ""

        for attempt in range(max_commit_retries):
            try:
                fresh_sha = _fetch_fresh_sha()
                if not fresh_sha:
                    logger.warning(f"Could not fetch fresh SHA for {path}")
                    last_error = "sha_fetch_failed"
                    if attempt < max_commit_retries - 1:
                        continue
                    else:
                        break
                sha = fresh_sha

                commit_sha = self.gh.commit_file(
                    path, refined_code, sha, commit_branch, commit_msg
                )

                if commit_sha:
                    commit_success = True
                    break
                else:
                    last_error = "commit_returned_empty"
                    if attempt < max_commit_retries - 1:
                        sleep_time = (base_delay * (2 ** attempt)) + random.uniform(0, 0.5)
                        _vlog(f"        -> Commit failed, retrying in {sleep_time:.1f}s...")
                        time.sleep(sleep_time)
            except Exception as exc:
                error_str = str(exc)
                last_error = error_str
                if "409" in error_str or "does not match" in error_str.lower():
                    if attempt < max_commit_retries - 1:
                        sleep_time = (base_delay * (2 ** attempt)) + random.uniform(0, 0.5)
                        _vlog(f"        -> SHA conflict (409), retrying in {sleep_time:.1f}s...")
                        time.sleep(sleep_time)
                        continue
                logger.warning(f"Commit exception for {path}: {exc}")

        duration = time.time() - file_start

        if commit_success and commit_sha:
            _vlog(f"  ✅ Committed! ({duration:.1f}s) — score {self_score:.1f}/10")
            self._run_stats["files_refined"] += 1
            self._run_stats["scores"].append(self_score)
            self.fine_tuner.record_success(path, language, improvements, "orchestrator")
            self._log_file_run(
                path, language, duration, True,
                self_score, iterations, commit_sha,
            )
            if self._cost_log is not None:
                self._cost_log.record(
                    _content_hash, "refine",
                    success=True,
                    score=round(min(self_score, 10.0) / 10.0, 3),
                    balance_mode=co_balance,
                )
            if hasattr(self, "_file_shas_cache") and isinstance(self._file_shas_cache, dict):
                self._file_shas_cache[path] = commit_sha
            return True, refined_code, self_score
        else:
            _vlog(f"  ❌ Commit failed ({duration:.1f}s)")
            self._run_stats["files_failed"] += 1
            self._log_file_run(path, language, duration, False, self_score, iterations,
                               error_msg=last_error or "commit failed after retries")
            if self._cost_log is not None:
                self._cost_log.record(
                    _content_hash, "refine", success=False, score=None,
                    balance_mode=co_balance,
                )
            return False, original_code, 0.0

    def run(self, trigger: str = "manual", changed_files: Optional[list] = None) -> None:
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

    def _execute_run(self, trigger: str, changed_files: Optional[list], started_at: str) -> None:
        _print_banner()
        _vlog(f"🚀 Trigger : {trigger.upper()}")
        _vlog(f"⏰ Started : {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S UTC')}")
        _vlog(f"📁 Repo   : {self.repo_name}")
        _vlog("\n📥 Fetching repo files...")
        target_branch = CONFIG.get("TARGET_BRANCH", "main")
        all_files = self.gh.get_repo_files(target_branch)
        if changed_files:
            changed_set = set(changed_files)
            all_files = [f for f in all_files if f["path"] in changed_set]
            _vlog(f"   🎯 Webhook mode: {len(all_files)} changed files")
        else:
            _vlog(f"   📂 Full scan: {len(all_files)} code files")
        if not all_files:
            _vlog("❌ No files to process")
            self._finalize_run(started_at, "")
            return
        all_files = all_files[: ORCHESTRATOR_CONFIG["max_files_per_run"]]
        self._run_stats["files_total"] = len(all_files)
        _vlog("\n📖 Loading file contents...")
        files_content: dict[str, str] = {}
        file_shas: dict[str, str] = {}
        for f in all_files:
            content, sha = self.gh.get_file_content(f)
            if content:
                files_content[f["path"]] = content
                file_shas[f["path"]] = sha
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
            sha = file_shas.get(path)
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
                self._run_stats["scores"].append(score)
            _sleep_between_files()
            if i % 10 == 0:
                refresh_run_lock()
        self._finalize_run(started_at, commit_branch)

    def _clean_code(self, refined: str, original: str) -> str:
        lines = refined.strip().split("\n")
        if lines and lines[0].startswith("```"):
            lines = lines[1:]
        if lines and lines[-1].strip().startswith("```"):
            lines = lines[:-1]
        cleaned = "\n".join(lines).strip()
        if not cleaned or len(cleaned) < len(original) * 0.3:
            return original
        return cleaned

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
        iterations: int = 1,
        commit_sha: str = "",
        error_msg: str = "",
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
        duration = time.time() - self._run_stats["start_time"]
        scores = self._run_stats["scores"]
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
        _vlog(f"  API calls — Claude: {usage['calls']['claude']} | DeepSeek: {usage['calls']['deepseek']} | Gemini: {usage['calls']['gemini']}")
        _vlog(f"  💰 Est. cost           : ${usage['estimated_cost_usd']}")
        _vlog(f"  🏦 Budget cap          : ${usage['budget_usd']}")
        _vlog(f"  🌿 Branch              : {commit_branch}")
        _vlog(f"  🔗 github.com/{self.repo_name}/compare/{commit_branch}")
        _vlog(f"{'=' * 55}\n")
        self.self_learner.get_session_report()
        if self._cost_log is not None:
            self._cost_log.close()

    def _snapshot_intelligence(self) -> None:
        try:
            with get_db(SELF_LEARNING_DB) as sl:
                c = sl.cursor()
                c.execute("SELECT COUNT(*) FROM finetune_memory")
                rules = c.fetchone()[0]
                c.execute("SELECT COUNT(*) FROM learned_patterns")
                patterns = c.fetchone()[0]
                c.execute("SELECT COUNT(*), AVG(self_score) FROM retrospect_log")
                row = c.fetchone()
                run_num = row[0]
                avg = row[1] or 0.0
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

def _print_banner() -> None:
    _vlog("""
╔══════════════════════════════════════════════════════╗
║      🤖 MASTER ORCHESTRATOR — PushClean Bot          ║
║   Fine Tuning + Self-Learning + Multi-API Fusion     ║
╚══════════════════════════════════════════════════════╝""")

def run_manual() -> None:
    MasterOrchestrator().run(trigger="manual")

def run_webhook(payload: dict) -> None:
    MasterOrchestrator().handle_webhook(payload)

def show_growth() -> None:
    init_orchestrator_db()
    learner = SelfLearningEngine(SmartAPICaller().general)
    MasterOrchestrator.show_intelligence_growth()
    learner.show_learned_rules()

if __name__ == "__main__":
    commands = {"growth": show_growth, "manual": run_manual}
    cmd = sys.argv[1] if len(sys.argv) > 1 else "manual"
    func = commands.get(cmd)
    if func:
        func()
    else:
        print(f"Unknown command '{cmd}'. Available: {', '.join(commands)}")
