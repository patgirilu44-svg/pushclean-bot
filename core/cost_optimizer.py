"""
╔══════════════════════════════════════════════════════════════════╗
║               COST OPTIMIZER — HARDENED v9                       ║
║                                                                  ║
║  HARDENING CHANGES vs v8:                                        ║
║  - print() pattern: word-boundary → negative lookbehind (?<![.w]) ║
║    (v8 flagged obj.print(), logger.print() as debug calls)       ║
║  - TODO/FIXME detection: strip string literals before scanning   ║
║    (v8 fired on '#TODO' inside URL strings and f-strings)        ║
║  - should_analyze key added to decision dict                     ║
║    (orchestrator reads this to gate the expensive analyze stage) ║
║  - AST-based checkers added for Python files:                    ║
║    · bare except / empty except body detection                   ║
║    · loop nesting depth (AST-based, not indent-based)            ║
║  - _count_duplicate_lines: keeps adjacent-pairs check (v8)       ║
║    + adds frequency-based check for scattered repeats (v6)       ║
║  - All v8 hardening retained                                     ║
╚══════════════════════════════════════════════════════════════════╝
"""
from __future__ import annotations

import ast
import hashlib
import logging
import os
import re
import sqlite3
import threading
import uuid
from collections import Counter
from contextlib import contextmanager
from datetime import datetime, timezone, timedelta
from pathlib import Path
from typing import Optional, Callable

logger = logging.getLogger("cost_optimizer")

COST_OPTIMIZER_DB: str
try:
    from db_paths import COST_OPTIMIZER_DB
except ImportError:
    _d = os.getenv("PUSHCLEAN_DATA_DIR", "").strip()
    COST_OPTIMIZER_DB = os.path.join(_d, "cost_optimizer.db") if _d else "cost_optimizer.db"

# ── Thread-safety ─────────────────────────────────────────────────────────────
_LEARNING_LOG_LOCK = threading.Lock()

# In-flight guard: prevents duplicate parallel processing of the same content hash
_IN_FLIGHT_LOCK = threading.Lock()
_IN_FLIGHT_HASHES: set[str] = set()


# ─────────────────────────────────────────────────────────────────
# HELPERS
# ─────────────────────────────────────────────────────────────────

def _compute_hash(content: object) -> str:
    """
    HARDENED: compute SHA-256 hash of content exactly once.
    Validates type before hashing. Returns "" on invalid input.
    """
    if isinstance(content, str):
        data = content.encode("utf-8", errors="replace")
    elif isinstance(content, (bytes, bytearray)):
        data = bytes(content)
    else:
        logger.warning(
            "_compute_hash: invalid type %s — returning empty hash",
            type(content).__name__,
        )
        return ""
    return hashlib.sha256(data).hexdigest()


def _safe_float(value: object, default: float = 0.0) -> float:
    """Convert value to float, clamping NaN to default."""
    try:
        v = float(value)  # type: ignore[arg-type]
        return default if v != v else v
    except (TypeError, ValueError):
        return default


@contextmanager
def _get_db(path: str = COST_OPTIMIZER_DB):
    """Thread-safe WAL SQLite context manager."""
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


# ─────────────────────────────────────────────────────────────────
# LEARNING LOG  (persistent outcome tracker)
# ─────────────────────────────────────────────────────────────────

class LearningLog:
    """
    Persistent log of refine outcomes — used for adaptive routing.
    HARDENED:
    - Thread-safe writes via _LEARNING_LOG_LOCK
    - Hash computed once by caller, not re-hashed here
    - All writes validated (success must be 0 or 1)
    - close() is idempotent
    """
    _DB_INITIALIZED: dict[str, bool] = {}
    _INIT_LOCK = threading.Lock()

    MIN_SAMPLES = 10  # minimum samples before success rate is meaningful

    def __init__(self, db_path: str = COST_OPTIMIZER_DB) -> None:
        self._db_path = db_path
        self._ensure_schema()

    def _ensure_schema(self) -> None:
        with self._INIT_LOCK:
            if self._db_path in self._DB_INITIALIZED:
                return
            try:
                with _get_db(self._db_path) as conn:
                    conn.execute("""
                        CREATE TABLE IF NOT EXISTS learning_log (
                            id           INTEGER PRIMARY KEY AUTOINCREMENT,
                            file_hash    TEXT    NOT NULL,
                            stage        TEXT    NOT NULL,
                            success      INTEGER NOT NULL DEFAULT 0,
                            score        REAL,
                            balance_mode TEXT,
                            recorded_at  TEXT    NOT NULL
                        )
                    """)
                    conn.execute("""
                        CREATE INDEX IF NOT EXISTS idx_ll_hash_stage
                        ON learning_log (file_hash, stage)
                    """)
                self._DB_INITIALIZED[self._db_path] = True
            except Exception:
                logger.exception("LearningLog._ensure_schema failed for %s", self._db_path)

    def record(
        self,
        file_hash: str,
        stage: str,
        success: bool,
        score: Optional[float] = None,
        balance_mode: Optional[str] = None,
    ) -> None:
        """
        HARDENED: thread-safe write with input validation.
        success is stored as 0/1 integer — never as Python bool directly.
        """
        if not isinstance(file_hash, str) or not file_hash:
            logger.warning("LearningLog.record: invalid file_hash — skipping")
            return
        success_int = 1 if success else 0
        score_val   = _safe_float(score) if score is not None else None

        with _LEARNING_LOG_LOCK:
            try:
                with _get_db(self._db_path) as conn:
                    conn.execute("""
                        INSERT INTO learning_log
                            (file_hash, stage, success, score, balance_mode, recorded_at)
                        VALUES (?, ?, ?, ?, ?, ?)
                    """, (
                        file_hash, stage, success_int, score_val,
                        balance_mode, datetime.now(timezone.utc).isoformat(),
                    ))
            except Exception:
                logger.exception("LearningLog.record failed for hash=%s", file_hash[:16])

    def get_success_rate(self, file_hash: str, stage: str = "refine") -> Optional[float]:
        """
        Return historical success rate for a (hash, stage) pair.
        Returns None if fewer than MIN_SAMPLES samples (insufficient data).

        v8: MIN_SAMPLES raised to 10 — 5 samples was statistically unreliable.
        A file could show 100% success rate from just 5 lucky runs.
        """
        if not file_hash:
            return None
        try:
            with _get_db(self._db_path) as conn:
                row = conn.execute("""
                    SELECT COUNT(*) as total, SUM(success) as wins
                    FROM learning_log
                    WHERE file_hash = ? AND stage = ?
                """, (file_hash, stage)).fetchone()
            if not row or row[0] < self.MIN_SAMPLES:
                return None
            return _safe_float(row[1]) / row[0]
        except Exception:
            logger.exception("LearningLog.get_success_rate failed")
            return None

    def get_global_success_rate(self, stage: str = "refine") -> Optional[float]:
        """
        Overall success rate across all files for a stage.
        Returns None if fewer than MIN_GLOBAL_SAMPLES total samples.

        v8: MIN_GLOBAL_SAMPLES raised to 30 for more reliable global signal.
        """
        MIN_GLOBAL_SAMPLES = 30
        try:
            with _get_db(self._db_path) as conn:
                row = conn.execute("""
                    SELECT COUNT(*) as total, SUM(success) as wins
                    FROM learning_log
                    WHERE stage = ?
                """, (stage,)).fetchone()
            if not row or row[0] < MIN_GLOBAL_SAMPLES:
                return None
            return _safe_float(row[1]) / row[0]
        except Exception:
            logger.exception("LearningLog.get_global_success_rate failed")
            return None

    def prune_old_records(self, days: int = 30) -> int:
        """Remove log entries older than `days`. Returns deleted count."""
        cutoff = (datetime.now(timezone.utc) - timedelta(days=days)).isoformat()
        with _LEARNING_LOG_LOCK:
            try:
                with _get_db(self._db_path) as conn:
                    result = conn.execute(
                        "DELETE FROM learning_log WHERE recorded_at < ?", (cutoff,)
                    )
                return result.rowcount
            except Exception:
                logger.exception("LearningLog.prune_old_records failed")
                return 0

    def close(self) -> None:
        """No-op — kept for API compatibility with callers that call .close()."""
        pass


# ─────────────────────────────────────────────────────────────────
# STATIC ANALYZER  (zero-cost local analysis)
# ─────────────────────────────────────────────────────────────────

class _StaticAnalyzer:
    """
    Pure local code analysis. No API calls. No DB writes.
    Returns (issues, fixes, issue_score).
    issue_score is a float in [0.0, 1.0]: 0=clean, 1=heavily broken.
    """

    # Pre-compiled patterns — compiled once at class definition time.
    #
    # v9 FIX — print pattern:
    #   Old: r'\bprint\s*\(' — \b matches word boundary before 'p' even after '.'
    #   So logger.print(x), obj.print(doc) were incorrectly flagged as debug calls.
    #   New: negative lookbehind (?<![.\w]) — only matches standalone print( calls.
    #
    # v9 FIX — TODO pattern:
    #   Old: r'#\s*TODO' — matched '#TODO' inside string literals and URLs.
    #   e.g. url = "http://host/#TODO" fired incorrectly.
    #   Moved TODO scanning to _count_todo_comments() which strips string literals first.
    #
    # v8 FIX — sleep pattern:
    #   Matches integers 5–9 AND any 2+-digit integer (10, 50, 100…)
    _BUG_PATTERNS: list[tuple[re.Pattern, str, str]] = [
        # v9: negative lookbehind prevents obj.print() false positives
        (re.compile(r'(?<![.\w])print\s*\(', re.MULTILINE),
         "debug print() call",
         "Remove or replace with logging"),
        (re.compile(r'except\s*:', re.MULTILINE),
         "bare except clause (catches everything)",
         "Catch specific exception types"),
        (re.compile(r'==\s*True\b', re.MULTILINE),
         "explicit comparison to True",
         "Use truthiness directly: if x:"),
        (re.compile(r'==\s*False\b', re.MULTILINE),
         "explicit comparison to False",
         "Use: if not x:"),
        (re.compile(r'==\s*None\b', re.MULTILINE),
         "equality comparison to None",
         "Use: if x is None:"),
        # v8: catches sleep(5), sleep(9), sleep(10), sleep(99), sleep(100)…
        (re.compile(r'time\.sleep\s*\(\s*(?:[5-9]|\d{2,})\s*\)', re.MULTILINE),
         "suspiciously long sleep() — 5 seconds or more",
         "Verify sleep duration is intentional; prefer event-driven patterns"),
    ]

    # Compiled once for string-literal stripping (TODO/FIXME detection)
    _STRIP_STRINGS_RE = re.compile(r'(["\'])(?:(?!\1).)*\1')
    _TODO_RE          = re.compile(r'#.*\b(?:TODO|FIXME)\b', re.IGNORECASE)
    _NESTING_THRESHOLD = 4  # indent levels

    def analyze(
        self,
        path: str,
        content: str,
    ) -> tuple[list[str], list[str], float]:
        """
        Run all local checks. Returns (issues, fixes, issue_score).
        HARDENED: validates content type before analysis.
        """
        if not isinstance(content, str):
            logger.warning(
                "StaticAnalyzer.analyze: content is %s — skipping",
                type(content).__name__,
            )
            return [], [], 0.0

        if not content.strip():
            return [], [], 0.0

        issues: list[str] = []
        fixes:  list[str] = []

        # 1. Bug patterns (all patterns except TODO — handled separately)
        for pattern, issue_text, fix_text in self._BUG_PATTERNS:
            matches = pattern.findall(content)
            if matches:
                issues.append(f"{issue_text} ({len(matches)}x)")
                fixes.append(fix_text)

        # 2. TODO/FIXME — v9: strip string literals line-by-line before scanning
        #    Prevents false positives from '#TODO' inside URLs, f-strings, docstrings.
        todo_count = self._count_todo_comments(content)
        if todo_count > 0:
            issues.append(f"TODO/FIXME comment(s) left in code ({todo_count}x)")
            fixes.append("Resolve each TODO/FIXME or open an issue tracker ticket")

        # 3. Nesting depth
        ext = Path(path).suffix.lower()
        if ext == ".py":
            # AST-based nesting (v9) — more accurate than indent-counting
            ast_issues, ast_fixes = self._ast_checks(content)
            issues.extend(ast_issues)
            fixes.extend(ast_fixes)
        else:
            # Fallback for non-Python: indent-based nesting
            max_depth = self._max_nesting(content)
            if max_depth > self._NESTING_THRESHOLD:
                issues.append(f"deep nesting detected (max {max_depth} levels)")
                fixes.append("Refactor deeply nested blocks into helper functions")

        # 4. Duplicate lines — adjacent pairs (hallucination signal) + frequency (scattered)
        adj_count  = self._count_duplicate_lines(content)
        freq_count = self._count_frequency_duplicates(content)
        if adj_count > 3:
            issues.append(f"duplicate adjacent lines ({adj_count} pairs)")
            fixes.append("Deduplicate via loops or helper functions")
        if freq_count > 0:
            issues.append(f"frequently repeated lines ({freq_count} line(s) appear 4+ times)")
            fixes.append("Extract repeated logic into a shared helper or constant")

        # Compute issue_score: 0.0 = no issues, approaches 1.0 with many issues
        issue_score = min(1.0, len(issues) / 10.0)
        return issues, fixes, issue_score

    def _count_todo_comments(self, content: str) -> int:
        """
        v9: Count TODO/FIXME only in real comment positions.
        Strips string literals from each line before scanning for '#' + TODO.
        Prevents false positives on '#TODO' inside URLs, f-strings, and docstrings.
        """
        count = 0
        for raw in content.splitlines():
            stripped = raw.strip()
            if stripped.startswith("#"):
                # Pure comment line — scan directly
                if self._TODO_RE.search(raw):
                    count += 1
            elif "#" in raw:
                # Possible inline comment — strip string literals first
                sanitised  = self._STRIP_STRINGS_RE.sub('""', raw)
                hash_pos   = sanitised.find("#")
                if hash_pos != -1:
                    inline_comment = sanitised[hash_pos:]
                    if self._TODO_RE.search(inline_comment):
                        count += 1
        return count

    def _ast_checks(self, content: str) -> tuple[list[str], list[str]]:
        """
        v9: AST-based checks for Python files.
        Runs after content is confirmed to be valid Python.
        Detects: bare except, empty except body, deep loop nesting.
        Returns (issues, fixes).
        """
        import ast as _ast

        issues: list[str] = []
        fixes:  list[str] = []

        try:
            tree = _ast.parse(content)
        except SyntaxError:
            return [], []

        # AST-based nesting depth
        def _loop_depth(node: _ast.AST, depth: int = 0) -> int:
            if isinstance(node, (_ast.For, _ast.While, _ast.AsyncFor)):
                depth += 1
            child_max = depth
            for child in _ast.iter_child_nodes(node):
                if isinstance(child, (_ast.FunctionDef, _ast.AsyncFunctionDef,
                                      _ast.ClassDef)):
                    continue  # don't cross function boundaries
                child_max = max(child_max, _loop_depth(child, depth))
            return child_max

        for node in _ast.walk(tree):
            if isinstance(node, (_ast.FunctionDef, _ast.AsyncFunctionDef)):
                depth = _loop_depth(node)
                if depth > self._NESTING_THRESHOLD:
                    lineno = getattr(node, "lineno", "?")
                    issues.append(
                        f"deep loop nesting in `{node.name}` "
                        f"(line {lineno}, depth={depth})"
                    )
                    fixes.append(
                        f"Refactor `{node.name}` — extract inner loops into helpers"
                    )

            if isinstance(node, _ast.ExceptHandler):
                lineno = getattr(node, "lineno", "?")
                if node.type is None:
                    issues.append(
                        f"bare `except:` at line {lineno} "
                        "(catches SystemExit, KeyboardInterrupt)"
                    )
                    fixes.append(
                        "Replace with `except Exception as exc:` and log the error"
                    )
                else:
                    # Check for empty body (pass-only or ellipsis-only)
                    body_stmts = [
                        s for s in node.body
                        if not isinstance(s, _ast.Pass)
                        and not (
                            isinstance(s, _ast.Expr)
                            and isinstance(s.value, _ast.Constant)
                            and s.value.value is ...
                        )
                    ]
                    if not body_stmts:
                        exc_name = getattr(node.type, "id", "Exception")
                        issues.append(
                            f"empty `except {exc_name}:` body at line {lineno} "
                            "(exception silently discarded)"
                        )
                        fixes.append(
                            f"Add at minimum `logger.exception(exc)` "
                            f"inside the except {exc_name} block"
                        )

        return issues, fixes

    def _count_frequency_duplicates(self, content: str) -> int:
        """
        v9: Frequency-based duplicate detection (from v6).
        Counts non-trivial lines (>15 chars) that appear 4+ times anywhere in file.
        Complements adjacent-pair detection — catches scattered repeated blocks.
        """
        freq: Counter = Counter()
        for raw in content.splitlines():
            stripped = raw.strip()
            if (
                stripped
                and not stripped.startswith("#")
                and len(stripped) > 15
                and stripped.lower() not in {
                    "pass", "return", "return none", "raise",
                    "break", "continue", "...",
                }
            ):
                freq[stripped] += 1
        return sum(1 for count in freq.values() if count >= 4)

    @staticmethod
    def _max_nesting(code: str) -> int:
        max_depth = 0
        for line in code.splitlines():
            stripped = line.lstrip()
            if not stripped:
                continue
            indent = len(line) - len(stripped)
            depth  = indent // 4
            if depth > max_depth:
                max_depth = depth
        return max_depth

    @staticmethod
    def _count_duplicate_lines(code: str) -> int:
        """
        Count adjacent identical lines.
        v8: minimum line length raised from 5 to 15 characters.
        This prevents false positives on common short lines (pass, }, ], return).
        """
        lines = [l.strip() for l in code.splitlines() if l.strip()]
        count = 0
        for i in range(1, len(lines)):
            if lines[i] == lines[i - 1] and len(lines[i]) > 15:
                count += 1
        return count


# ─────────────────────────────────────────────────────────────────
# COST OPTIMIZER  (main entry point)
# ─────────────────────────────────────────────────────────────────

class CostOptimizer:
    """
    Pre-flight routing intelligence. Decides whether to skip, analyze, or refine.

    HARDENED (v8):
    - _db_guard_available declared to False BEFORE the conditional block
      (v7 had a NameError risk: variable was only defined inside `if content_hash:`,
       but referenced in the finally clause which could theoretically be reached
       with content_hash truthy but the assignment missed — now impossible)
    - Hash computed ONCE per decide() call and reused everywhere
    - Duplicate processing prevention via _IN_FLIGHT_HASHES (mem) or DB
    - Decision uses expected_gain vs expected_cost logic
    - No unnecessary analyze calls on provably clean files
    - Thread-safe throughout
    - No silent failures

    Decision outputs (balance_mode):
      "low"    → file is clean; skip the API pipeline
      "medium" → some issues found; refine but skip heavy verification
      "high"   → significant issues or historically failing; full pipeline + verify
    """

    # Cost constants in abstract units (relative, not USD)
    _COST_SKIP   = 0.00
    _COST_MEDIUM = 0.30
    _COST_HIGH   = 1.00

    # Minimum expected gain to justify API call (medium mode)
    _MIN_GAIN_FOR_MEDIUM = 0.20

    def __init__(
        self,
        caller: Optional[Callable] = None,
        learning_log: Optional[LearningLog] = None,
        convergence_threshold: float = 0.90,
    ) -> None:
        self._caller    = caller
        self._log       = learning_log
        self._threshold = max(0.0, min(1.0, convergence_threshold))
        self._analyzer  = _StaticAnalyzer()

    def decide(self, path: str, content: object) -> dict:
        """
        Main entry point. Returns a routing decision dict.

        HARDENED (v8):
        - _db_guard_available initialised to False unconditionally before use
        - Validates content type before any analysis
        - Computes hash ONCE and passes it through
        - Checks duplicate in-flight processing
        - Applies expected_gain vs expected_cost logic

        Returns:
        {
            "balance_mode":    "low" | "medium" | "high",
            "should_analyze":  bool,
            "should_verify":   bool,
            "local_issues":    list[str],
            "suggested_fixes": list[str],
            "issue_score":     float,
            "run_id":          str,
            "content_hash":    str,
            "reason":          str,
        }
        """
        run_id = str(uuid.uuid4())[:8]

        # HARDENED: validate content type up front
        if not isinstance(content, str):
            logger.warning(
                "CostOptimizer.decide[%s]: content type %s — defaulting to high",
                run_id, type(content).__name__,
            )
            return self._decision("high", True, [], [], 0.5, run_id, "",
                                  "invalid_content_type", should_analyze=True)

        # HARDENED: compute hash ONCE
        content_hash = _compute_hash(content)

        # v8 FIX: declare _db_guard_available unconditionally so the finally
        # clause can always reference it safely, regardless of content_hash truthiness.
        _db_guard_available = False

        if content_hash:
            try:
                from webhook_server_fixed import _claim_inflight_db, _release_inflight_db
                _db_guard_available = True
            except ImportError:
                _db_guard_available = False

            if _db_guard_available:
                claimed = _claim_inflight_db(content_hash)
                if not claimed:
                    logger.info(
                        "CostOptimizer.decide[%s]: duplicate in-flight (DB) — skipping",
                        run_id,
                    )
                    return self._decision(
                        "low", False, [], [], 0.0, run_id, content_hash,
                        "duplicate_in_flight",
                        should_analyze=False,
                    )
            else:
                # Fallback: in-memory guard (single-process only)
                with _IN_FLIGHT_LOCK:
                    if content_hash in _IN_FLIGHT_HASHES:
                        logger.info(
                            "CostOptimizer.decide[%s]: duplicate in-flight (mem) — skipping",
                            run_id,
                        )
                        return self._decision(
                            "low", False, [], [], 0.0, run_id, content_hash,
                            "duplicate_in_flight",
                            should_analyze=False,
                        )
                    _IN_FLIGHT_HASHES.add(content_hash)

        try:
            return self._do_decide(path, content, content_hash, run_id)
        finally:
            if content_hash:
                try:
                    if _db_guard_available:
                        _release_inflight_db(content_hash)  # type: ignore[name-defined]
                    else:
                        with _IN_FLIGHT_LOCK:
                            _IN_FLIGHT_HASHES.discard(content_hash)
                except Exception:
                    pass

    def _do_decide(
        self,
        path: str,
        content: str,
        content_hash: str,
        run_id: str,
    ) -> dict:
        """
        Core decision logic — separated so the duplicate guard wrapper is clean.
        Uses expected_gain vs expected_cost to decide routing.
        """
        # ── Step 1: Check historical success rate (adaptive routing) ──────────
        if self._log and content_hash:
            hist_rate = self._log.get_success_rate(content_hash)
            if hist_rate is not None:
                if hist_rate < 0.35:
                    # This file has historically failed — escalate to HIGH
                    logger.info(
                        "CostOptimizer[%s]: historical fail rate %.0f%% → HIGH",
                        run_id, (1 - hist_rate) * 100,
                    )
                    return self._decision(
                        "high", True, [], [], hist_rate, run_id, content_hash,
                        f"historical_fail_rate={hist_rate:.2f}",
                        should_analyze=True,
                    )
                if hist_rate >= self._threshold:
                    # File has converged — skip
                    logger.info(
                        "CostOptimizer[%s]: converged (%.0f%% success) → LOW",
                        run_id, hist_rate * 100,
                    )
                    return self._decision(
                        "low", False, [], [], 0.0, run_id, content_hash,
                        f"converged_success_rate={hist_rate:.2f}",
                        should_analyze=False,
                    )

        # ── Step 2: Local static analysis (zero API cost) ─────────────────────
        issues, fixes, issue_score = self._analyzer.analyze(path, content)

        # ── Step 3: Expected gain vs expected cost decision ────────────────────
        # expected_gain: how much improvement do we expect?
        # Driven by issue_score (0=no issues, 1=heavily broken)
        # and global historical success rate (are we even effective globally?)
        expected_gain = issue_score
        if self._log:
            global_rate = self._log.get_global_success_rate()
            if global_rate is not None:
                # Scale expected gain by how often we actually succeed globally.
                # If system is globally failing, auto-reduce API usage.
                expected_gain = issue_score * global_rate

        # No issues found and no history of problems → skip
        if issue_score == 0.0 and not issues:
            return self._decision(
                "low", False, [], [], 0.0, run_id, content_hash,
                "no_local_issues_detected",
                should_analyze=False,
            )

        # Expected gain is too small to justify even medium API cost
        if expected_gain < self._MIN_GAIN_FOR_MEDIUM:
            return self._decision(
                "low", False, issues, fixes, issue_score, run_id, content_hash,
                f"expected_gain={expected_gain:.3f}_below_threshold",
                should_analyze=False,
            )

        # Significant issues (issue_score > 0.5) → full pipeline
        # should_analyze=True in high mode: file has enough issues to warrant
        # the expensive deep-analysis API call before refining.
        if issue_score > 0.5 or len(issues) >= 5:
            return self._decision(
                "high", True, issues, fixes, issue_score, run_id, content_hash,
                f"high_issue_score={issue_score:.2f}",
                should_analyze=True,
            )

        # Moderate issues → medium mode (refine but skip heavy analyze+verify)
        return self._decision(
            "medium",
            should_verify=issue_score > 0.3,
            issues=issues,
            fixes=fixes,
            issue_score=issue_score,
            run_id=run_id,
            content_hash=content_hash,
            reason=f"moderate_issues={len(issues)}",
            should_analyze=False,
        )

    @staticmethod
    def _decision(
        balance_mode: str,
        should_verify: bool,
        issues: list,
        fixes: list,
        issue_score: float,
        run_id: str,
        content_hash: str,
        reason: str,
        should_analyze: bool = False,
    ) -> dict:
        return {
            "balance_mode":    balance_mode,
            "should_analyze":  should_analyze,  # v9: orchestrator uses this to gate analyze stage
            "should_verify":   should_verify,
            "local_issues":    issues,
            "suggested_fixes": fixes,
            "issue_score":     round(issue_score, 3),
            "run_id":          run_id,
            "content_hash":    content_hash,
            "reason":          reason,
        }
