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
    issue_score is a float in [0.0, 1.0]: 0=clean, approaches 1.0 with many issues
    """

    # Pre-compiled patterns — compiled once at class definition time.
    #
    # v9 FIX — print pattern:
    #   Old: r'\bprint\s*\(' — \b matches word boundary before 'p' even after '.'
    #   So logger.print() and obj.print() were incorrectly flagged as debug calls.
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
                "StaticAnalyzer.analyze[%s]: content type %s — skipping" % (path, type(content).__name__),
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
        #    Prevents false positives from '#TODO' inside URLs, f-strings, and docstrings.
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
        Prevents false positives from '#TODO' inside URLs, f-strings, and docstrings.
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
            for child in _ast.iter_child_nodes(