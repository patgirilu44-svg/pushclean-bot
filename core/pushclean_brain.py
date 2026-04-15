from __future__ import annotations

import ast
import difflib
import hashlib
import json
import logging
import os
import re
import threading
from datetime import datetime, timezone
from typing import Optional, Callable

# Hard dependency — orchestrator must export get_db(path) → context manager
from orchestrator import get_db

logger = logging.getLogger("pushclean_brain")

# FIX: use db_paths.py so PUSHCLEAN_DATA_DIR is respected on Render.
# v3 had BRAIN_DB = "refinex_brain.db" — always wrote to CWD (ephemeral),
# losing all brain data on every redeploy.
try:
    from db_paths import BRAIN_DB
except ImportError:
    _data_dir = os.getenv("PUSHCLEAN_DATA_DIR", "").strip()
    BRAIN_DB = os.path.join(_data_dir, "pushclean_brain.db") if _data_dir else "pushclean_brain.db"

_BRAIN_DB_INITIALIZED: bool = False
_BRAIN_DB_LOCK = threading.Lock()

# Confidence gate: refined output must meet this threshold to be accepted.
# Value in [0.0, 1.0] — derived from self_score / 10.
# Tune without redeploy via PUSHCLEAN_CONFIDENCE_THRESHOLD env var.
CONFIDENCE_ACCEPT_THRESHOLD: float = float(
    os.getenv("PUSHCLEAN_CONFIDENCE_THRESHOLD", "0.60")
)

# When _adaptive_behavior() detects a low-success pattern, post_refine
# applies this additive penalty to the confidence threshold.
# Tune via PUSHCLEAN_ADAPTIVE_PENALTY env var.
ADAPTIVE_CONFIDENCE_PENALTY: float = float(
    os.getenv("PUSHCLEAN_ADAPTIVE_PENALTY", "0.15")
)

# Adaptive suppression: if a (language, repo) combination has a historical
# success rate below this, apply stricter validation in post_refine.
# Tune without redeploy via PUSHCLEAN_ADAPTIVE_THRESHOLD env var.
ADAPTIVE_LOW_SUCCESS_THRESHOLD: float = float(
    os.getenv("PUSHCLEAN_ADAPTIVE_THRESHOLD", "0.35")
)


# ─────────────────────────────────────────────────────────────────
# INTERNAL HELPERS
# ─────────────────────────────────────────────────────────────────

def _safe_sha256(content: object) -> str:
    """
    Compute SHA-256 of content.
    HARDENED: validates that content is str or bytes before hashing.
    Never raises — returns empty string on invalid input.
    """
    if isinstance(content, str):
        data = content.encode("utf-8", errors="replace")
    elif isinstance(content, (bytes, bytearray)):
        data = bytes(content)
    else:
        logger.warning(
            "_safe_sha256: expected str/bytes, got %s — skipping hash",
            type(content).__name__,
        )
        return ""
    return hashlib.sha256(data).hexdigest()


def _clamp_score(raw: object) -> float:
    """
    Normalize and clamp a raw score value to [0.0, 1.0].
    HARDENED: handles None, NaN, strings, negatives, overflow.
    """
    try:
        v = float(raw)  # type: ignore[arg-type]
    except (TypeError, ValueError):
        return 0.0
    if v != v:  # NaN
        return 0.0
    return max(0.0, min(1.0, v))


def _validate_refined_code(refined_code: object) -> tuple[bool, str]:
    """
    HARDENED: Reject refined_code that is empty, non-string, or whitespace-only.
    Returns (is_valid, reason).
    """
    if not isinstance(refined_code, str):
        return False, f"refined_code_not_string: got {type(refined_code).__name__}"
    if not refined_code.strip():
        return False, "refined_code_empty_or_whitespace"
    return True, "ok"


def _compile_check_python(code: str) -> tuple[bool, str]:
    """
    Attempt to compile Python code via compile() — stricter than ast.parse()
    because it catches some errors that ast.parse() misses.
    Returns (ok, reason).
    """
    try:
        compile(code, "<refined>", "exec")
        return True, "compile_ok"
    except SyntaxError as exc:
        return False, f"compile_error: {exc}"
    except Exception as exc:
        return False, f"compile_unexpected: {exc}"


# ─────────────────────────────────────────────────────────────────
# MODULE 1 — PushCleanCollector
# ONLY JOB: Save data only
# ─────────────────────────────────────────────────────────────────

class PushCleanCollector:
    """
    Pure data sink. Accepts data, writes to DB. Nothing else.
    """

    def __init__(self) -> None:
        self._init_tables()

    def _init_tables(self) -> None:
        global _BRAIN_DB_INITIALIZED
        with _BRAIN_DB_LOCK:
            if _BRAIN_DB_INITIALIZED:
                return
            with get_db(BRAIN_DB) as conn:
                conn.execute("""
                    CREATE TABLE IF NOT EXISTS refinement_pairs (
                        id            INTEGER PRIMARY KEY AUTOINCREMENT,
                        repo_name     TEXT,
                        file_path     TEXT,
                        language      TEXT    NOT NULL,
                        original_code TEXT    NOT NULL,
                        refined_code  TEXT    NOT NULL,
                        self_score    REAL    DEFAULT 0.0,
                        confidence    REAL    DEFAULT 0.0,
                        accepted      INTEGER DEFAULT 0,
                        model_used    TEXT    DEFAULT 'deepseek',
                        created_at    TEXT    NOT NULL
                    )
                """)
                # Migration-safe: add confidence column if missing
                try:
                    conn.execute(
                        "ALTER TABLE refinement_pairs ADD COLUMN confidence REAL DEFAULT 0.0"
                    )
                except Exception:
                    pass  # Column already exists — expected on schema upgrades

                # v2 FIX: replaced INSERT OR IGNORE + UPDATE (two-query TOCTOU race) with
                # a single atomic UPSERT. Requires UNIQUE(language, error_type) —
                # guaranteed by _init_tables().
                conn.execute("""
                    CREATE TABLE IF NOT EXISTS validation_failures (
                        id          INTEGER PRIMARY KEY AUTOINCREMENT,
                        language    TEXT NOT NULL,
                        error_type  TEXT NOT NULL,
                        error_msg   TEXT,
                        file_path   TEXT,
                        occurrences INTEGER DEFAULT 1,
                        last_seen   TEXT NOT NULL,
                        UNIQUE(language, error_type)
                    )
                """)
                # Migration-safe unique index for existing tables without the constraint
                try:
                    conn.execute("""
                        CREATE UNIQUE INDEX IF NOT EXISTS idx_vf_lang_type
                        ON validation_failures (language, error_type)
                    """)
                except Exception:
                    pass  # Already present — safe to ignore

                conn.execute("""
                    CREATE TABLE IF NOT EXISTS learned_patterns (
                        id          INTEGER PRIMARY KEY AUTOINCREMENT,
                        language    TEXT    NOT NULL,
                        pattern     TEXT    NOT NULL UNIQUE,
                        confidence  REAL    DEFAULT 0.5,
                        used_count  INTEGER DEFAULT 0,
                        created_at  TEXT    NOT NULL
                    )
                """)

                # Failure rate tracking per (language, repo) for adaptive behavior
                conn.execute("""
                    CREATE TABLE IF NOT EXISTS adaptive_stats (
                        id           INTEGER PRIMARY KEY AUTOINCREMENT,
                        language     TEXT NOT NULL,
                        repo_name    TEXT NOT NULL DEFAULT '',
                        total        INTEGER DEFAULT 0,
                        accepted     INTEGER DEFAULT 0,
                        last_updated TEXT NOT NULL,
                        UNIQUE(language, repo_name)
                    )
                """)

            _BRAIN_DB_INITIALIZED = True

    def save_pair(
        self,
        repo_name: str,
        file_path: str,
        language: str,
        original_code: str,
        refined_code: str,
        self_score: float,
        accepted: bool,
        model_used: str = "deepseek",
        confidence: float = 0.0,
    ) -> None:
        """Save one refinement pair. No analysis — just store."""
        # HARDENED: validate types before storing
        valid_rc, rc_reason = _validate_refined_code(refined_code)
        if not valid_rc:
            logger.warning("Collector.save_pair: %s — not saving", rc_reason)
            return

        clamped_confidence = _clamp_score(confidence)

        try:
            with get_db(BRAIN_DB) as conn:
                conn.execute("""
                    INSERT INTO refinement_pairs
                        (repo_name, file_path, language,
                         original_code, refined_code,
                         self_score, confidence, accepted, model_used, created_at)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, (
                    repo_name or "", file_path or "", language,
                    original_code, refined_code,
                    float(self_score), clamped_confidence,
                    1 if accepted else 0,
                    model_used,
                    datetime.now(timezone.utc).isoformat(),
                ))

                # Update adaptive stats (atomic UPSERT)
                now = datetime.now(timezone.utc).isoformat()
                conn.execute("""
                    INSERT INTO adaptive_stats (language, repo_name, total, accepted, last_updated)
                    VALUES (?, ?, 1, ?, ?)
                    ON CONFLICT(language, repo_name) DO UPDATE SET
                        total        = total + 1,
                        accepted     = accepted + ?,
                        last_updated = ?
                """, (
                    language, repo_name or "",
                    1 if accepted else 0, now,
                    1 if accepted else 0, now,
                ))
        except Exception:
            logger.exception("Collector.save_pair failed")

    def save_failure(
        self,
        language: str,
        error_type: str,
        error_msg: str,
        file_path: str = "",
    ) -> None:
        """
        Record a validation failure.

        v2 FIX: replaced INSERT OR IGNORE + UPDATE (two-query TOCTOU race) with
        a single atomic ON CONFLICT DO UPDATE UPSERT. Requires UNIQUE(language,
        error_type) — guaranteed by _init_tables().
        """
        try:
            now = datetime.now(timezone.utc).isoformat()
            conn = get_db(BRAIN_DB)
            conn.execute("""
                INSERT INTO validation_failures
                    (language, error_type, error_msg, file_path, occurrences, last_seen)
                VALUES (?, ?, ?, ?, 1, ?)
                ON CONFLICT(language, error_type) DO UPDATE SET
                    occurrences = occurrences + 1,
                    last_seen   = excluded.last_seen,
                    error_msg   = excluded.error_msg
            """, (language, error_type, str(error_msg)[:300], file_path or "", now))
        except Exception:
            logger.exception("Collector.save_failure failed")

    def save_pattern(
        self,
        language: str,
        pattern: str,
        confidence: float,
    ) -> None:
        """Save one learned pattern. UNIQUE on pattern text — no duplicates."""
        try:
            clamped = _clamp_score(confidence)
            with get_db(BRAIN_DB) as conn:
                conn.execute("""
                    INSERT OR IGNORE INTO learned_patterns
                        (language, pattern, confidence, created_at)
                    VALUES (?, ?, ?, ?)
                """, (
                    language, pattern.strip(),
                    round(clamped, 2),
                    datetime.now(timezone.utc).isoformat(),
                ))
        except Exception:
            logger.exception("Collector.save_pattern failed")

    def increment_pattern_usage(self, pattern: str) -> None:
        """Track how often a pattern gets used in prompts."""
        try:
            with get_db(BRAIN_DB) as conn:
                conn.execute("""
                    UPDATE learned_patterns
                    SET used_count = used_count + 1
                    WHERE pattern = ?
                """, (pattern,))
        except Exception:
            logger.exception("Collector.increment_pattern_usage failed")

    def get_adaptive_success_rate(self, language: str, repo_name: str = "") -> Optional[float]:
        """
        Return historical success rate for (language, repo) pair.
        Returns None if insufficient data (<5 samples).
        """
        try:
            with get_db(BRAIN_DB) as conn:
                row = conn.execute("""
                    SELECT total, accepted FROM adaptive_stats
                    WHERE language = ? AND repo_name = ?
                """, (language, repo_name or "")).fetchone()
            if not row or row[0] < 10:  # MIN_SAMPLES=10 — consistent with cost_optimizer v9
                return None
            return row[1] / row[0]
        except Exception:
            logger.exception("Collector.get_adaptive_success_rate failed")
            return None

    def get_stats(self) -> dict:
        """Return raw counts. No analysis — just numbers."""
        try:
            with get_db(BRAIN_DB) as conn:
                pairs    = conn.execute(
                    "SELECT COUNT(*), AVG(self_score) FROM refinement_pairs "
                    "WHERE accepted = 1"
                ).fetchone()
                failures = conn.execute(
                    "SELECT COUNT(*) FROM validation_failures"
                ).fetchone()
                patterns = conn.execute(
                    "SELECT COUNT(*) FROM learned_patterns"
                ).fetchone()
            return {
                "accepted_pairs": pairs[0] or 0,
                "avg_score":      round(pairs[1] or 0.0, 2),
                "failure_types":  failures[0] or 0,
                "patterns":       patterns[0] or 0,
            }
        except Exception:
            return {}


# ─────────────────────────────────────────────────────────────────
# MODULE 2 — PushCleanValidator
# ONLY JOB: Check code. Return (is_valid, reason). Save nothing.
# ─────────────────────────────────────────────────────────────────

class PushCleanValidator:
    """
    Pure validation. Takes code in, returns verdict. Nothing else.
    HARDENED v2: compile check, confidence gate, empty/invalid rejection,
    size-aware length ceiling, scale-mismatch warning for confidence_gate.
    """

    def __init__(self, api_caller: Optional[Callable] = None) -> None:
        self._api = api_caller

    # ── Stage 1: Static Checks ────────────────────────────────────

    def static_check(
        self,
        language: str,
        original_code: str,
        refined_code: str,
    ) -> tuple[bool, str]:
        """
        HARDENED static validation — no API, no DB, instant.

        Checks:
        0. Type and emptiness of refined_code
        1. Length sanity (size-aware ceiling in v2)
        2. Language syntax
        3. Python compile check
        4. Critical functions preserved
        5. No placeholder text inserted
        6. No hardcoded secrets introduced
        """

        # CHECK 0: validate refined_code type and emptiness
        valid_rc, rc_reason = _validate_refined_code(refined_code)
        if not valid_rc:
            return False, rc_reason

        orig_len    = len(original_code.strip()) if isinstance(original_code, str) else 0
        refined_len = len(refined_code.strip())

        if orig_len == 0:
            return False, "original_code_empty: nothing to refine"
        if refined_len < orig_len * 0.5:
            return False, "refined_too_short: likely truncated by API"

        # v2 FIX: size-aware upper ceiling.
        # Old: refined_len > orig_len * 3 — too permissive for large files.
        # A 5,000-char file could balloon to 15,000 chars (hallucination territory).
        # New: for files > 2,000 chars, cap at 2x; for smaller files keep 3x or +500.
        if orig_len > 2000:
            max_refined = orig_len * 2
        else:
            max_refined = max(orig_len * 3, orig_len + 500)

        if refined_len > max_refined:
            return (
                False,
                f"refined_too_long: {refined_