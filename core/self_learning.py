╔══════════════════════════════════════════════════════════════════╗
║         SELF-LEARNING ENGINE — Autonomous Retrospect             ║
║                                                                  ║
║  Flow per file:                                                  ║
║  1. refine()  → call DeepSeek with full context prompt           ║
║  2. score()   → Claude/Gemini self-evaluates output (0–10)       ║
║  3. if score < MIN_SCORE → retry (max MAX_ATTEMPTS)              ║
║  4. on success → extract patterns → save to learned_patterns     ║
║  5. log to retrospect_log for intelligence tracking              ║
║                                                                  ║
║  DB: SELF_LEARNING_DB → retrospect_log + learned_patterns        ║
╚══════════════════════════════════════════════════════════════════╝
"""

from __future__ import annotations

import json
import logging
import os
import re
import threading
from contextlib import contextmanager
from datetime import datetime, timezone
from typing import Optional, Callable
import sqlite3

logger = logging.getLogger("self_learning")

# ── Config ────────────────────────────────────────────────────────────────────
MIN_SCORE_TO_ACCEPT:  float = float(os.getenv("PUSHCLEAN_MIN_SCORE", "6.5"))
MAX_REFINE_ATTEMPTS:  int   = int(os.getenv("PUSHCLEAN_MAX_ATTEMPTS", "3"))
MIN_PATTERN_SCORE:    float = float(os.getenv("PUSHCLEAN_PATTERN_MIN", "8.0"))

# ── DB path ───────────────────────────────────────────────────────────────────
try:
    from db_paths import SELF_LEARNING_DB
except ImportError:
    _d = os.getenv("PUSHCLEAN_DATA_DIR", "").strip()
    SELF_LEARNING_DB = os.path.join(_d, "self_learning.db") if _d else "self_learning.db"

_SCHEMA_LOCK   = threading.Lock()
_SCHEMA_INITED = False


# ── DB helper ─────────────────────────────────────────────────────────────────

@contextmanager
def _get_db():
    conn = sqlite3.connect(SELF_LEARNING_DB, check_same_thread=False)
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

def _ensure_schema() -> None:
    global _SCHEMA_INITED
    with _SCHEMA_LOCK:
        if _SCHEMA_INITED:
            return
        with _get_db() as conn:
            # Retrospect log — every refine attempt recorded
            conn.execute("""
                CREATE TABLE IF NOT EXISTS retrospect_log (
                    id           INTEGER PRIMARY KEY AUTOINCREMENT,
                    repo_name    TEXT NOT NULL DEFAULT '',
                    file_path    TEXT NOT NULL,
                    language     TEXT NOT NULL,
                    self_score   REAL NOT NULL DEFAULT 0.0,
                    iterations   INTEGER NOT NULL DEFAULT 1,
                    accepted     INTEGER NOT NULL DEFAULT 0,
                    score_reason TEXT NOT NULL DEFAULT '',
                    recorded_at  TEXT NOT NULL
                )
            """)
            conn.execute("""
                CREATE INDEX IF NOT EXISTS idx_retro_repo_lang
                ON retrospect_log (repo_name, language, accepted)
            """)
            _SCHEMA_INITED = True
            logger.info("SelfLearning DB initialised at %s", SELF_LEARNING_DB)

# ── DB helper ─────────────────────────────────────────────────────────────────

@staticmethod
def _get_db():
    conn = sqlite3.connect(SELF_LEARNING_DB, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA busy_timeout=5000")
    try:
        return conn
    except Exception:
        return None

def _ensure_schema() -> None:
    global _SCHEMA_INITED
    with _SCHEMA_LOCK:
        if _SCHEMA_INITED:
            return
        with _get_db() as conn:
            # Retrospect log — every refine call
            conn.execute("""
                CREATE TABLE IF NOT EXISTS retrospect_log (
                    id           INTEGER PRIMARY KEY AUTOINCREMENT,
                    repo_name    TEXT NOT NULL DEFAULT '',
                    file_path    TEXT NOT NULL,
                    language     TEXT NOT NULL,
                    self_score   REAL NOT NULL DEFAULT 0.0,
                    iterations   INTEGER NOT NULL DEFAULT 1,
                    accepted     INTEGER NOT NULL DEFAULT 0,
                    score_reason TEXT NOT NULL DEFAULT '',
                    recorded_at  TEXT NOT NULL
                )
            """)
            conn.execute("""
                CREATE INDEX IF NOT EXISTS idx_retro_repo_lang
                ON retrospect_log (repo_name, language, accepted)
            """)
            _SCHEMA_INITED = True
            logger.info("SelfLearning DB initialised at %s", SELF_LEARNING_DB)

@staticmethod
def _extract_original_code(full_input: str) -> str:
    """
    Extract the CODE TO REFINE section from the full prompt.
    Falls back to last 2000 chars of the prompt.
    """
    marker = "CODE TO REFINE:"
    idx = full_input.find(marker)
    if idx != -1:
        return full_input[idx + len(marker):].strip()[:3000]
    # Fallback
    return full_input[-2000:].strip()

def _extract_patterns(
    self,
    language: str,
    original: str,
    refined: str,
    score: float,
    repo_name: str,
) -> int:
    """
    Ask the AI to extract reusable improvement rules from this refinement.
    Saves up to 3 rules to learned_patterns.
    """
    if not original or not refined:
        return 0

    prompt = f"""Compare BEFORE and AFTER {language} code. Extract up to 3 reusable improvement rules.
Each rule on its own line starting with RULE:
Rules must be specific (not generic). Under 15 words each.

BEFORE:
{original[:600]}

AFTER:
{refined[:600]}

Rules:"""
    try:
        response = self._api(prompt, max_tokens=250)
        if not response:
            return 0
        confidence = round(min(score / 10.0, 1.0), 2)
        saved = 0
        now = datetime.now(timezone.utc).isoformat()
        with _get_db() as conn:
            for line in response.splitlines():
                line = line.strip()
                if line.upper().startswith("RULE:"):
                    rule = line[5:].strip()
                    words = rule.split()
                    if 3 < len(words) < 20:
                        try:
                            conn.execute("""
                            INSERT INTO learned_patterns
                                (language, repo_name, pattern, confidence, created_at)
                                VALUES (?, ?, ?, ?, ?)
                            ON CONFLICT(language, pattern) DO UPDATE SET
                            used_count  = used_count + 1,
                            confidence  = MAX(confidence, excluded.confidence)
                            """, (language, repo_name, rule, confidence, now))
                            saved += 1
                        except Exception:
                            pass
        if saved:
            logger.debug("Learned %d new patterns from refinement", saved)
        return saved
    except Exception:
        logger.exception("_extract_patterns failed")
        return 0

@staticmethod
def _log_retrospect(
    repo_name: str,
    file_path: str,
    language: str,
    score: float,
    iterations: int,
    accepted: bool,
    reason: str,
) -> None:
    try:
        with _get_db() as conn:
            rows = conn.execute("""
                INSERT INTO retrospect_log (repo_name, file_path, language, self_score,
                        iterations, accepted, score_reason, recorded_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                repo_name, file_path, language,
                round(score, 2),
                iterations,
                accepted,
                reason,
                datetime.now(timezone.utc).isoformat(),
            ))
        logger.info(
            f"SelfLearning: Session {repo_name} {file_path} - Refine: {score:.1f}/10 - Iterations: {iterations} - Accepted: {accepted} - Rejected: {self._session_rejected}"
        )
    except Exception:
        logger.exception("_log_retrospect failed")

@staticmethod
def _extract_original_code(full_input: str) -> str:
    """
    Extract the CODE TO REFINE section from the full prompt.
    Falls back to last 2000 chars of the prompt.
    """
    marker = "CODE TO REFINE:"
    idx = full_input.find(marker)
    if idx != -1:
        return full_input[idx + len(marker):].strip()[:3000]
    # Fallback
    return full_input[-2000:].strip()

def _extract_patterns(
    self,
    language: str,
    original: str,
    refined: str,
    score: float,
    repo_name: str,
) -> int:
    """
    Ask the AI to extract reusable improvement rules from this refinement.
    Saves up to 3 rules to learned_patterns.
    """
    if not original or not refined:
        return 0

    prompt = f"""Compare BEFORE and AFTER {language} code. Extract up to 3 reusable improvement rules.
Each rule on its own line starting with RULE:
Rules must be specific (not generic). Under 15 words each.

BEFORE:
{original[:600]}

AFTER:
{refined[:600]}

Rules:"""
    try:
        response = self._api(prompt, max_tokens=250)
        if not response:
            return 0
        confidence = round(min(score / 10.0, 1.0), 2)
        saved = 0
        now = datetime.now(timezone.utc).isoformat()
        with _get_db() as conn:
            for line in response.splitlines():
                line = line.strip()
                if line.upper().startswith("RULE:"):
                    rule = line[5:].strip()
                    words = rule.split()
                    if 3 < len(words) < 20:
                        try:
                            conn.execute("""
                            INSERT INTO learned_patterns (language, repo_name, pattern, confidence, created_at)
                            VALUES (?, ?, ?, ?, ?)
                            ON CONFLICT(language, pattern) DO UPDATE SET
                            used_count  = used_count + 1,
                            confidence  = MAX(confidence, excluded.confidence)
                            """, (language, repo_name, rule, confidence, now))
                            saved += 1
                        except Exception:
                            pass
        if saved:
            logger.debug("Learned %d new patterns from refinement", saved)
        return saved
    except Exception:
        logger.exception("_extract_patterns failed")
        return 0