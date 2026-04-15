"""
╔══════════════════════════════════════════════════════════════════╗
║                PUSHCLEAN BRAIN — HARDENED v3                       ║
║           Non-Overlapping Self-Learning System                   ║
║                                                                  ║
║  PushCleanCollector  → save data only                             ║
║  PushCleanValidator  → validate only                              ║
║  PushCleanContext    → build context only                         ║
║  PushCleanLearner    → extract patterns only                      ║
║  PushCleanBrain      → coordinate only                            ║
║                                                                  ║
║  HARDENING CHANGES vs v2:                                        ║
║  - secret_re SyntaxError fixed: ["\''] → triple-quoted string   ║
║    (v2 broke at import — file was completely unusable)           ║
║  - get_adaptive_success_rate MIN_SAMPLES: 5 → 10                 ║
║    (consistent with cost_optimizer v9; 5 samples too few for     ║
║     statistically reliable adaptive suppression)                 ║
║  - All v2 hardening retained                                     ║
╚══════════════════════════════════════════════════════════════════╝

DEPENDENCY NOTE:
  This module requires `orchestrator.py` to be present and to export
  a `get_db(db_path)` context manager returning a sqlite3 connection.
  All database operations in this file route through that utility.
"""

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
# ONLY JOB: Save raw data. No analysis. No learning. No validation.
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

                # v2: UNIQUE constraint on (language, error_type) enables atomic UPSERT
                # in save_failure(), eliminating the INSERT+UPDATE TOCTOU race.
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
            with get_db(BRAIN_DB) as conn:
                now = datetime.now(timezone.utc).isoformat()
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
        Return historical success rate for (language, repo_name).
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
                f"refined_too_long: {refined_len} chars > ceiling {max_refined} "
                f"(orig={orig_len}, likely hallucination)",
            )

        # CHECK 2: Syntax
        syntax_ok, syntax_msg = self._syntax_check(language, refined_code)
        if not syntax_ok:
            return False, syntax_msg

        # CHECK 3: Python compile check (stricter than ast.parse alone)
        if language == "Python":
            compile_ok, compile_msg = _compile_check_python(refined_code)
            if not compile_ok:
                return False, compile_msg

        # CHECK 4: Functions preserved
        funcs_ok, funcs_msg = self._functions_preserved(
            language, original_code, refined_code
        )
        if not funcs_ok:
            return False, funcs_msg

        # CHECK 5: No placeholders
        placeholder_patterns = [
            "TODO: implement",
            "pass  # implement",
            "# Your code here",
            "raise NotImplementedError",
        ]
        for p in placeholder_patterns:
            if p in refined_code and p not in original_code:
                return False, f"placeholder_inserted: {p}"

        # CHECK 6: No new hardcoded secrets
        secret_re = re.compile(
            r"""(password|api_key|secret|token)\s*=\s*["'][^"']{6,}["']""",
            re.IGNORECASE,
        )
        if secret_re.search(refined_code) and not secret_re.search(original_code):
            return False, "hardcoded_secret_detected"

        return True, "static_ok"

    def confidence_gate(
        self,
        self_score_raw: object,
        threshold: float = CONFIDENCE_ACCEPT_THRESHOLD,
    ) -> tuple[bool, float]:
        """
        HARDENED v2: Convert raw self_score (0–10 scale) to [0.0, 1.0] confidence.
        Reject if below threshold.

        v2 FIX: warns when raw > 10 — indicates the caller is passing a score on a
        different scale (0–100, or already normalised 0–1). Silent acceptance would
        produce garbage confidence values (e.g. raw=85 → confidence=min(8.5,1.0)=1.0).

        Returns (accepted, confidence).
        """
        try:
            raw = float(self_score_raw)  # type: ignore[arg-type]
        except (TypeError, ValueError):
            return False, 0.0
        if raw != raw:  # NaN
            return False, 0.0

        # v2: scale-mismatch guard
        if raw > 10:
            logger.warning(
                "confidence_gate: raw score %.1f exceeds 0–10 scale — "
                "possible scale mismatch (expected 0–10, got %.1f). "
                "Clamping to 1.0 confidence. Verify score source.",
                raw, raw,
            )

        # Score is on a 0–10 scale; normalize to [0.0, 1.0]
        confidence = _clamp_score(raw / 10.0)
        accepted = confidence >= threshold
        return accepted, confidence

    def _syntax_check(
        self, language: str, code: str
    ) -> tuple[bool, str]:
        if language == "Python":
            try:
                ast.parse(code)
            except SyntaxError as e:
                return False, f"syntax_error: {e}"

        elif language in ("JavaScript", "TypeScript"):
            if code.count("{") != code.count("}"):
                return False, "unbalanced_braces"
            if code.count("(") != code.count(")"):
                return False, "unbalanced_parens"
            if code.count("[") != code.count("]"):
                return False, "unbalanced_brackets"

        elif language == "JSON":
            try:
                json.loads(code)
            except Exception as e:
                return False, f"invalid_json: {e}"

        return True, "syntax_ok"

    def _functions_preserved(
        self,
        language: str,
        original: str,
        refined: str,
    ) -> tuple[bool, str]:
        if language == "Python":
            pattern = r'def (\w+)\s*\('
        elif language in ("JavaScript", "TypeScript"):
            pattern = r'(?:function (\w+)|(\w+)\s*(?:=|:)\s*(?:async\s*)?\()'
        else:
            return True, "ok"

        orig_fns    = set(f for group in re.findall(pattern, original)
                         for f in group if f)
        refined_fns = set(f for group in re.findall(pattern, refined)
                         for f in group if f)
        missing     = orig_fns - refined_fns

        if missing:
            return False, f"functions_removed: {missing}"
        return True, "functions_ok"

    # ── Stage 2: Pre-Refine Analysis ─────────────────────────────

    def pre_refine_analysis(
        self,
        language: str,
        original_code: str,
        api_caller: Optional[Callable],
    ) -> dict:
        """
        Analyze BEFORE refining — tell bot what to touch and what not to.
        Returns structured analysis dict.

        v2 FIX: JSON cleanup now uses regex (re.sub) instead of fragile
        line-by-line stripping. The old approach failed when content appeared
        after the closing fence or when the fence wasn't the last line.
        """
        if api_caller is None:
            return {"risky_areas": [], "safe_to_refine": []}
        prompt = f"""Analyze this {language} code. Return ONLY valid JSON:
{{
  "risky_areas": ["list of things that must NOT be changed"],
  "safe_to_refine": ["list of things that CAN be improved"],
  "entry_points": ["public functions/exports"]
}}

CODE:
{original_code[:800]}"""

        try:
            result = api_caller(prompt, max_tokens=300)
            if not result:
                return {}
            # v2 FIX: regex-based fence removal — handles all common LLM fence formats
            # (```json, ```JSON, ```, ``` with trailing content, etc.)
            clean = result.strip()
            clean = re.sub(r'^```(?:json)?\s*\n?', '', clean, flags=re.IGNORECASE)
            clean = re.sub(r'\n?```\s*$', '', clean)
            clean = clean.strip()
            parsed = json.loads(clean)
            if isinstance(parsed, dict):
                return parsed
        except Exception:
            pass
        return {}

    # ── Stage 3: AI Self-Check ────────────────────────────────────

    def ai_self_check(
        self,
        language: str,
        original_code: str,
        refined_code: str,
        api_caller: Optional[Callable],
    ) -> tuple[bool, str]:
        """
        Ask AI to verify its own output — fresh perspective.

        v2 FIX: context window expanded from 500 → 1500 chars.
        At 500 chars (~10 lines), bugs introduced in the middle or end of
        any medium-sized file were completely invisible to this check.
        1500 chars covers ~25–40 lines, a more realistic review window.

        HARDENED: validates refined_code before making the API call.
        Gracefully skips if no api_caller provided.
        """
        if api_caller is None:
            logger.warning(
                "ai_self_check: no api_caller provided — skipping AI verification "
                "(set api_verification on PushCleanBrain to enable)"
            )
            return True, "skipped_no_api_caller"

        # Skip API call if refined_code is already invalid
        valid_rc, rc_reason = _validate_refined_code(refined_code)
        if not valid_rc:
            return False, f"ai_self_check_skipped_invalid_input: {rc_reason}"

        prompt = f"""Code reviewer task. Compare ORIGINAL vs REFINED {language}.
Respond with exactly 2 lines:
SAFE: yes or no
REASON: one sentence

Reject (no) if:
- Logic broken or functions missing
- Placeholders inserted
- Syntax appears wrong
- Functionality changed

ORIGINAL (first 1500 chars):
{original_code[:1500]}

REFINED (first 1500 chars):
{refined_code[:1500]}"""

        try:
            response = api_caller(prompt, max_tokens=80)
            if not response:
                return True, "ai_check_skipped"

            lower = response.lower()
            if "safe: no" in lower or "safe:no" in lower:
                for line in response.splitlines():
                    if "reason:" in line.lower():
                        return False, line.split(":", 1)[-1].strip()
                return False, "ai_self_check_rejected"

            return True, "ai_approved"
        except Exception:
            logger.exception("ai_self_check failed")
            return True, "ai_check_error_skipped"


# ─────────────────────────────────────────────────────────────────
# MODULE 3 — PushCleanContext
# ONLY JOB: Read data, build prompt context string. Save nothing.
# ─────────────────────────────────────────────────────────────────

class PushCleanContext:
    """
    Pure context builder. Reads DB, returns strings. Nothing else.
    """

    def history_context(
        self,
        language: str,
        min_score: float = 7.5,
        limit: int = 3,
    ) -> str:
        """
        v2 FIX: replaced set-membership diff with difflib.unified_diff.

        Old approach:
            orig_set = set(orig_lines)
            added = [l for l in refined_lines if l not in orig_set]
        Problem: line ORDER is ignored. A line at position 5 in original that
        was moved to position 50 in refined would show as "not added" — making
        the learning context show zero changes for many real improvements.

        New approach: unified_diff gives accurate added-line extraction
        regardless of position, including moved blocks.
        """
        try:
            with get_db(BRAIN_DB) as conn:
                rows = conn.execute("""
                    SELECT original_code, refined_code, self_score
                    FROM refinement_pairs
                    WHERE language   = ?
                    AND   accepted   = 1
                    AND   self_score >= ?
                    ORDER BY self_score DESC
                    LIMIT ?
                """, (language, min_score, limit)).fetchall()

            if not rows:
                return ""

            ctx  = f"\n\n=== PAST SUCCESSFUL {language} REFINEMENTS ===\n"
            ctx += "Learn from these — apply similar improvements:\n\n"

            for i, (orig, refined, score) in enumerate(rows, 1):
                orig_lines    = orig.strip().splitlines() if orig else []
                refined_lines = refined.strip().splitlines() if refined else []

                # v2: proper positional diff
                diff = difflib.unified_diff(
                    orig_lines, refined_lines, lineterm="", n=0
                )
                added = [
                    line[1:]  # strip the leading '+'
                    for line in diff
                    if line.startswith("+") and not line.startswith("+++")
                    and line[1:].strip()
                ]

                ctx += f"[Example {i} — score {score:.1f}/10]\n"
                ctx += "\n".join(added[:8]) + "\n\n"

            ctx += "=== END EXAMPLES ===\n"
            return ctx

        except Exception:
            logger.exception("Context.history_context failed")
            return ""

    def pattern_context(
        self,
        language: str,
        min_confidence: float = 0.6,
        limit: int = 10,
    ) -> str:
        try:
            with get_db(BRAIN_DB) as conn:
                rows = conn.execute("""
                    SELECT pattern, confidence
                    FROM learned_patterns
                    WHERE language    = ?
                    AND   confidence >= ?
                    ORDER BY confidence DESC, used_count DESC
                    LIMIT ?
                """, (language, min_confidence, limit)).fetchall()

            if not rows:
                return ""

            ctx  = f"\n\n=== LEARNED {language} RULES ===\n"
            ctx += "Apply these rules learned from past refinements:\n"
            for pattern, conf in rows:
                ctx += f"RULE: {pattern} (confidence: {conf:.0%})\n"
            ctx += "=== END RULES ===\n"
            return ctx

        except Exception:
            logger.exception("Context.pattern_context failed")
            return ""

    def error_warning_context(
        self,
        language: str,
        min_occurrences: int = 2,
    ) -> str:
        try:
            with get_db(BRAIN_DB) as conn:
                rows = conn.execute("""
                    SELECT error_type, error_msg, occurrences
                    FROM validation_failures
                    WHERE language    = ?
                    AND   occurrences >= ?
                    ORDER BY occurrences DESC
                    LIMIT 5
                """, (language, min_occurrences)).fetchall()

            if not rows:
                return ""

            ctx  = f"\n\n=== AVOID THESE KNOWN {language} ERRORS ===\n"
            for err_type, msg, count in rows:
                ctx += f"- {err_type} (seen {count}x): {msg}\n"
            ctx += "=== END WARNINGS ===\n"
            return ctx

        except Exception:
            logger.exception("Context.error_warning_context failed")
            return ""

    def build_full_context(
        self,
        language: str,
    ) -> str:
        return (
            self.error_warning_context(language)
            + self.pattern_context(language)
            + self.history_context(language)
        )


# ─────────────────────────────────────────────────────────────────
# MODULE 4 — PushCleanLearner
# ONLY JOB: Extract patterns from successful refinements.
# ─────────────────────────────────────────────────────────────────

class PushCleanLearner:
    """
    Pure pattern extractor. Reads pairs, extracts rules, tells Collector to save.
    HARDENED v2: word-count-based rule length check (was char-based),
    score clamped to [0,1] internally.
    """

    def __init__(
        self,
        collector: PushCleanCollector,
        api_caller: Callable,
        min_score: float = 8.0,
    ) -> None:
        self._collector  = collector
        self._api        = api_caller
        self._min_score  = min_score

    def learn_from_pair(
        self,
        language: str,
        original_code: str,
        refined_code: str,
        self_score: float,
    ) -> int:
        """
        Extract patterns from one refinement pair.

        v2 FIX: rule length check changed from char-based to word-count-based.
        Old: `8 < len(rule) < 120` — allowed up to ~20 words (contradicts
        the prompt instruction "Keep each rule under 15 words").
        New: `3 < len(rule.split()) < 20` — directly enforces word limits.
        Min words = 3 (excludes trivially vague rules like "add logging").
        Max words = 20 (generous upper bound for complex rules).
        """
        if self_score < self._min_score:
            return 0

        # Skip if refined_code invalid
        valid_rc, _ = _validate_refined_code(refined_code)
        if not valid_rc:
            return 0
        if not isinstance(original_code, str) or not original_code.strip():
            return 0

        prompt = f"""Compare BEFORE and AFTER {language} code.
Extract up to 3 reusable improvement rules.
Format — each rule on its own line, start with RULE:
Keep each rule under 15 words. Be specific, not generic.

BEFORE:
{original_code[:600]}

AFTER:
{refined_code[:600]}

Rules:"""

        try:
            response = self._api(prompt, max_tokens=250)
            if not response:
                return 0

            saved = 0
            confidence = _clamp_score(self_score / 10.0)

            for line in response.splitlines():
                line = line.strip()
                if line.upper().startswith("RULE:"):
                    rule = line[5:].strip()
                    # v2 FIX: word-count-based length check
                    word_count = len(rule.split())
                    if 3 < word_count < 20:
                        self._collector.save_pattern(language, rule, confidence)
                        saved += 1

            return saved

        except Exception:
            logger.exception("Learner.learn_from_pair failed")
            return 0

    def batch_learn(
        self,
        language: str,
        min_score: float = 8.5,
        limit: int = 20,
    ) -> int:
        """Re-process best historical pairs to extract more patterns."""
        try:
            with get_db(BRAIN_DB) as conn:
                rows = conn.execute("""
                    SELECT original_code, refined_code, self_score
                    FROM refinement_pairs
                    WHERE language   = ?
                    AND   accepted   = 1
                    AND   self_score >= ?
                    ORDER BY self_score DESC
                    LIMIT ?
                """, (language, min_score, limit)).fetchall()

            total = 0
            for orig, refined, score in rows:
                total += self.learn_from_pair(language, orig, refined, score)

            logger.info(
                "Batch learn: %d new patterns from %d pairs (%s)",
                total, len(rows), language,
            )
            return total

        except Exception:
            logger.exception("Learner.batch_learn failed")
            return 0


# ─────────────────────────────────────────────────────────────────
# MODULE 5 — PushCleanBrain
# ONLY JOB: Coordinate the 4 modules. Zero business logic itself.
# HARDENED v2: _adaptive_behavior returns bool and ACTUALLY changes
# pipeline behavior (stricter confidence threshold in post_refine).
# ─────────────────────────────────────────────────────────────────

class PushCleanBrain:
    """
    Pure coordinator. Wires the 4 modules together.

    HARDENING CHANGES v2:
    - _adaptive_behavior() returns bool (is_low_success) — was void/log-only
    - post_refine() calls _adaptive_behavior() and raises confidence threshold
      by ADAPTIVE_CONFIDENCE_PENALTY when low success is detected
    - This closes the gap between "warning logged" and "behavior changed"
    - All prior v1 hardening retained
    """

    def __init__(
        self,
        api_for_analysis:     Optional[Callable] = None,
        api_for_verification: Optional[Callable] = None,
        api_general:          Optional[Callable] = None,
    ) -> None:
        self._api_analysis     = api_for_analysis
        self._api_verification = api_for_verification
        self._api_general      = api_general
        self.collector = PushCleanCollector()
        self.validator = PushCleanValidator(api_caller=api_for_verification)
        self.context   = PushCleanContext()
        self.learner   = PushCleanLearner(
            collector  = self.collector,
            api_caller = api_general,
        )

    # ── PRE-REFINE ────────────────────────────────────────────────

    def pre_refine(
        self,
        language: str,
        original_code: str,
        co_issues: Optional[list] = None,
        co_fixes:  Optional[list] = None,
        repo_name: str = "",
    ) -> tuple[str, dict]:
        """
        Called BEFORE refining.
        Returns (context_str, analysis).

        Note: adaptive behavior is also checked independently in post_refine
        to actually enforce stricter validation — not just log here.
        """
        context_str = self.context.build_full_context(language)

        if co_issues:
            co_ctx  = f"\n\n=== STATIC PRE-FLIGHT ({language}) ===\n"
            co_ctx += "Local analysis found these issues — fix them during refine:\n"
            _pairs = list(zip(co_issues, co_fixes or []))
            for issue, fix in _pairs[:6]:
                co_ctx += f"  ISSUE: {issue}\n"
                if fix:
                    co_ctx += f"  FIX:   {fix}\n"
            if len(co_issues) > 6:
                co_ctx += f"  ... and {len(co_issues) - 6} more issue(s)\n"
            co_ctx += "=== END STATIC PRE-FLIGHT ===\n"
            context_str += co_ctx

        analysis = self.validator.pre_refine_analysis(
            language, original_code, self._api_analysis
        )

        if analysis.get("risky_areas"):
            context_str += (
                f"\n\nDO NOT MODIFY THESE AREAS:\n"
                + "\n".join(f"- {r}" for r in analysis["risky_areas"])
                + "\n"
            )
        if analysis.get("safe_to_refine"):
            context_str += (
                f"\nFOCUS IMPROVEMENTS HERE:\n"
                + "\n".join(f"- {s}" for s in analysis["safe_to_refine"])
                + "\n"
            )

        return context_str, analysis

    # ── POST-REFINE ───────────────────────────────────────────────

    def post_refine(
        self,
        repo_name: str,
        file_path: str,
        language: str,
        original_code: str,
        refined_code: str,
        self_score: float,
    ) -> tuple[bool, str]:
        """
        Called AFTER refining, BEFORE committing.

        HARDENED v2:
        0.  Type/emptiness validation
        0a. Adaptive behavior check → adjusts confidence threshold if low success
        0b. Confidence gate (threshold raised when adaptive detects low success)
        1.  Static validation (no API)
        1b. Python compile check
        2.  AI self-check (Gemini, cheap)
        3.  Save pair + learn patterns on success

        Returns (is_safe, reason).
        """

        # ── Stage 0: Type/emptiness validation ───────────────────
        valid_rc, rc_reason = _validate_refined_code(refined_code)
        if not valid_rc:
            self.collector.save_failure(
                language, "invalid_refined_code", rc_reason, file_path
            )
            return False, f"invalid_input: {rc_reason}"

        # ── Stage 0a: Adaptive behavior — ACTUALLY CHANGES THRESHOLD ──────────
        # v2 FIX: _adaptive_behavior now returns bool.
        # When the (language, repo) pair has historically low success,
        # we increase the confidence threshold by ADAPTIVE_CONFIDENCE_PENALTY.
        # Old v1 behavior: logged a warning and did nothing else.
        is_low_success = self._adaptive_behavior(language, repo_name)
        if is_low_success:
            effective_threshold = min(
                CONFIDENCE_ACCEPT_THRESHOLD + ADAPTIVE_CONFIDENCE_PENALTY,
                0.95,  # hard ceiling — never demand near-perfect score
            )
            logger.info(
                "Brain.post_refine: low success detected for lang=%s repo=%s — "
                "raising confidence threshold %.2f → %.2f",
                language, repo_name or "(any)",
                CONFIDENCE_ACCEPT_THRESHOLD, effective_threshold,
            )
        else:
            effective_threshold = CONFIDENCE_ACCEPT_THRESHOLD

        # ── Stage 0b: Confidence gate (with adaptive threshold) ──
        conf_accepted, confidence = self.validator.confidence_gate(
            self_score, threshold=effective_threshold
        )
        if not conf_accepted:
            reason = (
                f"confidence_too_low: {confidence:.2f} < "
                f"{effective_threshold:.2f} (score={self_score})"
            )
            self.collector.save_failure(language, "low_confidence", reason, file_path)
            logger.info("Brain.post_refine: %s", reason)
            return False, reason

        # ── Stage 1: Static checks (no API cost) ─────────────────
        static_ok, static_reason = self.validator.static_check(
            language, original_code, refined_code
        )
        if not static_ok:
            self.collector.save_failure(
                language, "static_check", static_reason, file_path
            )
            return False, f"static: {static_reason}"

        # ── Stage 1b: Python compile check ───────────────────────
        if language == "Python":
            compile_ok, compile_msg = _compile_check_python(refined_code)
            if not compile_ok:
                self.collector.save_failure(
                    language, "compile_check", compile_msg, file_path
                )
                return False, f"compile: {compile_msg}"

        # ── Stage 2: AI self-check ────────────────────────────────
        ai_ok, ai_reason = self.validator.ai_self_check(
            language, original_code, refined_code, self._api_verification
        )
        if not ai_ok:
            self.collector.save_failure(
                language, "ai_check", ai_reason, file_path
            )
            return False, f"ai_check: {ai_reason}"

        # ── Stage 3: Save + Learn (only on valid refinements) ─────
        self.collector.save_pair(
            repo_name, file_path, language,
            original_code, refined_code,
            self_score, accepted=True,
            confidence=confidence,
        )

        new_patterns = self.learner.learn_from_pair(
            language, original_code, refined_code, self_score
        )
        if new_patterns:
            logger.info(
                "Brain: %d new pattern(s) learned from %s", new_patterns, file_path
            )

        return True, "approved"

    def on_commit_failed(
        self,
        file_path: str,
        language: str,
        original_code: str,
        refined_code: str,
        self_score: float,
        reason: str,
    ) -> None:
        """
        Called when a refinement was rejected.
        HARDENED: validates refined_code before saving negative example.
        """
        valid_rc, _ = _validate_refined_code(refined_code)
        if valid_rc:
            _, confidence = self.validator.confidence_gate(self_score)
            self.collector.save_pair(
                "", file_path, language,
                original_code, refined_code,
                self_score, accepted=False,
                confidence=confidence,
            )
        self.collector.save_failure(
            language, "commit_rejected", reason, file_path
        )

    # ── ADAPTIVE BEHAVIOR ─────────────────────────────────────────

    def _adaptive_behavior(self, language: str, repo_name: str = "") -> bool:
        """
        Check historical success rate for this (language, repo) pair.

        v2 FIX: now returns bool (True = low success detected).
        v1 only logged a warning and returned None — zero effect on pipeline.
        The return value is now used by post_refine() to raise the confidence
        threshold, making adaptation a real behavioral change, not just a log entry.

        Returns:
            True  — low success rate detected; caller should apply stricter checks.
            False — success rate is acceptable, or insufficient data.
        """
        try:
            rate = self.collector.get_adaptive_success_rate(language, repo_name)
            if rate is None:
                return False  # insufficient data — don't penalise
            if rate < ADAPTIVE_LOW_SUCCESS_THRESHOLD:
                logger.warning(
                    "Brain.adaptive: low success rate %.1f%% for language=%s repo=%s "
                    "— stricter confidence threshold will be applied",
                    rate * 100, language, repo_name or "(any)",
                )
                return True
            return False
        except Exception:
            logger.exception("Brain._adaptive_behavior failed — defaulting to False")
            return False

    # ── UTILITY ───────────────────────────────────────────────────

    def get_brain_report(self) -> dict:
        """Return stats from all modules — for dashboard/health endpoint."""
        stats = self.collector.get_stats()
        try:
            with get_db(BRAIN_DB) as conn:
                top_patterns = conn.execute("""
                    SELECT language, COUNT(*) as count
                    FROM learned_patterns
                    GROUP BY language
                    ORDER BY count DESC
                    LIMIT 5
                """).fetchall()
            stats["patterns_by_language"] = {
                lang: count for lang, count in top_patterns
            }
        except Exception:
            pass
        return stats

    def run_batch_learning(self, language: str) -> int:
        """Manual trigger — re-process historical data for deeper learning."""
        return self.learner.batch_learn(language)
