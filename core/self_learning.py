"""
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
MIN_SCORE_TO_ACCEPT:  float = float(os.getenv("PUSHCLEAN_MIN_SCORE",  "6.5"))
MAX_REFINE_ATTEMPTS:  int   = int(os.getenv("PUSHCLEAN_MAX_ATTEMPTS", "3"))
MIN_PATTERN_SCORE:    float = float(os.getenv("PUSHCLEAN_PATTERN_MIN","8.0"))

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
                    repo_name    TEXT    NOT NULL DEFAULT '',
                    file_path    TEXT    NOT NULL,
                    language     TEXT    NOT NULL,
                    self_score   REAL    NOT NULL DEFAULT 0.0,
                    iterations   INTEGER NOT NULL DEFAULT 1,
                    accepted     INTEGER NOT NULL DEFAULT 0,
                    score_reason TEXT    NOT NULL DEFAULT '',
                    recorded_at  TEXT    NOT NULL
                )
            """)
            conn.execute("""
                CREATE INDEX IF NOT EXISTS idx_retro_repo_lang
                ON retrospect_log (repo_name, language, accepted)
            """)

            # Learned patterns — extracted from successful refinements
            conn.execute("""
                CREATE TABLE IF NOT EXISTS learned_patterns (
                    id          INTEGER PRIMARY KEY AUTOINCREMENT,
                    language    TEXT    NOT NULL,
                    repo_name   TEXT    NOT NULL DEFAULT '',
                    pattern     TEXT    NOT NULL,
                    confidence  REAL    NOT NULL DEFAULT 0.5,
                    used_count  INTEGER NOT NULL DEFAULT 0,
                    created_at  TEXT    NOT NULL,
                    UNIQUE(language, pattern)
                )
            """)
            conn.execute("""
                CREATE INDEX IF NOT EXISTS idx_lp_lang_conf
                ON learned_patterns (language, confidence DESC)
            """)
        _SCHEMA_INITED = True
        logger.info("SelfLearning DB initialised at %s", SELF_LEARNING_DB)


# ─────────────────────────────────────────────────────────────────
# SELF-LEARNING ENGINE
# ─────────────────────────────────────────────────────────────────

class SelfLearningEngine:
    """
    Iterative code cleaning with self-evaluation and pattern learning.

    Interface (called by orchestrator):
        refine(full_input, path, language, repo_name)
            → tuple(cleaned_code, score, iterations)
        get_session_report()  → logs a session summary
        show_learned_rules()  → prints learned patterns table
        session_rules_learned → int (patterns saved this session)
    """

    def __init__(self, api_caller: Optional[Callable]) -> None:
        self._api = api_caller              # DeepSeek primary via SmartAPICaller.general
        self.session_rules_learned: int = 0
        self._session_scores: list[float]  = []
        self._session_accepted: int        = 0
        self._session_rejected: int        = 0
        _ensure_schema()

    # ── Public: refine ────────────────────────────────────────────

    def refine(
        self,
        full_input: str,
        path:       str,
        language:   str,
        repo_name:  str = "",
    ) -> tuple[str, float, int]:
        """
        Refine code iteratively until score >= MIN_SCORE or MAX_ATTEMPTS reached.

        full_input: complete prompt already containing context + code.
        Returns (best_refined_code, best_score, total_iterations).
        Returns ("", 0.0, 0) if all attempts fail.
        """
        best_code:  str   = ""
        best_score: float = 0.0
        iterations: int   = 0
        reason:     str   = ""   # FIX: initialise before loop — may never be assigned if all attempts hit continue
        current_input     = full_input

        for attempt in range(1, MAX_REFINE_ATTEMPTS + 1):
            iterations = attempt
            logger.debug("refine attempt %d/%d: %s", attempt, MAX_REFINE_ATTEMPTS, path)

            # ── Step 1: Refine ────────────────────────────────────
            refined = self._call_refine(current_input, language)
            if not refined or not refined.strip():
                logger.warning("refine attempt %d: empty response for %s", attempt, path)
                continue

            refined = self._strip_fences(refined)
            if not refined:
                continue

            # ── Step 2: Self-score ────────────────────────────────
            score, reason = self._self_score(refined, full_input, language)
            logger.debug(
                "refine attempt %d: score=%.1f/10 path=%s reason=%s",
                attempt, score, path, reason[:60],
            )

            if score > best_score:
                best_score = score
                best_code  = refined

            if score >= MIN_SCORE_TO_ACCEPT:
                # Good enough — stop iterating
                break

            # ── Step 3: Re-retry prompt (inject score feedback) ──
            if attempt < MAX_REFINE_ATTEMPTS:
                current_input = self._build_retry_prompt(
                    full_input, refined, score, reason, language
                )

        # ── Persist retrospect ────────────────────────────────────
        accepted = best_score >= MIN_SCORE_TO_ACCEPT and bool(best_code)
        self._log_retrospect(
            repo_name, path, language, best_score, iterations,
            accepted, reason,
        )

        # ── Learn patterns from good refinements ──────────────────
        if accepted and best_score >= MIN_PATTERN_SCORE and best_code:
            original = self._extract_original_code(full_input)
            if original:
                new_patterns = self._extract_patterns(
                    language, original, best_code, best_score, repo_name
                )
                self.session_rules_learned += new_patterns

        self._session_scores.append(best_score)
        if accepted:
            self._session_accepted += 1
        else:
            self._session_rejected += 1

        return best_code, best_score, iterations

    # ── Refine API call ───────────────────────────────────────────

    def _call_refine(self, prompt: str, language: str) -> Optional[str]:
        """Call the API to refine code. Returns raw text or None."""
        full_prompt = (
            f"You are an expert {language} developer and code quality engineer.\n"
            f"Refine the following code: improve quality, fix bugs, improve readability.\n"
            f"Preserve ALL existing functionality. Return ONLY the refined code.\n"
            f"Do NOT add markdown fences, explanations, or commentary.\n\n"
            f"{prompt}"
        )
        if self._api:
            return self._api(full_prompt, max_tokens=4000)
        return None

    # ── Self-scoring ──────────────────────────────────────────────

    def _self_score(
        self,
        refined_code: str,
        original_prompt: str,
        language: str,
    ) -> tuple[float, str]:
        """
        Ask the AI to score its own output.
        Returns (score 0.0–10.0, reason string).
        Falls back to heuristic score if API fails.
        """
        original = self._extract_original_code(original_prompt)
        if not original:
            return self._heuristic_score(refined_code), "heuristic"

        prompt = f"""Evaluate this {language} code refinement. Respond ONLY with JSON:
{{"score": 7.5, "reason": "one sentence explaining the score"}}

Score 0–10 where:
  9–10 = excellent: bugs fixed, clean, idiomatic, type-safe
  7–8  = good: clear improvements, no regressions
  5–6  = acceptable: minor improvements only
  0–4  = poor: regressions, placeholders, truncation, or no real change

ORIGINAL (first 800 chars):
{original[:800]}

REFINED (first 800 chars):
{refined_code[:800]}"""

        if self._api:
            raw = self._api(prompt, max_tokens=150)
        else:
            raw = None

        if not raw:
            score = self._heuristic_score(refined_code)
            return score, "heuristic_no_api"

        try:
            clean = re.sub(r"^```(?:json)?\s*\n?", "", raw.strip(), flags=re.IGNORECASE)
            clean = re.sub(r"\n?```\s*$", "", clean).strip()
            parsed = json.loads(clean)
            score  = float(parsed.get("score", 5.0))
            score  = max(0.0, min(10.0, score))
            reason = str(parsed.get("reason", ""))[:200]
            return score, reason
        except Exception:
            # Fallback: scan for a number like "score: 7.5", "7.50/10", "10/10"
            # 10 alternative listed first to avoid matching the "1" prefix as a single digit
            m = re.search(r"\b(10(?:\.[0-9]+)?|[0-9](?:\.[0-9]+)?)\s*(?:/10|out of 10)?", raw)
            if m:
                try:
                    score = max(0.0, min(10.0, float(m.group(1))))
                    return score, "parsed_fallback"
                except ValueError:
                    pass
            return self._heuristic_score(refined_code), "parse_failed"

    def _heuristic_score(self, refined_code: str) -> float:
        """
        Zero-API score estimate based on code properties.
        Used when all API calls fail — better than returning 0.
        """
        if not refined_code or not refined_code.strip():
            return 0.0
        score = 5.0
        # Penalize obvious problems
        if "TODO" in refined_code or "FIXME" in refined_code:
            score -= 1.0
        if "pass  # " in refined_code or "raise NotImplementedError" in refined_code:
            score -= 2.0
        if len(refined_code.strip()) < 20:
            score -= 3.0
        # Reward improvements
        if "logging" in refined_code or "logger" in refined_code:
            score += 0.5
        if '"""' in refined_code or "'''" in refined_code:
            score += 0.3
        return round(max(0.0, min(10.0, score)), 1)

    # ── Retry prompt ──────────────────────────────────────────────

    def _build_retry_prompt(
        self,
        original_prompt: str,
        prev_refined:    str,
        prev_score:      float,
        reason:          str,
        language:        str,
    ) -> str:
        """Build a follow-up prompt when score is below threshold."""
        return (
            f"{original_prompt}\n\n"
            f"=== PREVIOUS ATTEMPT FEEDBACK ===\n"
            f"Score: {prev_score:.1f}/10 — {reason}\n"
            f"Your previous output scored below {MIN_SCORE_TO_ACCEPT}/10.\n"
            f"Re-refine the code addressing the feedback above.\n"
            f"Return ONLY the improved {language} code.\n"
            f"=== END FEEDBACK ===\n"
        )

    # ── Pattern extraction ────────────────────────────────────────

    def _extract_patterns(
        self,
        language:     str,
        original:     str,
        refined:      str,
        score:        float,
        repo_name:    str,
    ) -> int:
        """
        Ask the AI to extract reusable improvement rules from this refinement.
        Saves up to 3 rules to learned_patterns.
        Returns count of new patterns saved.
        """
        if not self._api:
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
            now   = datetime.now(timezone.utc).isoformat()
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
                logger.debug("Learned %d new pattern(s) from refinement", saved)
            return saved
        except Exception:
            logger.exception("_extract_patterns failed")
            return 0

    # ── DB persistence ────────────────────────────────────────────

    def _log_retrospect(
        self,
        repo_name:  str,
        file_path:  str,
        language:   str,
        score:      float,
        iterations: int,
        accepted:   bool,
        reason:     str,
    ) -> None:
        try:
            with _get_db() as conn:
                conn.execute("""
                    INSERT INTO retrospect_log
                        (repo_name, file_path, language, self_score,
                         iterations, accepted, score_reason, recorded_at)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """, (
                    repo_name, file_path, language,
                    round(score, 2), iterations,
                    1 if accepted else 0,
                    reason[:300],
                    datetime.now(timezone.utc).isoformat(),
                ))
        except Exception:
            logger.exception("_log_retrospect failed for %s", file_path)

    # ── Utilities ─────────────────────────────────────────────────

    @staticmethod
    def _strip_fences(code: str) -> str:
        """Remove markdown code fences from LLM output."""
        lines = code.strip().splitlines()
        if lines and lines[0].startswith("```"):
            lines = lines[1:]
        if lines and lines[-1].strip().startswith("```"):
            lines = lines[:-1]
        return "\n".join(lines).strip()

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

    # ── Session reporting ─────────────────────────────────────────

    def get_session_report(self) -> None:
        """Log a summary of this session's refine activity."""
        if not self._session_scores:
            logger.info("SelfLearner: no files processed this session")
            return
        avg   = sum(self._session_scores) / len(self._session_scores)
        total = len(self._session_scores)
        logger.info(
            "SelfLearner session: %d files | avg score %.1f/10 | "
            "%d accepted | %d rejected | %d new patterns",
            total, avg,
            self._session_accepted,
            self._session_rejected,
            self.session_rules_learned,
        )

    def show_learned_rules(self) -> None:
        """Print learned patterns table (used by show_growth command)."""
        try:
            with _get_db() as conn:
                rows = conn.execute("""
                    SELECT language, pattern, confidence, used_count
                    FROM learned_patterns
                    ORDER BY confidence DESC, used_count DESC
                    LIMIT 30
                """).fetchall()
        except Exception:
            logger.exception("show_learned_rules failed")
            return

        if not rows:
            print("No learned patterns yet.")
            return

        print(f"\n{'─' * 70}")
        print(f"{'LANGUAGE':<12} {'CONF':>5}  {'USED':>4}  PATTERN")
        print(f"{'─' * 70}")
        for r in rows:
            print(
                f"{r['language']:<12} {r['confidence']:>5.0%}  "
                f"{r['used_count']:>4}  {r['pattern'][:45]}"
            )
        print(f"{'─' * 70}")
        print(f"Total: {len(rows)} patterns\n")
