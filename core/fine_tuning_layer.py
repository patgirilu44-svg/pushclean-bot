"""
╔══════════════════════════════════════════════════════════════════╗
║           FINE TUNING LAYER — Style + Standards + Context        ║
║                                                                  ║
║  Responsibilities:                                               ║
║  1. Analyze code style from sample repo files                    ║
║  2. Load company coding standards from repo                      ║
║  3. Build enhanced prompts with learned context                  ║
║  4. Record successful refinements to finetune_memory             ║
║                                                                  ║
║  DB: SELF_LEARNING_DB → finetune_memory table                    ║
╚══════════════════════════════════════════════════════════════════╝
"""

from __future__ import annotations

import json
import logging
import os
import re
import threading
from collections import Counter
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional, Callable
import sqlite3

logger = logging.getLogger("fine_tuning_layer")

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
            conn.execute("""
                CREATE TABLE IF NOT EXISTS finetune_memory (
                    id           INTEGER PRIMARY KEY AUTOINCREMENT,
                    repo_name    TEXT    NOT NULL DEFAULT '',
                    file_path    TEXT    NOT NULL,
                    language     TEXT    NOT NULL,
                    rule_type    TEXT    NOT NULL,
                    rule_text    TEXT    NOT NULL,
                    source       TEXT    NOT NULL DEFAULT 'orchestrator',
                    used_count   INTEGER NOT NULL DEFAULT 0,
                    recorded_at  TEXT    NOT NULL
                )
            """)
            conn.execute("""
                CREATE INDEX IF NOT EXISTS idx_ft_repo_lang
                ON finetune_memory (repo_name, language)
            """)
        _SCHEMA_INITED = True
        logger.info("FineTuningLayer DB initialised at %s", SELF_LEARNING_DB)


# ─────────────────────────────────────────────────────────────────
# STYLE ANALYZER (pure local — no API)
# ─────────────────────────────────────────────────────────────────

class _StyleAnalyzer:
    """
    Detects code style conventions from sample files.
    No API calls — pure text analysis.
    """

    def analyze(self, files: list[dict]) -> dict:
        """
        Analyze list of {"path": str, "content": str} dicts.
        Returns style profile dict.
        """
        indent_votes: list[int]    = []
        naming_votes: Counter      = Counter()
        docstring_votes: Counter   = Counter()
        quote_votes: Counter       = Counter()
        has_type_hints             = False

        for f in files:
            content = f.get("content", "")
            path    = f.get("path", "")
            if not content or not isinstance(content, str):
                continue
            ext = Path(path).suffix.lower()
            if ext == ".py":
                self._analyze_python(
                    content, indent_votes, naming_votes,
                    docstring_votes, quote_votes,
                )
                if ":" in content and "->" in content:
                    has_type_hints = True

        indent = self._mode(indent_votes, default=4)
        naming = naming_votes.most_common(1)[0][0] if naming_votes else "snake_case"
        quotes = quote_votes.most_common(1)[0][0] if quote_votes else "double"
        docs   = docstring_votes.most_common(1)[0][0] if docstring_votes else "google"

        return {
            "indent_size":   indent,
            "naming":        naming,
            "quote_style":   quotes,
            "docstring":     docs,
            "type_hints":    has_type_hints,
        }

    def _analyze_python(
        self,
        content: str,
        indent_votes: list,
        naming_votes: Counter,
        docstring_votes: Counter,
        quote_votes: Counter,
    ) -> None:
        lines = content.splitlines()

        # Indentation
        for line in lines:
            stripped = line.lstrip()
            if stripped and not stripped.startswith("#"):
                indent = len(line) - len(stripped)
                if indent in (2, 4, 8):
                    indent_votes.append(indent)

        # Naming (function names)
        for m in re.finditer(r"def ([a-zA-Z_]\w*)\s*\(", content):
            name = m.group(1)
            if re.match(r"^[a-z][a-z0-9_]*$", name):
                naming_votes["snake_case"] += 1
            elif re.match(r"^[a-z][a-zA-Z0-9]*$", name):
                naming_votes["camelCase"] += 1

        # Docstrings
        if '"""' in content:
            docstring_votes["google"] += 1
        if "'''" in content:
            docstring_votes["numpy"] += 1

        # Quotes
        single = len(re.findall(r"'[^']*'", content))
        double = len(re.findall(r'"[^"]*"', content))
        if single > double:
            quote_votes["single"] += 1
        else:
            quote_votes["double"] += 1

    @staticmethod
    def _mode(lst: list, default: int = 4) -> int:
        if not lst:
            return default
        return Counter(lst).most_common(1)[0][0]


# ─────────────────────────────────────────────────────────────────
# FINE TUNING LAYER
# ─────────────────────────────────────────────────────────────────

class FineTuningLayer:
    """
    Builds enhanced refine prompts by combining:
    - Repo style profile (detected from sample files)
    - Learned rules from finetune_memory DB
    - Project context summary
    - Standards files if found in repo (.pushclean, STANDARDS.md etc.)

    Interface contract (orchestrator calls these in order):
        1. setup(sample_files, all_files_summary, repo_files_content)
        2. build_enhanced_prompt(path, language, original_code, all_files) → str
        3. record_success(path, language, improvements, source)
    """

    # Files that may contain coding standards
    _STANDARDS_PATHS = [
        ".pushclean", ".pushclean.md", "STANDARDS.md", "CONTRIBUTING.md",
        ".editorconfig", "pyproject.toml", "setup.cfg",
    ]

    def __init__(self, api_caller: Optional[Callable], repo_name: str) -> None:
        self._api        = api_caller
        self.repo_name   = repo_name
        self._style      = {}          # populated by setup()
        self._standards  = []          # list of rule strings
        self._proj_ctx   = ""          # short project context summary
        self._analyzer   = _StyleAnalyzer()
        _ensure_schema()

    # ── Setup (once per run) ──────────────────────────────────────

    def setup(
        self,
        sample_files:       list[dict],
        all_files_summary:  list[dict],
        repo_files_content: dict[str, str],
    ) -> None:
        """
        Initialize style profile, standards, and project context.
        Called once before the per-file refine loop.
        """
        # 1. Detect style from sample files
        self._style = self._analyzer.analyze(sample_files)
        logger.info(
            "Style profile: indent=%d naming=%s quotes=%s type_hints=%s",
            self._style.get("indent_size", 4),
            self._style.get("naming", "?"),
            self._style.get("quote_style", "?"),
            self._style.get("type_hints", False),
        )

        # 2. Load standards from repo files
        for std_path in self._STANDARDS_PATHS:
            if std_path in repo_files_content:
                raw = repo_files_content[std_path]
                rules = self._extract_rules_from_text(raw)
                self._standards.extend(rules)
                logger.info("Loaded %d rules from %s", len(rules), std_path)

        # 3. Build project context string
        self._proj_ctx = self._build_project_context(all_files_summary)

    def _extract_rules_from_text(self, text: str) -> list[str]:
        """Extract bullet-point rules from standards files."""
        rules = []
        for line in text.splitlines():
            line = line.strip()
            # Capture markdown bullets and numbered lists
            if re.match(r"^[-*•]\s+.{10,}", line):
                rules.append(re.sub(r"^[-*•]\s+", "", line))
            elif re.match(r"^\d+\.\s+.{10,}", line):
                rules.append(re.sub(r"^\d+\.\s+", "", line))
        return rules[:20]   # cap at 20 rules

    def _build_project_context(self, all_files: list[dict]) -> str:
        """Build a short text summary of the project structure."""
        ext_counts: Counter = Counter()
        for f in all_files:
            ext = Path(f.get("path", "")).suffix.lower()
            if ext:
                ext_counts[ext] += 1

        if not ext_counts:
            return ""

        top = ext_counts.most_common(5)
        summary = "Project uses: " + ", ".join(
            f"{ext} ({count} files)" for ext, count in top
        )
        return summary

    # ── Build enhanced prompt ─────────────────────────────────────

    def build_enhanced_prompt(
        self,
        path:          str,
        language:      str,
        original_code: str,
        all_files:     list[dict],
    ) -> str:
        """
        Build the context string injected BEFORE the code in the refine prompt.
        Returns a string — caller appends the actual code.
        """
        parts: list[str] = []

        # Style rules
        if self._style:
            style_rules = self._style_to_rules(language)
            if style_rules:
                parts.append("=== REPO STYLE RULES ===")
                parts.extend(f"STYLE: {r}" for r in style_rules)
                parts.append("=== END STYLE ===\n")

        # Standards
        if self._standards:
            parts.append("=== CODING STANDARDS ===")
            for rule in self._standards[:10]:
                parts.append(f"STANDARD: {rule}")
            parts.append("=== END STANDARDS ===\n")

        # Project context
        if self._proj_ctx:
            parts.append(f"PROJECT: {self._proj_ctx}\n")

        # Learned rules from DB for this language
        db_rules = self._load_db_rules(language)
        if db_rules:
            parts.append(f"=== LEARNED {language} RULES ===")
            for rule_text, used in db_rules:
                parts.append(f"RULE: {rule_text}")
            parts.append("=== END RULES ===\n")

        return "\n".join(parts)

    def _style_to_rules(self, language: str) -> list[str]:
        """Convert style profile dict to human-readable rule strings."""
        rules = []
        if not self._style:
            return rules
        indent = self._style.get("indent_size", 4)
        naming = self._style.get("naming", "snake_case")
        quotes = self._style.get("quote_style", "double")
        hints  = self._style.get("type_hints", False)

        if language == "Python":
            rules.append(f"Use {indent}-space indentation")
            rules.append(f"Use {naming} for function and variable names")
            rules.append(f"Prefer {quotes}-quoted strings")
            if hints:
                rules.append("Add type hints to function signatures where missing")
        elif language in ("JavaScript", "TypeScript"):
            rules.append(f"Use {indent}-space indentation")
            rules.append("Prefer const over let; avoid var")
        return rules

    def _load_db_rules(self, language: str, limit: int = 8) -> list[tuple]:
        """Load learned rules for this language from DB."""
        try:
            with _get_db() as conn:
                rows = conn.execute("""
                    SELECT rule_text, used_count
                    FROM finetune_memory
                    WHERE repo_name = ? AND language = ?
                    ORDER BY used_count DESC, recorded_at DESC
                    LIMIT ?
                """, (self.repo_name, language, limit)).fetchall()
            return [(r["rule_text"], r["used_count"]) for r in rows]
        except Exception:
            logger.exception("_load_db_rules failed")
            return []

    # ── Record success ────────────────────────────────────────────

    def record_success(
        self,
        path:         str,
        language:     str,
        improvements: list[str],
        source:       str = "orchestrator",
    ) -> None:
        """
        Save improvements from a successful refinement to finetune_memory.
        These become RULE entries in future refine prompts.
        """
        if not improvements:
            return
        now = datetime.now(timezone.utc).isoformat()
        try:
            with _get_db() as conn:
                for imp in improvements[:5]:
                    imp = imp.strip()
                    if len(imp.split()) < 3:
                        continue  # too short to be useful
                    conn.execute("""
                        INSERT INTO finetune_memory
                            (repo_name, file_path, language, rule_type,
                             rule_text, source, recorded_at)
                        VALUES (?, ?, ?, 'improvement', ?, ?, ?)
                    """, (self.repo_name, path, language, imp, source, now))
        except Exception:
            logger.exception("record_success failed for %s", path)
