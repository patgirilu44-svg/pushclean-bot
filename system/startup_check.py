"""
startup_check.py — PushClean pre-flight system check
===================================================
Run before starting the server to catch configuration problems early.

Usage:
    python startup_check.py           # just check
    python startup_check.py --fix     # check + create missing dirs

Called automatically by webhook_server_fixed.py at import time.
Can also be run standalone on Render via shell tab for debugging.
"""

from __future__ import annotations

import logging
import os
import sqlite3
import sys
from pathlib import Path

logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
log = logging.getLogger("startup_check")

REQUIRED_ENV = [
    ("GITHUB_TOKEN",          True,  "GitHub API access token"),
    ("GITHUB_REPO",           True,  "Target repository (owner/repo)"),
    ("GITHUB_WEBHOOK_SECRET", False, "REQUIRED in PROD_MODE=1"),
    ("ADMIN_API_KEY",         False, "Protects /run and /stats"),
    ("COMMIT_BRANCH",         False, "Branch for refined commits (default: pushclean-suggestions)"),
]

CHECKS_PASSED = 0
CHECKS_FAILED = 0


def _pass(msg: str) -> None:
    global CHECKS_PASSED
    CHECKS_PASSED += 1
    log.info("  ✅ %s", msg)


def _fail(msg: str, fatal: bool = False) -> None:
    global CHECKS_FAILED
    CHECKS_FAILED += 1
    log.error("  ❌ %s", msg)
    if fatal:
        log.critical("Fatal check failed — aborting startup.")
        sys.exit(1)


def _warn(msg: str) -> None:
    log.warning("  ⚠️  %s", msg)


# ── 1. Data directory check ───────────────────────────────────────────────────

def check_data_dir() -> str:
    log.info("\n── Data Directory ──────────────────────────────────")
    data_dir = os.getenv("PUSHCLEAN_DATA_DIR", "")

    if not data_dir:
        _warn(
            "PUSHCLEAN_DATA_DIR not set — DBs will be created in CWD. "
            "Data will be LOST on every Render redeploy. "
            "Set PUSHCLEAN_DATA_DIR=/data with a Render persistent disk."
        )
        return ""

    p = Path(data_dir)
    if not p.exists():
        _fail(f"PUSHCLEAN_DATA_DIR={data_dir} does not exist. "
              "Render disk not mounted?", fatal=True)

    if not p.is_dir():
        _fail(f"PUSHCLEAN_DATA_DIR={data_dir} is not a directory.", fatal=True)

    # Write test
    test = p / ".pushclean_startup_test"
    try:
        test.write_text("ok")
        test.unlink()
        _pass(f"Data directory writable: {data_dir}")
    except Exception as exc:
        _fail(f"Data directory NOT writable: {data_dir} — {exc}", fatal=True)

    # Show disk space
    try:
        stat = os.statvfs(data_dir)
        free_mb = (stat.f_bavail * stat.f_frsize) // (1024 * 1024)
        if free_mb < 50:
            _warn(f"Only {free_mb}MB free in {data_dir} — consider pruning old records")
        else:
            _pass(f"Disk space: {free_mb}MB free in {data_dir}")
    except Exception:
        pass

    return data_dir


# ── 2. DB paths check ─────────────────────────────────────────────────────────

def check_db_paths() -> None:
    log.info("\n── Database Paths ──────────────────────────────────")
    try:
        from db_paths import (
            ORCHESTRATOR_DB, SELF_LEARNING_DB,
            COST_OPTIMIZER_DB, BRAIN_DB, DELIVERY_DB, MONETIZATION_DB,
        )
        dbs = {
            "ORCHESTRATOR_DB":   ORCHESTRATOR_DB,
            "SELF_LEARNING_DB":  SELF_LEARNING_DB,
            "COST_OPTIMIZER_DB": COST_OPTIMIZER_DB,
            "BRAIN_DB":          BRAIN_DB,
            "DELIVERY_DB":       DELIVERY_DB,
            "MONETIZATION_DB":   MONETIZATION_DB,
        }
    except ImportError:
        _warn("db_paths.py not found — using legacy hardcoded paths (not persistent)")
        return

    for name, path in dbs.items():
        parent = Path(path).parent
        if str(parent) != "." and not parent.exists():
            try:
                parent.mkdir(parents=True, exist_ok=True)
                _pass(f"{name}: created dir {parent}")
            except Exception as exc:
                _fail(f"{name}: cannot create dir {parent} — {exc}")
        else:
            # Test SQLite can connect
            try:
                conn = sqlite3.connect(path)
                conn.execute("SELECT 1")
                conn.close()
                _pass(f"{name}: {path}")
            except Exception as exc:
                _fail(f"{name}: {path} — {exc}")


# ── 3. Environment variables check ───────────────────────────────────────────

def check_env_vars() -> None:
    log.info("\n── Environment Variables ───────────────────────────")
    prod_mode = os.getenv("PROD_MODE", "0") == "1"

    for key, required, description in REQUIRED_ENV:
        val = os.getenv(key, "")
        if val:
            # Mask secrets in log
            display = val[:4] + "****" if len(val) > 8 else "****"
            _pass(f"{key}: {display}  ({description})")
        elif required:
            _fail(f"{key} not set — REQUIRED. {description}")
        elif prod_mode and key == "GITHUB_WEBHOOK_SECRET":
            _fail(
                f"{key} not set but PROD_MODE=1 — server will refuse to start. "
                "Set this in Render dashboard → Environment.",
                fatal=False,
            )
        else:
            _warn(f"{key} not set — {description}")

    # At least one API key
    api_keys = ["OPENROUTER_API_KEY", "ANTHROPIC_API_KEY", "DEEPSEEK_API_KEY", "GEMINI_API_KEY"]
    if not any(os.getenv(k) for k in api_keys):
        _fail("No API key found. Set at least one of: " + ", ".join(api_keys))
    else:
        found = [k for k in api_keys if os.getenv(k)]
        _pass(f"API key(s) present: {', '.join(found)}")


# ── 4. Import check ───────────────────────────────────────────────────────────

def check_imports() -> None:
    log.info("\n── Module Imports ──────────────────────────────────")
    modules = [
        ("fastapi",    "pip install fastapi"),
        ("uvicorn",    "pip install uvicorn[standard]"),
        ("requests",   "pip install requests"),
    ]
    for mod, install_hint in modules:
        try:
            __import__(mod)
            _pass(f"import {mod}")
        except ImportError:
            _fail(f"import {mod} — run: {install_hint}")


# ── Main ──────────────────────────────────────────────────────────────────────

def run_all_checks() -> bool:
    global CHECKS_PASSED, CHECKS_FAILED
    CHECKS_PASSED = CHECKS_FAILED = 0   # reset so repeated calls don't accumulate

    log.info("=" * 52)
    log.info("PushClean Startup Check")
    log.info("=" * 52)

    check_data_dir()
    check_db_paths()
    check_env_vars()
    check_imports()

    log.info("\n" + "=" * 52)
    log.info("Results: %d passed, %d failed", CHECKS_PASSED, CHECKS_FAILED)
    if CHECKS_FAILED == 0:
        log.info("✅  All checks passed — safe to start server")
    else:
        log.warning(
            "⚠️  %d check(s) failed — review above before deploying", CHECKS_FAILED
        )
    log.info("=" * 52)
    return CHECKS_FAILED == 0


if __name__ == "__main__":
    ok = run_all_checks()
    sys.exit(0 if ok else 1)
