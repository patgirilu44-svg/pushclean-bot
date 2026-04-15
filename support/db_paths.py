"""
db_paths.py — Centralised database path resolution for PushClean
==============================================================
Single source of truth for all SQLite file locations.
LOCAL  (default): all DBs in current working directory.
RENDER (deploy):  set PUSHCLEAN_DATA_DIR=/data  (persistent disk mount).
                  All DBs land in /data/ — survives redeploys.

Usage in every module:
    from db_paths import ORCHESTRATOR_DB, SELF_LEARNING_DB, BRAIN_DB, ...

Override any individual DB via its own env var if needed:
    ORCHESTRATOR_DB=/custom/path/orchestrator.db python webhook_server_fixed.py

RENDER SETUP (render.yaml):
    disk:
      name: pushclean-data
      mountPath: /data
      sizeGB: 1
    envVars:
      - key: PUSHCLEAN_DATA_DIR
        value: /data
"""
from __future__ import annotations

import logging
import os
from pathlib import Path

logger = logging.getLogger("db_paths")

# ── Data directory ────────────────────────────────────────────────────────────
# Set PUSHCLEAN_DATA_DIR=/data on Render (persistent disk mount path).
# Leave unset (or "") locally — DBs will be created in the working directory.
_DATA_DIR: str = os.getenv("PUSHCLEAN_DATA_DIR", "").strip()


def _db(filename: str, env_var: str = "") -> str:
    """
    Resolve a DB filename to a full path.
    Priority:
      1. Explicit env var override (e.g. ORCHESTRATOR_DB=/custom/path.db)
      2. PUSHCLEAN_DATA_DIR + filename           (Render persistent disk)
      3. filename alone                         (local dev, CWD)

    Always creates missing parent directories so the app never crashes
    at startup because /data hasn't been initialised yet.
    """
    # Priority 1: individual env var override
    if env_var:
        explicit = os.getenv(env_var, "").strip()
        if explicit:
            _ensure_dir(explicit)
            return explicit

    # Priority 2: data directory prefix
    if _DATA_DIR:
        path = os.path.join(_DATA_DIR, filename)
        _ensure_dir(path)
        return path

    # Priority 3: local CWD
    return filename


def _ensure_dir(path: str) -> None:
    """Create parent directory if it does not exist. Never raises."""
    try:
        parent = Path(path).parent
        if str(parent) != ".":
            parent.mkdir(parents=True, exist_ok=True)
    except Exception as exc:
        logger.warning("db_paths._ensure_dir: could not create %s — %s", path, exc)


# ── Resolved DB paths ─────────────────────────────────────────────────────────
ORCHESTRATOR_DB   = _db("orchestrator.db",        env_var="ORCHESTRATOR_DB")
SELF_LEARNING_DB  = _db("self_learning.db",        env_var="SELF_LEARNING_DB")
COST_OPTIMIZER_DB = _db("cost_optimizer.db",       env_var="COST_OPTIMIZER_DB")
BRAIN_DB          = _db("pushclean_brain.db",        env_var="BRAIN_DB")
DELIVERY_DB       = _db("webhook_deliveries.db",   env_var="DELIVERY_DB")
MONETIZATION_DB   = _db("monetization.db",         env_var="MONETIZATION_DB")


# ── Startup validation ────────────────────────────────────────────────────────
def validate_data_dir() -> bool:
    """
    Called at server startup. Verifies the data directory is writable.
    Returns True if OK, False if there's a problem (logs the reason).

    On Render: if PUSHCLEAN_DATA_DIR=/data but the disk is not mounted,
    this catches it immediately at startup rather than failing mid-run.
    """
    if not _DATA_DIR:
        logger.info(
            "db_paths: PUSHCLEAN_DATA_DIR not set — using CWD for all DBs (dev mode)"
        )
        return True

    data_path = Path(_DATA_DIR)

    if not data_path.exists():
        logger.error(
            "db_paths: PUSHCLEAN_DATA_DIR=%s does not exist. "
            "On Render, check that the persistent disk is mounted at this path. "
            "DBs will fail to persist across deploys.",
            _DATA_DIR,
        )
        return False

    if not data_path.is_dir():
        logger.error(
            "db_paths: PUSHCLEAN_DATA_DIR=%s exists but is not a directory.", _DATA_DIR
        )
        return False

    # Write test
    test_file = data_path / ".pushclean_write_test"
    try:
        test_file.write_text("ok")
        test_file.unlink()
    except Exception as exc:
        logger.error(
            "db_paths: PUSHCLEAN_DATA_DIR=%s is not writable — %s. "
            "Check Render disk permissions.",
            _DATA_DIR, exc,
        )
        return False

    logger.info(
        "db_paths: persistent data directory OK — %s\n"
        "  orchestrator  → %s\n"
        "  self_learning → %s\n"
        "  cost_optimizer→ %s\n"
        "  brain         → %s\n"
        "  deliveries    → %s\n"
        "  monetization  → %s",
        _DATA_DIR,
        ORCHESTRATOR_DB, SELF_LEARNING_DB,
        COST_OPTIMIZER_DB, BRAIN_DB, DELIVERY_DB, MONETIZATION_DB,
    )
    return True


if __name__ == "__main__":
    # Quick sanity check: python db_paths.py
    import logging as _logging
    _logging.basicConfig(level=_logging.INFO, format="%(levelname)s: %(message)s")
    ok = validate_data_dir()
    print(f"\nAll DB paths:")
    print(f"  ORCHESTRATOR_DB   = {ORCHESTRATOR_DB}")
    print(f"  SELF_LEARNING_DB  = {SELF_LEARNING_DB}")
    print(f"  COST_OPTIMIZER_DB = {COST_OPTIMIZER_DB}")
    print(f"  BRAIN_DB          = {BRAIN_DB}")
    print(f"  DELIVERY_DB       = {DELIVERY_DB}")
    print(f"  MONETIZATION_DB   = {MONETIZATION_DB}")
    print(f"\nValidation: {'✅ OK' if ok else '❌ FAILED'}")
