"""
github_commenter.py — Post refinement results to GitHub
=========================================================
After PushClean refines and commits code, this module:
  1. Posts a commit comment with before/after summary
  2. Optionally creates a Pull Request from pushclean-suggestions → main

No additional dependencies — uses only `requests` (already in requirements).

Usage:
    from github_commenter import post_refinement_comment, create_refinement_pr

    post_refinement_comment(
        repo="owner/repo",
        commit_sha="abc123",
        path="src/api.py",
        language="Python",
        before_score=4.2,
        after_score=7.8,
        improvements=["Removed bare except", "Extracted helper function"],
        local_issues=["bare except clause", "deep nesting"],
        plan="free",
        usage=5,
        limit=20,
    )
"""

from __future__ import annotations

import logging
import os
from typing import Optional

import requests

logger = logging.getLogger("github_commenter")

COMMIT_BRANCH:  str = os.getenv("COMMIT_BRANCH", "pushclean-suggestions")
WATCHED_BRANCH: str = os.getenv("WATCHED_BRANCH", "main")
MARKETPLACE_URL = "https://github.com/marketplace/pushclean"

# FIX: _HEADERS was built at module import time using GITHUB_TOKEN.
# If load_dotenv() is called after import, or if the token is rotated,
# the Authorization header would be permanently stale ("token ").
# Now built dynamically per-request so the current env var is always used.
def _get_headers() -> dict:
    """Return GitHub API headers with the current GITHUB_TOKEN (read at call time)."""
    return {
        "Authorization": f"token {os.getenv('GITHUB_TOKEN', '')}",
        "Accept":        "application/vnd.github+json",
        "X-GitHub-Api-Version": "2022-11-28",
    }


# ── Commit comment ────────────────────────────────────────────────────────────

def post_refinement_comment(
    repo:          str,
    commit_sha:    str,
    path:          str,
    language:      str,
    before_score:  float,
    after_score:   float,
    improvements:  list[str],
    local_issues:  list[str],
    plan:          str  = "free",
    usage:         int  = 0,
    limit:         int  = 20,
    skipped:       bool = False,
    skip_reason:   str  = "",
) -> bool:
    """
    Post a commit comment on the refinement commit.
    Shows before/after summary + usage meter + upgrade CTA if near limit.
    Returns True on success.
    """
    if not os.getenv("GITHUB_TOKEN"):
        logger.warning("github_commenter: GITHUB_TOKEN not set — skipping comment")
        return False

    body = _build_comment(
        path, language, before_score, after_score,
        improvements, local_issues, plan, usage, limit,
        skipped, skip_reason,
    )

    url  = f"https://api.github.com/repos/{repo}/commits/{commit_sha}/comments"
    try:
        resp = requests.post(url, json={"body": body}, headers=_get_headers(), timeout=10)
        if resp.status_code == 201:
            logger.info(
                "Comment posted on commit %s in %s", commit_sha[:8], repo
            )
            return True
        else:
            logger.warning(
                "Comment post failed: HTTP %d — %s",
                resp.status_code, resp.text[:200],
            )
            return False
    except Exception:
        logger.exception("post_refinement_comment failed for %s", repo)
        return False


def _build_comment(
    path:         str,
    language:     str,
    before_score: float,
    after_score:  float,
    improvements: list[str],
    local_issues: list[str],
    plan:         str,
    usage:        int,
    limit:        int,
    skipped:      bool,
    skip_reason:  str,
) -> str:
    """Build the markdown comment body."""
    filename = os.path.basename(path)  # FIX: use module-level os, not re-import

    if skipped:
        return (
            f"### 🤖 PushClean — `{filename}`\n\n"
            f"✨ **Already optimal** — no changes needed.\n\n"
            f"{_usage_bar(usage, limit, plan)}"
        )

    score_delta = after_score - before_score
    arrow = "📈" if score_delta > 0 else "📉"
    delta_str = f"+{score_delta:.1f}" if score_delta >= 0 else f"{score_delta:.1f}"

    # Score bar (0–10 scale)
    before_bar = _score_bar(before_score)
    after_bar  = _score_bar(after_score)

    # Improvements list
    if improvements:
        imp_lines = "\n".join(f"  - {i}" for i in improvements[:5])
    else:
        imp_lines = "  - General code quality improvements"

    # Issues fixed
    if local_issues:
        issues_text = "\n".join(f"  - ~~{i}~~" for i in local_issues[:4])
        issues_section = f"\n**Issues resolved:**\n{issues_text}\n"
    else:
        issues_section = ""

    # Usage meter + upgrade CTA
    usage_section = _usage_bar(usage, limit, plan)

    return (
        f"### 🤖 PushClean — `{filename}` ({language})\n\n"
        f"| | Score | Visual |\n"
        f"|---|---|---|\n"
        f"| **Before** | `{before_score:.1f}/10` | {before_bar} |\n"
        f"| **After**  | `{after_score:.1f}/10`  | {after_bar}  |\n"
        f"| **Delta**  | `{delta_str}` {arrow} | |\n\n"
        f"**Improvements made:**\n{imp_lines}\n"
        f"{issues_section}\n"
        f"---\n"
        f"{usage_section}"
        f"\n*Review the changes in branch `{COMMIT_BRANCH}` before merging.*"
    )


def _score_bar(score: float, width: int = 10) -> str:
    """Visual score bar using emoji blocks."""
    # FIX: guard against NaN — round(NaN) raises ValueError
    try:
        safe = float(score)
        if safe != safe:  # NaN check
            safe = 0.0
    except (TypeError, ValueError):
        safe = 0.0
    filled = round(min(max(safe, 0.0), 10.0))
    return "🟩" * filled + "⬜" * (width - filled)


def _usage_bar(usage: int, limit: int, plan: str) -> str:
    """Usage meter with upgrade CTA if approaching limit."""
    pct = usage / limit if limit > 0 else 1.0
    remaining = limit - usage

    if plan != "free":
        return f"📊 **Usage:** {usage}/{limit} files this month (plan: `{plan}`)\n"

    bar_filled = min(round(pct * 10), 10)  # FIX: cap at 10 — when usage > limit, pct > 1.0
    bar = "🟦" * bar_filled + "⬜" * (10 - bar_filled)

    if pct >= 1.0:
        cta = (
            f"\n> ⚠️ **Free limit reached.** "
            f"[Upgrade to Pro]({MARKETPLACE_URL}) for 200 files/month."
        )
    elif pct >= 0.8:
        cta = (
            f"\n> 💡 {remaining} file(s) remaining this month. "
            f"[Upgrade to Pro]({MARKETPLACE_URL}) for 200 files/month."
        )
    else:
        cta = ""

    return (
        f"📊 **Usage:** {usage}/{limit} files this month "
        f"(free plan) {bar}{cta}\n"
    )


# ── Pull Request creation ─────────────────────────────────────────────────────

def create_refinement_pr(
    repo:          str,
    files_refined: int,
    avg_score:     float,
) -> Optional[str]:
    """
    Create a PR from COMMIT_BRANCH → WATCHED_BRANCH.
    Returns PR URL on success, None on failure or if PR already exists.

    Called once per orchestration run (not per file) — creates one PR
    containing all refined files from this run.
    """
    if not os.getenv("GITHUB_TOKEN"):
        logger.warning("github_commenter: GITHUB_TOKEN not set — skipping PR creation")
        return None

    url   = f"https://api.github.com/repos/{repo}/pulls"
    title = f"🤖 PushClean: {files_refined} file(s) refined (avg score {avg_score:.1f}/10)"
    body  = _build_pr_body(files_refined, avg_score)

    try:
        # Check if PR already exists for this branch
        check = requests.get(
            url,
            params={"head": f"{repo.split('/')[0]}:{COMMIT_BRANCH}", "state": "open"},
            headers=_get_headers(),
            timeout=10,
        )
        if check.status_code == 200:
            # FIX: check.json() was called twice — parse once, reuse
            existing_prs = check.json()
            if existing_prs:
                existing = existing_prs[0]
                logger.info(
                    "PR already exists: %s — skipping creation", existing["html_url"]
                )
                return existing["html_url"]

        resp = requests.post(
            url,
            json={
                "title": title,
                "body":  body,
                "head":  COMMIT_BRANCH,
                "base":  WATCHED_BRANCH,
            },
            headers=_get_headers(),
            timeout=10,
        )
        if resp.status_code == 201:
            pr_url = resp.json()["html_url"]
            logger.info("PR created: %s", pr_url)
            return pr_url
        elif resp.status_code == 422:
            # PR already exists or no diff — not a real error
            logger.debug("PR creation 422: %s", resp.text[:100])
            return None
        else:
            logger.warning(
                "PR creation failed: HTTP %d — %s",
                resp.status_code, resp.text[:200],
            )
            return None
    except Exception:
        logger.exception("create_refinement_pr failed for %s", repo)
        return None


def _build_pr_body(files_refined: int, avg_score: float) -> str:
    return (
        f"## 🤖 PushClean Automated Refinement\n\n"
        f"This PR contains automated code quality improvements.\n\n"
        f"| Metric | Value |\n"
        f"|---|---|\n"
        f"| Files refined | {files_refined} |\n"
        f"| Avg quality score | {avg_score:.1f}/10 |\n"
        f"| Generated by | [PushClean]({MARKETPLACE_URL}) |\n\n"
        f"### Review checklist\n"
        f"- [ ] Review each file diff carefully\n"
        f"- [ ] Run your test suite\n"
        f"- [ ] Approve and merge when satisfied\n\n"
        f"---\n"
        f"*PushClean uses AI to improve code quality while preserving logic and functionality. "
        f"Always review changes before merging.*"
    )
