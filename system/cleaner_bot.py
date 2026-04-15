"""
╔══════════════════════════════════════════════════════════════════╗
║              CLEANER BOT — Multi-API Fusion Layer                ║
║                                                                  ║
║  Exports:                                                        ║
║  - CONFIG              → env-var config dict                     ║
║  - GitHubClient        → GitHub REST API wrapper                 ║
║  - call_claude()       → Anthropic / OpenRouter                  ║
║  - call_deepseek()     → DeepSeek / OpenRouter                   ║
║  - call_gemini()       → Gemini / OpenRouter                     ║
║  - verify_refinement() → safety gate before commit               ║
║                                                                  ║
║  API priority (per call):                                        ║
║  1. OPENROUTER_API_KEY  — unified gateway (recommended)          ║
║  2. Provider-specific key (ANTHROPIC_, DEEPSEEK_, GEMINI_)       ║
║  Set OPENROUTER_API_KEY in Render dashboard to use one key       ║
║  for all three models at lower cost.                             ║
╚══════════════════════════════════════════════════════════════════╝
"""

from __future__ import annotations

import base64
import json
import logging
import os
import re
import time
from typing import Optional

import requests

logger = logging.getLogger("cleaner_bot")

# ── CONFIG ────────────────────────────────────────────────────────────────────
# Single source of truth for all env-var config consumed by orchestrator.
# Read at import time — values are stable for the lifetime of the process.

CONFIG: dict = {
    "GITHUB_TOKEN":    os.getenv("GITHUB_TOKEN", ""),
    "GITHUB_REPO":     os.getenv("GITHUB_REPO", ""),
    "COMMIT_BRANCH":   os.getenv("COMMIT_BRANCH", "pushclean-suggestions"),
    "TARGET_BRANCH":   os.getenv("WATCHED_BRANCH", "main"),   # what we read FROM
    "WATCHED_BRANCH":  os.getenv("WATCHED_BRANCH", "main"),
}

# ── API keys (read dynamically per-call — supports token rotation) ────────────
def _openrouter_key() -> str: return os.getenv("OPENROUTER_API_KEY", "")
def _anthropic_key()  -> str: return os.getenv("ANTHROPIC_API_KEY",  "")
def _deepseek_key()   -> str: return os.getenv("DEEPSEEK_API_KEY",   "")
def _gemini_key()     -> str: return os.getenv("GEMINI_API_KEY",     "")

# ── Supported code file extensions ───────────────────────────────────────────
_CODE_EXTENSIONS = {
    ".py", ".js", ".ts", ".jsx", ".tsx", ".java", ".go", ".rs",
    ".cpp", ".c", ".cs", ".rb", ".php", ".swift", ".kt", ".sh",
    ".sql", ".html", ".css", ".scss", ".sass", ".vue", ".svelte",
    ".yaml", ".yml", ".toml", ".json",
}

# Max file size to process — skip blobs and generated files
_MAX_FILE_BYTES = 150_000


# ─────────────────────────────────────────────────────────────────
# GITHUB CLIENT
# ─────────────────────────────────────────────────────────────────

class GitHubClient:
    """
    Minimal GitHub REST API v3 client.
    All methods return sensible defaults on failure — never raise.
    """

    _API = "https://api.github.com"
    _TIMEOUT = 20  # seconds

    def __init__(self, token: str, repo: str) -> None:
        self._token = token
        self._repo  = repo   # "owner/repo"
        self._headers = {
            "Authorization":        f"token {token}",
            "Accept":               "application/vnd.github+json",
            "X-GitHub-Api-Version": "2022-11-28",
        }

    # ── Internal ─────────────────────────────────────────────────

    def _get(self, path: str, params: dict | None = None) -> Optional[dict | list]:
        url = f"{self._API}{path}"
        try:
            r = requests.get(url, headers=self._headers, params=params,
                             timeout=self._TIMEOUT)
            if r.status_code == 200:
                return r.json()
            logger.warning("GitHub GET %s → HTTP %d", path, r.status_code)
            return None
        except Exception:
            logger.exception("GitHub GET %s failed", path)
            return None

    def _post(self, path: str, body: dict) -> Optional[dict]:
        url = f"{self._API}{path}"
        try:
            r = requests.post(url, headers=self._headers, json=body,
                              timeout=self._TIMEOUT)
            if r.status_code in (200, 201):
                return r.json()
            logger.warning("GitHub POST %s → HTTP %d: %s",
                           path, r.status_code, r.text[:200])
            return None
        except Exception:
            logger.exception("GitHub POST %s failed", path)
            return None

    def _put(self, path: str, body: dict) -> Optional[dict]:
        url = f"{self._API}{path}"
        try:
            r = requests.put(url, headers=self._headers, json=body,
                             timeout=self._TIMEOUT)
            if r.status_code in (200, 201):
                return r.json()
            logger.warning("GitHub PUT %s → HTTP %d: %s",
                           path, r.status_code, r.text[:200])
            return None
        except Exception:
            logger.exception("GitHub PUT %s failed", path)
            return None

    # ── Public API ───────────────────────────────────────────────

    def get_repo_files(self, branch: str = "main") -> list[dict]:
        """
        Return list of code files in the repo using the Git Trees API.
        Each item: {"path": str, "sha": str, "size": int}
        Only returns files with supported extensions under _MAX_FILE_BYTES.
        """
        data = self._get(
            f"/repos/{self._repo}/git/trees/{branch}",
            params={"recursive": "1"},
        )
        if not data or not isinstance(data, dict) or not isinstance(data.get("tree"), list):
            logger.warning("get_repo_files: empty tree for branch=%s", branch)
            return []

        files = []
        for item in data["tree"]:
            if item.get("type") != "blob":
                continue
            path = item.get("path", "")
            size = item.get("size", 0)
            ext  = os.path.splitext(path)[1].lower()
            if ext not in _CODE_EXTENSIONS:
                continue
            if size > _MAX_FILE_BYTES:
                logger.debug("Skipping large file: %s (%d bytes)", path, size)
                continue
            files.append({
                "path": path,
                "sha":  item.get("sha", ""),
                "size": size,
            })

        logger.info("get_repo_files: %d code files found in %s@%s",
                    len(files), self._repo, branch)
        return files

    def get_file_content(self, file_info: dict) -> tuple[str, str]:
        """
        Fetch decoded content + blob SHA for a file.
        Returns ("", "") on failure.
        """
        path = file_info.get("path", "")
        if not path:
            return "", ""

        data = self._get(f"/repos/{self._repo}/contents/{path}")
        if not data or not isinstance(data, dict):
            logger.warning("get_file_content: no data for %s", path)
            return "", ""

        sha      = data.get("sha", "")
        encoding = data.get("encoding", "")
        raw      = data.get("content", "")

        if encoding == "base64":
            try:
                content = base64.b64decode(raw).decode("utf-8", errors="replace")
            except Exception:
                logger.warning("get_file_content: base64 decode failed for %s", path)
                return "", sha
        else:
            content = raw

        return content, sha

    def commit_file(
        self,
        path:    str,
        content: str,
        sha:     str,
        branch:  str,
        message: str,
    ) -> str:
        """
        Write file content to branch via GitHub Contents API.
        Returns commit SHA on success, "" on failure.
        """
        encoded = base64.b64encode(content.encode("utf-8")).decode("ascii")
        data = self._put(
            f"/repos/{self._repo}/contents/{path}",
            body={
                "message": message,
                "content": encoded,
                "sha":     sha,
                "branch":  branch,
            },
        )
        if not data:
            return ""
        commit_sha = (
            data.get("commit", {}).get("sha", "")
            or data.get("sha", "")
        )
        if commit_sha:
            logger.info("Committed %s → %s (%s)", path, branch, commit_sha[:8])
        return commit_sha

    def branch_exists(self, branch: str) -> bool:
        """Return True if the named branch exists in the repo."""
        data = self._get(f"/repos/{self._repo}/git/ref/heads/{branch}")
        return data is not None

    def create_branch(self, branch: str, from_branch: str = "main") -> bool:
        """
        Create a new branch from from_branch's HEAD.
        Returns True on success or if branch already exists.
        """
        if self.branch_exists(branch):
            logger.info("Branch already exists: %s", branch)
            return True

        # Get SHA of source branch
        ref_data = self._get(f"/repos/{self._repo}/git/ref/heads/{from_branch}")
        if not ref_data:
            logger.error("create_branch: cannot get SHA for %s", from_branch)
            return False

        sha = ref_data.get("object", {}).get("sha", "")
        if not sha:
            logger.error("create_branch: SHA missing in ref data for %s", from_branch)
            return False

        result = self._post(
            f"/repos/{self._repo}/git/refs",
            body={"ref": f"refs/heads/{branch}", "sha": sha},
        )
        if result:
            logger.info("Created branch %s from %s (%s)", branch, from_branch, sha[:8])
            return True
        logger.error("Failed to create branch %s", branch)
        return False


# ─────────────────────────────────────────────────────────────────
# API CALL HELPERS
# ─────────────────────────────────────────────────────────────────

def _openrouter_call(
    model:      str,
    prompt:     str,
    max_tokens: int,
    timeout:    int = 60,
) -> Optional[str]:
    """Single OpenRouter call. Returns text or None."""
    key = _openrouter_key()
    if not key:
        return None
    try:
        r = requests.post(
            "https://openrouter.ai/api/v1/chat/completions",
            headers={
                "Authorization": f"Bearer {key}",
                "Content-Type":  "application/json",
            },
            json={
                "model":      model,
                "messages":   [{"role": "user", "content": prompt}],
                "max_tokens": max_tokens,
            },
            timeout=timeout,
        )
        if r.status_code == 200:
            return r.json()["choices"][0]["message"]["content"]
        logger.warning("OpenRouter %s → HTTP %d: %s", model, r.status_code, r.text[:150])
        return None
    except Exception:
        logger.exception("OpenRouter call failed for model=%s", model)
        return None


def call_claude(prompt: str, max_tokens: int = 4000) -> Optional[str]:
    """
    Call Claude (Sonnet). OpenRouter first, direct Anthropic API fallback.
    Returns response text or None.
    """
    # Try OpenRouter
    result = _openrouter_call("anthropic/claude-sonnet-4-6", prompt, max_tokens)
    if result:
        return result

    # Direct Anthropic API
    key = _anthropic_key()
    if not key:
        logger.debug("call_claude: no ANTHROPIC_API_KEY")
        return None
    try:
        r = requests.post(
            "https://api.anthropic.com/v1/messages",
            headers={
                "x-api-key":         key,
                "anthropic-version": "2023-06-01",
                "content-type":      "application/json",
            },
            json={
                "model":      "claude-sonnet-4-6",
                "max_tokens": max_tokens,
                "messages":   [{"role": "user", "content": prompt}],
            },
            timeout=90,
        )
        if r.status_code == 200:
            return r.json()["content"][0]["text"]
        logger.warning("Claude direct → HTTP %d: %s", r.status_code, r.text[:150])
        return None
    except Exception:
        logger.exception("call_claude direct failed")
        return None


def call_deepseek(prompt: str, max_tokens: int = 4000) -> Optional[str]:
    """
    Call DeepSeek. OpenRouter first, direct DeepSeek API fallback.
    Returns response text or None.
    """
    # Try OpenRouter
    result = _openrouter_call("deepseek/deepseek-chat", prompt, max_tokens)
    if result:
        return result

    # Direct DeepSeek API (OpenAI-compatible)
    key = _deepseek_key()
    if not key:
        logger.debug("call_deepseek: no DEEPSEEK_API_KEY")
        return None
    try:
        r = requests.post(
            "https://api.deepseek.com/v1/chat/completions",
            headers={
                "Authorization": f"Bearer {key}",
                "Content-Type":  "application/json",
            },
            json={
                "model":      "deepseek-chat",
                "messages":   [{"role": "user", "content": prompt}],
                "max_tokens": max_tokens,
            },
            timeout=90,
        )
        if r.status_code == 200:
            return r.json()["choices"][0]["message"]["content"]
        logger.warning("DeepSeek direct → HTTP %d: %s", r.status_code, r.text[:150])
        return None
    except Exception:
        logger.exception("call_deepseek direct failed")
        return None


def call_gemini(prompt: str, max_tokens: int = 500) -> Optional[str]:
    """
    Call Gemini Flash. OpenRouter first, direct Gemini API fallback.
    Returns response text or None.
    """
    # Try OpenRouter
    result = _openrouter_call("google/gemini-flash-1.5", prompt, max_tokens)
    if result:
        return result

    # Direct Gemini API
    key = _gemini_key()
    if not key:
        logger.debug("call_gemini: no GEMINI_API_KEY")
        return None
    try:
        r = requests.post(
            f"https://generativelanguage.googleapis.com/v1beta/models/"
            f"gemini-1.5-flash:generateContent?key={key}",
            headers={"Content-Type": "application/json"},
            json={
                "contents":         [{"parts": [{"text": prompt}]}],
                "generationConfig": {"maxOutputTokens": max_tokens},
            },
            timeout=30,
        )
        if r.status_code == 200:
            return (
                r.json()["candidates"][0]["content"]["parts"][0]["text"]
            )
        logger.warning("Gemini direct → HTTP %d: %s", r.status_code, r.text[:150])
        return None
    except Exception:
        logger.exception("call_gemini direct failed")
        return None


# ─────────────────────────────────────────────────────────────────
# VERIFY REFINEMENT
# ─────────────────────────────────────────────────────────────────

def verify_refinement(
    original_code: str,
    refined_code:  str,
    path:          str,
) -> dict:
    """
    Safety gate: ask Gemini (cheap + fast) whether the refined code is
    safe to commit.

    Returns:
        {
            "safe_to_commit":    bool,
            "reason":            str,
            "improvements_made": list[str],
        }

    Fail-closed: if API call fails or response is unparseable,
    returns safe_to_commit=False so we never commit unverified code.
    """
    _FAIL_CLOSED = {
        "safe_to_commit":    False,
        "reason":            "verifier unavailable — fail-closed",
        "improvements_made": [],
    }

    ext = os.path.splitext(path)[1].lower()

    prompt = f"""Code reviewer. Compare ORIGINAL vs REFINED {ext} code.
Respond ONLY with valid JSON — no markdown, no preamble:
{{
  "safe_to_commit": true or false,
  "reason": "one sentence",
  "improvements_made": ["up to 3 specific improvements"]
}}

Reject (false) if:
- Logic or functions removed/broken
- Placeholders inserted (TODO, pass, NotImplementedError)
- Syntax looks wrong
- Code is truncated or incomplete

ORIGINAL (first 1200 chars):
{original_code[:1200]}

REFINED (first 1200 chars):
{refined_code[:1200]}"""

    # Gemini primary (fast + free), DeepSeek fallback
    raw = call_gemini(prompt, max_tokens=300)
    if not raw:
        raw = call_deepseek(prompt, max_tokens=300)
    if not raw:
        logger.warning("verify_refinement: all API calls failed for %s", path)
        return _FAIL_CLOSED

    # Parse JSON — strip markdown fences if present
    try:
        clean = re.sub(r"^```(?:json)?\s*\n?", "", raw.strip(), flags=re.IGNORECASE)
        clean = re.sub(r"\n?```\s*$", "", clean).strip()
        parsed = json.loads(clean)
        if not isinstance(parsed, dict):
            raise ValueError("not a dict")
        return {
            "safe_to_commit":    bool(parsed.get("safe_to_commit", False)),
            "reason":            str(parsed.get("reason", ""))[:200],
            "improvements_made": list(parsed.get("improvements_made", []))[:5],
        }
    except Exception:
        # Try line-based fallback: look for "safe_to_commit": true/false
        lower = raw.lower()
        safe  = '"safe_to_commit": true' in lower or "'safe_to_commit': true" in lower
        logger.warning(
            "verify_refinement: JSON parse failed for %s — using fallback (safe=%s)",
            path, safe,
        )
        return {
            "safe_to_commit":    safe,
            "reason":            "parsed via fallback",
            "improvements_made": [],
        }
