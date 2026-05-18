"""Cloudflare challenge types, state, and shared helpers."""

import asyncio
import enum
import re as _re
from dataclasses import dataclass
from typing import Optional
from urllib.parse import urlparse

from playwright.async_api import Page

from scrapy.http import Request


# ── Challenge classification ──────────────────────────────────────────

class ChallengeType(enum.Enum):
    """Classification of Cloudflare challenge responses."""
    NONE = "none"
    JS_CHALLENGE = "js_challenge"
    MANAGED_CHALLENGE = "managed_challenge"
    PRIVATE_TOKEN = "private_token"
    TURNSTILE = "turnstile"
    PLAIN_403 = "plain_403"


class ChallengeState(enum.Enum):
    """Per-context challenge solving state."""
    IDLE = "idle"
    SOLVING = "solving"
    VALIDATED = "validated"
    FAILED = "failed"


# Body patterns that indicate an active Cloudflare challenge page.
CF_CHALLENGE_BODY_PATTERNS = [
    _re.compile(r'challenge-platform/\S+orchestrate/(captcha|managed|jsch)/v1', _re.M | _re.S),
    _re.compile(r'class="cf-turnstile"', _re.M | _re.S),
    _re.compile(r'challenges\.cloudflare\.com/turnstile', _re.M | _re.S),
    _re.compile(r'data-sitekey="[0-9A-Za-z]+"', _re.M | _re.S),
    _re.compile(r'window\._cf_chl_opt', _re.M | _re.S),
    _re.compile(r'id="challenge-form"', _re.M | _re.S),
]

# Known Cloudflare challenge page title markers (case-insensitive).
CF_TITLE_MARKERS = ("just a moment", "um momento")


# ── Per-context challenge state ───────────────────────────────────────

@dataclass
class ContextChallengeInfo:
    """Per-context Cloudflare challenge solving state."""
    state: ChallengeState = ChallengeState.IDLE
    challenge_type: ChallengeType = ChallengeType.NONE
    attempt_count: int = 0
    last_validated_url: Optional[str] = None
    carrier_page: Optional[Page] = None
    solve_event: Optional[asyncio.Event] = None
    error: Optional[Exception] = None
    solver_marker: object = None


# ── Shared helpers ────────────────────────────────────────────────────

def has_cf_clearance(cookies: list) -> bool:
    """Return *True* if the cookie list contains ``cf_clearance``."""
    return any(c["name"] == "cf_clearance" for c in cookies)


def body_has_challenge_markers(body: str) -> bool:
    """Return *True* if *body* still contains Cloudflare challenge markers."""
    if not body:
        return False
    return any(pat.search(body) for pat in CF_CHALLENGE_BODY_PATTERNS)


def classify_challenge(response) -> ChallengeType:
    """Inspect a Scrapy *response* and return its Cloudflare challenge type."""
    server = response.headers.get(b"Server", b"")
    if isinstance(server, list):
        server = server[0]
    if isinstance(server, bytes):
        server = server.decode("utf-8", errors="replace")
    if not server.lower().startswith("cloudflare"):
        return ChallengeType.NONE

    if is_private_token_challenge_response(response.url, response.status, response.headers):
        return ChallengeType.PRIVATE_TOKEN

    if response.status not in (403, 429, 503):
        return ChallengeType.NONE

    cf_mitigated = response.headers.get(b"Cf-Mitigated", b"")
    if isinstance(cf_mitigated, list):
        cf_mitigated = cf_mitigated[0]
    if isinstance(cf_mitigated, bytes):
        cf_mitigated = cf_mitigated.decode("utf-8", errors="replace")

    body = ""
    if response.body:
        try:
            body = response.body.decode("utf-8", errors="replace")
        except Exception:
            pass

    # Turnstile markers (most specific first)
    if (
        _re.search(r'class="cf-turnstile"', body)
        or _re.search(r'challenges\.cloudflare\.com/turnstile', body)
        or _re.search(r'data-sitekey="[0-9A-Za-z]+"', body)
    ):
        return ChallengeType.TURNSTILE

    # Managed / captcha challenge
    if _re.search(
        r'challenge-platform/\S+orchestrate/(captcha|managed)/v1', body,
    ):
        return ChallengeType.MANAGED_CHALLENGE

    # JS challenge
    if _re.search(
        r'challenge-platform/\S+orchestrate/jsch/v1', body,
    ):
        return ChallengeType.JS_CHALLENGE

    # Cf-Mitigated header fallback
    if cf_mitigated.lower() == "challenge":
        return ChallengeType.JS_CHALLENGE

    if response.status == 403:
        return ChallengeType.PLAIN_403

    return ChallengeType.NONE


def is_private_token_challenge_response(url: str, status: int, headers: dict) -> bool:
    """Return True when response metadata matches a Cloudflare PAT challenge."""
    if status != 401:
        return False
    if "/cdn-cgi/challenge-platform/" not in url or "pat/" not in url:
        return False

    www_authenticate = None
    for key, value in headers.items():
        key_str = key.decode("utf-8", errors="replace") if isinstance(key, bytes) else str(key)
        if key_str.lower() == "www-authenticate":
            www_authenticate = value
            break
    if isinstance(www_authenticate, list):
        www_authenticate = www_authenticate[0] if www_authenticate else None
    if isinstance(www_authenticate, bytes):
        www_authenticate = www_authenticate.decode("utf-8", errors="replace")
    return bool(www_authenticate and "PrivateToken challenge=" in str(www_authenticate))


def resolve_first_party_url(
    request: Request,
    seed_url: Optional[str] = None,
    *,
    for_fetch: bool = False,
) -> str:
    """Resolve the primary first-party URL for a request.

    Used for both challenge solving and clearance validation.

    *for_fetch* should be True when the request uses ``playwright_fetch`` —
    API endpoints won't render a challenge page, so we fall back to
    Referer / Origin / domain root.
    """
    if seed_url:
        return seed_url

    if for_fetch or request.meta.get("playwright_fetch"):
        referer = request.headers.get("Referer")
        if referer:
            return referer.decode("utf-8") if isinstance(referer, bytes) else referer
        origin = request.headers.get("Origin")
        if origin:
            o = origin.decode("utf-8") if isinstance(origin, bytes) else origin
            return o.rstrip("/") + "/"
        parsed = urlparse(request.url)
        return f"{parsed.scheme}://{parsed.netloc}/"

    return request.url
