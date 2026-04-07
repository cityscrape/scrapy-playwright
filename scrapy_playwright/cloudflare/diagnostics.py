"""Cloudflare diagnostic logging helpers.

Keeps Turnstile diagnostics as first-class, moves verbose diagnostics
behind a dedicated flag, and removes site-specific URL filtering.
"""

import json as _json
import logging
import pathlib
import re as _re
from time import time
from typing import TYPE_CHECKING, Optional
from urllib.parse import urlparse

from playwright.async_api import (
    Page,
    Request as PlaywrightRequest,
    Response as PlaywrightResponse,
)
from scrapy.http import Request, Response

from scrapy_playwright.cloudflare.types import (
    ContextChallengeInfo,
    ChallengeType,
    body_has_challenge_markers,
)
from scrapy_playwright import signals as pw_signals

if TYPE_CHECKING:
    from scrapy_playwright.playwright import PlaywrightContext

logger = logging.getLogger("scrapy-playwright")


class CloudflareDiagnostics:
    """Encapsulates all Cloudflare-related diagnostic logging.

    Receives a reference to the engine only for stats helpers and
    ``_sanitize_for_log``.
    """

    def __init__(self, engine) -> None:
        self._engine = engine

    # ── Turnstile diagnostics (first-class) ────────────────────────────

    def log_turnstile_diagnostics(self, response: Response) -> None:
        """Extract and log Turnstile sitekey/action for future provider integration."""
        body = ""
        if response.body:
            try:
                body = response.body.decode("utf-8", errors="replace")
            except Exception:
                return
        sitekey_match = _re.search(r'data-sitekey="([0-9A-Za-z]+)"', body)
        action_match = _re.search(
            r'<form\s[^>]*action="([^"]+)"', body, _re.DOTALL,
        )
        logger.info(
            "[Cloudflare] Turnstile diagnostics — "
            "sitekey=%s, action=%s, url=%s",
            sitekey_match.group(1) if sitekey_match else "<not found>",
            action_match.group(1) if action_match else "<not found>",
            response.url,
        )
        self._engine._set_stat(
            "playwright/cloudflare/turnstile_sitekey",
            sitekey_match.group(1) if sitekey_match else None,
        )

    # ── Response / context diagnostics ──────────────────────────────────

    def log_response_summary(
        self, response: Response, label: str, request: Optional[Request] = None,
    ) -> None:
        body_text = ""
        if response.body:
            try:
                body_text = response.body.decode("utf-8", errors="replace")
            except Exception:
                body_text = ""
        snippet = _re.sub(r"\s+", " ", body_text[:300]).strip() if body_text else ""
        logger.info(
            "[Cloudflare] Response summary [%s]: req=%s %s resp_url=%s status=%s "
            "server=%r cf-mitigated=%r location=%r body_len=%d body_has_markers=%s "
            "snippet=%r",
            label,
            request.method if request else "<unknown>",
            request.url if request else "<unknown>",
            response.url,
            response.status,
            response.headers.get(b"Server"),
            response.headers.get(b"Cf-Mitigated"),
            response.headers.get(b"Location"),
            len(response.body or b""),
            body_has_challenge_markers(body_text),
            snippet,
        )

    def log_context_pages(self, pw_ctx: "PlaywrightContext", label: str) -> None:
        pages = []
        for idx, page in enumerate(pw_ctx.context.pages):
            try:
                pages.append({
                    "index": idx,
                    "closed": page.is_closed(),
                    "url": page.url,
                })
            except Exception:
                pages.append({"index": idx, "closed": True, "url": "<unavailable>"})
        logger.info(
            "[Cloudflare] Context pages [%s] context='%s': %s",
            label, pw_ctx.name, _json.dumps(pages, ensure_ascii=False),
        )

    async def log_context_cookies(
        self, pw_ctx: "PlaywrightContext", label: str,
    ) -> None:
        cookies = await pw_ctx.context.cookies()
        cf_cookies = [
            {
                "name": c.get("name"),
                "domain": c.get("domain"),
                "path": c.get("path"),
                "expires": c.get("expires"),
                "httpOnly": c.get("httpOnly"),
                "secure": c.get("secure"),
            }
            for c in cookies
            if "cf" in c.get("name", "").lower()
        ]
        logger.info(
            "[Cloudflare] Context cookies [%s] context='%s': %s",
            label, pw_ctx.name, _json.dumps(cf_cookies, ensure_ascii=False),
        )

    async def log_page_snapshot(
        self,
        page: Page,
        pw_ctx: "PlaywrightContext",
        label: str,
        request: Optional[Request] = None,
    ) -> None:
        try:
            snapshot = await page.evaluate(
                """() => {
                    const nav = navigator;
                    const glCanvas = document.createElement('canvas');
                    let webgl = null;
                    try {
                        const gl = glCanvas.getContext('webgl') || glCanvas.getContext('experimental-webgl');
                        if (gl) {
                            const dbg = gl.getExtension('WEBGL_debug_renderer_info');
                            webgl = {
                                vendor: dbg ? gl.getParameter(dbg.UNMASKED_VENDOR_WEBGL) : null,
                                renderer: dbg ? gl.getParameter(dbg.UNMASKED_RENDERER_WEBGL) : null,
                            };
                        }
                    } catch (e) {}
                    return {
                        href: location.href,
                        title: document.title,
                        readyState: document.readyState,
                        webdriver: nav.webdriver,
                        userAgent: nav.userAgent,
                        language: nav.language,
                        languages: nav.languages,
                        platform: nav.platform,
                        vendor: nav.vendor,
                        hardwareConcurrency: nav.hardwareConcurrency,
                        deviceMemory: nav.deviceMemory,
                        maxTouchPoints: nav.maxTouchPoints,
                        cookieEnabled: nav.cookieEnabled,
                        innerWidth: window.innerWidth,
                        innerHeight: window.innerHeight,
                        outerWidth: window.outerWidth,
                        outerHeight: window.outerHeight,
                        screenWidth: window.screen.width,
                        screenHeight: window.screen.height,
                        colorDepth: window.screen.colorDepth,
                        pixelRatio: window.devicePixelRatio,
                        hasChrome: !!window.chrome,
                        timezone: Intl.DateTimeFormat().resolvedOptions().timeZone,
                        uaData: nav.userAgentData ? {
                            mobile: nav.userAgentData.mobile,
                            platform: nav.userAgentData.platform,
                            brands: nav.userAgentData.brands,
                        } : null,
                        webgl,
                    };
                }"""
            )
            logger.info(
                "[Cloudflare] Page snapshot [%s] context='%s' req=%s %s: %s",
                label,
                pw_ctx.name,
                request.method if request else "<unknown>",
                request.url if request else "<unknown>",
                _json.dumps(snapshot, ensure_ascii=False),
            )
        except Exception as exc:
            logger.debug(
                "[Cloudflare] Failed to collect page snapshot [%s]: %s",
                label, exc, exc_info=True,
            )

    def dump_challenge_html(
        self, body: str, context_name: str, info: ContextChallengeInfo,
    ) -> None:
        """Persist a snippet of challenge HTML for post-mortem analysis."""
        snippet = body[:4096] if body else "<empty>"
        logger.debug(
            "[Cloudflare] Challenge HTML snippet (context='%s', type=%s):\n%s",
            context_name, info.challenge_type.value, snippet,
        )
        try:
            output_dir = pathlib.Path("output") / "weird_responses"
            output_dir.mkdir(parents=True, exist_ok=True)
            file_path = output_dir / (
                f"cloudflare_{context_name}_{info.challenge_type.value}_{int(time())}.html"
            )
            file_path.write_text(body or "", encoding="utf-8")
            logger.info("[Cloudflare] Persisted challenge HTML to %s", file_path)
        except Exception as exc:
            logger.debug(
                "[Cloudflare] Failed to persist challenge HTML: %s",
                exc, exc_info=True,
            )

    # ── Network event logging ──────────────────────────────────────────

    async def log_request_event(self, request: PlaywrightRequest) -> None:
        try:
            headers = await request.all_headers()
            logger.info(
                "[Cloudflare] Network request: method=%s resource_type=%s navigation=%s "
                "url=%s headers=%s",
                request.method,
                request.resource_type,
                request.is_navigation_request(),
                request.url,
                _json.dumps(
                    self._engine._sanitize_for_log(headers),
                    ensure_ascii=False,
                    sort_keys=True,
                ),
            )
        except Exception as exc:
            logger.debug(
                "[Cloudflare] Failed to log request headers for %s: %s",
                request.url,
                exc,
                exc_info=True,
            )

    async def log_response_event(self, response: PlaywrightResponse) -> None:
        try:
            headers = await response.all_headers()
            private_token_challenge = None
            if (
                response.status == 401
                and "/cdn-cgi/challenge-platform/" in response.url
                and "pat/" in response.url
            ):
                www_authenticate = headers.get("www-authenticate", "")
                if "PrivateToken challenge=" in www_authenticate:
                    private_token_challenge = {
                        "url": response.url,
                        "www_authenticate": www_authenticate,
                    }
            logger.info(
                "[Cloudflare] Network response: method=%s resource_type=%s status=%s "
                "url=%s server=%r cf-mitigated=%r set-cookie=%r headers=%s",
                response.request.method,
                response.request.resource_type,
                response.status,
                response.url,
                headers.get("server"),
                headers.get("cf-mitigated"),
                headers.get("set-cookie"),
                _json.dumps(
                    self._engine._sanitize_for_log(headers),
                    ensure_ascii=False,
                    sort_keys=True,
                ),
            )
            if private_token_challenge is not None:
                self._engine._inc_stat("playwright/cloudflare/private_token_challenge_count")
                self._engine._set_stat(
                    "playwright/cloudflare/last_private_token_challenge_url",
                    private_token_challenge["url"],
                )
                request = getattr(response, "request", None)
                if request is not None:
                    context_name = getattr(request.frame.page, "context", None)
                    self._engine._emit_signal(
                        pw_signals.playwright_blocked,
                        request=None,
                        url=request.url,
                        method=request.method,
                        mode="page" if request.resource_type == "document" else request.resource_type,
                        context_name=getattr(getattr(context_name, "_impl_obj", None), "_guid", "unknown"),
                        challenge_type=ChallengeType.MANAGED_CHALLENGE.value,
                        blocked_reason="private_token_challenge",
                        status=response.status,
                        resp_url=response.url,
                        cf_mitigated=headers.get("cf-mitigated"),
                    )
                logger.warning(
                    "[Cloudflare] PrivateToken challenge encountered at %s. "
                    "The browser did not satisfy Cloudflare's PAT step and received 401 "
                    "with WWW-Authenticate=%r. This usually indicates platform/browser "
                    "support is missing or unavailable in the automated session.",
                    private_token_challenge["url"],
                    private_token_challenge["www_authenticate"],
                )
        except Exception as exc:
            logger.debug(
                "[Cloudflare] Failed to log response headers for %s: %s",
                response.url,
                exc,
                exc_info=True,
            )

    @staticmethod
    def should_log_network_event(request: "PlaywrightRequest") -> bool:
        """Decide whether a network event is Cloudflare-relevant enough to log."""
        url = request.url
        resource_type = request.resource_type
        is_nav = request.is_navigation_request()
        host = (urlparse(url).netloc or "").lower()
        if "cloudflare.com" not in host:
            if not is_nav:
                return False
        if "/cdn-cgi/challenge-platform/" in url:
            return True
        if is_nav and resource_type == "document":
            return True
        return False
