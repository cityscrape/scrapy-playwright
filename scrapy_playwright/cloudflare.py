"""**DEPRECATED** — Kept for backward compatibility only.

Cloudflare challenge handling has moved into the Playwright download handler
(``playwright.py``).  The handler-owned flow is now the single authority for
challenge detection, solve coordination, and clearance validation.

Do **not** register this middleware alongside the handler-owned flow;
doing so would create two competing challenge-resolution paths.
"""

import logging
from collections import defaultdict
from typing import Dict, List, Optional

from scrapy import Request, Spider, signals
from scrapy.exceptions import IgnoreRequest, NotConfigured
from scrapy.http import Response

from scrapy_playwright.page import PageMethod

logger = logging.getLogger("scrapy-playwright.cloudflare")

# Sentinel URL used for internal seed requests.
_SEED_URL_MARKER = "__cf_challenge_seed__"


class CloudflareChallengeRetryMiddleware:
    """Retry requests that receive a Cloudflare Turnstile challenge.

    Settings
    --------
    PLAYWRIGHT_CLOUDFLARE_CHALLENGE_RETRY : bool (default False)
        Master switch.
    PLAYWRIGHT_CLOUDFLARE_MAX_RETRIES : int (default 3)
        Maximum times any single URL will be retried after a challenge.
    PLAYWRIGHT_CLOUDFLARE_SEED_URL : str (optional)
        Explicit URL to navigate for the challenge seed.  If unset, the
        middleware re-uses the challenged URL.
    PLAYWRIGHT_CLOUDFLARE_SEED_TIMEOUT : int (default 90000)
        Page-level navigation timeout (ms) for the seed request.
    PLAYWRIGHT_CLOUDFLARE_WAIT_TIMEOUT : int (default 60000)
        Timeout (ms) for the ``cf_clearance`` cookie wait function.
    PLAYWRIGHT_DEFAULT_CONTEXT : str (optional)
        Context name forwarded to seed requests so cookies are shared.
    """

    def __init__(self, settings):
        self.max_retries: int = settings.getint(
            "PLAYWRIGHT_CLOUDFLARE_MAX_RETRIES", 3
        )
        self.seed_url: Optional[str] = settings.get("PLAYWRIGHT_CLOUDFLARE_SEED_URL")
        self.seed_timeout: int = settings.getint(
            "PLAYWRIGHT_CLOUDFLARE_SEED_TIMEOUT", 90_000
        )
        self.wait_timeout: int = settings.getint(
            "PLAYWRIGHT_CLOUDFLARE_WAIT_TIMEOUT", 60_000
        )
        self.default_context: Optional[str] = settings.get("PLAYWRIGHT_DEFAULT_CONTEXT")

        # url → retry count
        self._retry_counts: Dict[str, int] = defaultdict(int)
        # parked requests waiting for a seed to resolve
        self._parked: List[Request] = []
        # True while a seed request is in-flight
        self._seed_in_flight: bool = False

    @classmethod
    def from_crawler(cls, crawler):
        if not crawler.settings.getbool("PLAYWRIGHT_CLOUDFLARE_CHALLENGE_RETRY", False):
            raise NotConfigured
        o = cls(crawler.settings)
        crawler.signals.connect(o._spider_closed, signal=signals.spider_closed)
        return o

    # ------------------------------------------------------------------
    # Detection
    # ------------------------------------------------------------------

    @staticmethod
    def _is_challenge(response: Response) -> bool:
        if response.status != 403:
            return False
        cf_mitigated = response.headers.get(b"Cf-Mitigated", b"").decode("utf-8", errors="replace")
        return cf_mitigated.lower() == "challenge"

    # ------------------------------------------------------------------
    # Seed request builder
    # ------------------------------------------------------------------

    def _build_seed_request(self, challenged_url: str) -> Request:
        url = self.seed_url or challenged_url
        meta = {
            "playwright": True,
            "playwright_include_page": True,
            _SEED_URL_MARKER: True,
            "playwright_page_goto_kwargs": {
                "wait_until": "networkidle",
                "timeout": self.seed_timeout,
            },
            "playwright_page_methods": [
                PageMethod(
                    "wait_for_function",
                    '() => document.cookie.includes("cf_clearance")',
                    timeout=self.wait_timeout,
                ),
            ],
        }
        if self.default_context:
            meta["playwright_context"] = self.default_context

        return Request(
            url=url,
            callback=self._seed_callback,
            errback=self._seed_errback,
            dont_filter=True,
            priority=1000,
            meta=meta,
        )

    # ------------------------------------------------------------------
    # Seed callbacks
    # ------------------------------------------------------------------

    async def _seed_callback(self, response: Response):
        """Called when the seed navigation succeeds."""
        page = response.meta.get("playwright_page")
        if page:
            try:
                cookies = await page.context.cookies()
                names = [c["name"] for c in cookies]
                logger.info(
                    "[Cloudflare] Seed resolved — extracted cookies: %s",
                    names,
                )
            finally:
                await page.close()
        self._seed_in_flight = False
        for req in self._flush_parked():
            yield req

    def _seed_errback(self, failure):
        logger.error(
            "[Cloudflare] Seed request failed: %s", failure.getErrorMessage()
        )
        self._seed_in_flight = False
        # Drop all parked requests — we can't solve the challenge.
        dropped = len(self._parked)
        self._parked.clear()
        if dropped:
            logger.warning(
                "[Cloudflare] Dropped %d parked requests after seed failure",
                dropped,
            )

    # ------------------------------------------------------------------
    # Parked-request management
    # ------------------------------------------------------------------

    def _flush_parked(self):
        """Yield copies of all parked requests for retry."""
        reqs = list(self._parked)
        self._parked.clear()
        logger.info("[Cloudflare] Flushing %d parked request(s)", len(reqs))
        for req in reqs:
            yield req.copy()

    # ------------------------------------------------------------------
    # Middleware interface
    # ------------------------------------------------------------------

    def process_response(self, request: Request, response: Response, spider: Spider):
        # Let seed responses pass through (handled by callback).
        if request.meta.get(_SEED_URL_MARKER):
            return response

        if not self._is_challenge(response):
            return response

        url = request.url
        self._retry_counts[url] += 1
        if self._retry_counts[url] > self.max_retries:
            logger.warning(
                "[Cloudflare] Max retries (%d) reached for %s — giving up",
                self.max_retries, url,
            )
            return response

        logger.info(
            "[Cloudflare] Challenge detected on %s (attempt %d/%d)",
            url, self._retry_counts[url], self.max_retries,
        )

        # Park this request for later retry.
        self._parked.append(request.copy())

        # If there's no seed in-flight, schedule one.
        if not self._seed_in_flight:
            self._seed_in_flight = True
            seed = self._build_seed_request(url)
            logger.info("[Cloudflare] Scheduling challenge seed → %s", seed.url)
            spider.crawler.engine.crawl(seed)

        # Tell Scrapy to ignore this response (the request is parked).
        raise IgnoreRequest(f"Cloudflare challenge — request parked for retry: {url}")

    def _spider_closed(self, spider: Spider, reason: str):
        if self._parked:
            logger.warning(
                "[Cloudflare] Spider closing with %d parked request(s) — dropping",
                len(self._parked),
            )
            self._parked.clear()
