"""Playwright facade — manages browser contexts, page pools, and request
execution (both navigation and fetch modes).

``handler.py`` instantiates a :class:`Playwright` facade and delegates all
browser work to it.  Everything Scrapy-specific (Twisted deferreds,
``download_request`` branching, ``Config.from_settings``) stays in the handler.
"""

import asyncio
import enum
import json as _json
import logging
import pathlib
import random
import re as _re
from dataclasses import dataclass
from contextlib import suppress
from functools import partial
from ipaddress import ip_address
from time import time
from typing import Callable, Dict, Optional, Tuple, Union
from urllib.parse import urlparse

from playwright._impl._errors import TargetClosedError
from playwright.async_api import (
    Browser,
    BrowserContext,
    BrowserType,
    Download as PlaywrightDownload,
    Error as PlaywrightError,
    Page,
    Playwright as AsyncPlaywright,
    PlaywrightContextManager,
    Request as PlaywrightRequest,
    Response as PlaywrightResponse,
    Route,
)
from scrapy import Spider
from scrapy.http import Request, Response
from scrapy.http.headers import Headers
from scrapy.responsetypes import responsetypes
from scrapy.utils.misc import load_object

from scrapy_playwright.network_recorder import NetworkRecorder
from scrapy_playwright.page import PageMethod, PagePool, POOL_STRATEGY_REUSE_FIRST
from scrapy_playwright._utils import (
    _encode_body,
    _get_header_value,
    _get_page_content,
    _is_safe_close_error,
    _maybe_await,
)

logger = logging.getLogger("scrapy-playwright")

DEFAULT_CONTEXT_NAME = "default"
PERSISTENT_CONTEXT_PATH_KEY = "user_data_dir"
CAMOUFOX_BROWSER_TYPE = "camoufox"


# ── Download container ─────────────────────────────────────────────────

class Download:
    """Accumulates data produced by Playwright download events."""
    __slots__ = (
        "body", "url", "suggested_filename", "exception",
        "response_status", "headers",
    )

    def __init__(self) -> None:
        self.body: bytes = b""
        self.url: str = ""
        self.suggested_filename: str = ""
        self.exception: Optional[Exception] = None
        self.response_status: int = 200
        self.headers: dict = {}

    def __bool__(self) -> bool:
        return bool(self.body) or bool(self.exception)


# ── Cloudflare challenge types ─────────────────────────────────────────

class ChallengeType(enum.Enum):
    """Classification of Cloudflare challenge responses."""
    NONE = "none"
    JS_CHALLENGE = "js_challenge"            # orchestrate/jsch
    MANAGED_CHALLENGE = "managed_challenge"   # orchestrate/managed or captcha
    TURNSTILE = "turnstile"                  # cf-turnstile widget / turnstile API
    PLAIN_403 = "plain_403"                  # 403 from Cloudflare, no challenge markers


class ChallengeState(enum.Enum):
    """Per-context challenge solving state."""
    IDLE = "idle"
    SOLVING = "solving"
    VALIDATED = "validated"
    FAILED = "failed"


# Body patterns that indicate an active Cloudflare challenge page.
_CF_CHALLENGE_BODY_PATTERNS = [
    _re.compile(r'challenge-platform/\S+orchestrate/(captcha|managed|jsch)/v1', _re.M | _re.S),
    _re.compile(r'class="cf-turnstile"', _re.M | _re.S),
    _re.compile(r'challenges\.cloudflare\.com/turnstile', _re.M | _re.S),
    _re.compile(r'data-sitekey="[0-9A-Za-z]+"', _re.M | _re.S),
    _re.compile(r'window\._cf_chl_opt', _re.M | _re.S),
    _re.compile(r'id="challenge-form"', _re.M | _re.S),
]


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

    def reset(self) -> None:
        self.state = ChallengeState.IDLE
        self.challenge_type = ChallengeType.NONE
        self.attempt_count = 0
        self.carrier_page = None
        self.error = None
        self.solver_marker = None
        if self.solve_event:
            self.solve_event.clear()


# ── PlaywrightContext ──────────────────────────────────────────────────

class PlaywrightContext:
    """Wraps a single Playwright browser context with its page pool,
    semaphore, and optional network recorder.

    The :meth:`process` method is the main entry point: it acquires a
    page, dispatches to the appropriate handler (navigation or fetch),
    and returns the page to the pool.
    """

    def __init__(
        self,
        name: str,
        context: BrowserContext,
        max_pages: int,
        *,
        persistent: bool = False,
        pool_strategy: str = POOL_STRATEGY_REUSE_FIRST,
        pool_enabled: bool = True,
        init_scripts: Optional[list] = None,
        recorder: Optional[NetworkRecorder] = None,
        navigation_timeout: Optional[float] = None,
    ) -> None:
        self.name = name
        self.context = context
        self.persistent = persistent
        self.recorder = recorder
        self._navigation_timeout = navigation_timeout

        if pool_enabled:
            self.pool = PagePool(
                max_pages,
                strategy=pool_strategy,
                init_scripts=init_scripts,
            )
        else:
            self.pool = PagePool(
                max_pages,
                strategy=pool_strategy,
                init_scripts=init_scripts,
            )

        # Method-based dispatch — extend this dict to add custom handlers.
        self.method_handlers: Dict[str, Callable] = {
            "POST": self._process_post,
        }

    # -- page lifecycle -------------------------------------------------

    async def acquire_page(self) -> Tuple[Page, bool]:
        """Return ``(page, is_new)``."""
        return await self.pool.acquire(self.context)

    def return_page(self, page: Page) -> None:
        self.pool.release(page)

    async def drain_pool(self) -> None:
        await self.pool.drain()

    # -- request processing (navigation/fetch) --------------------------

    async def process(self, request: Request) -> Optional[PlaywrightResponse]:
        """Acquire a page, execute the request, return the page."""
        page: Optional[Page] = None
        result = None
        try:
            page, is_new = await self.acquire_page()
            handler = self.method_handlers.get(request.method, self._process_default)
            result = await handler(request, page, is_new)
        finally:
            if page is not None:
                self.return_page(page)
        return result

    async def _process_default(
        self, request: Request, page: Page, is_new: bool,
    ) -> Optional[PlaywrightResponse]:
        """Navigate to the request URL and return the Playwright response."""
        return await page.goto(request.url, wait_until="networkidle")

    async def _process_post(
        self, request: Request, page: Page, is_new: bool,
    ) -> Optional[PlaywrightResponse]:
        """For POST requests, seed the page with the referer first."""
        if is_new:
            referer = request.headers.get("Referer")
            if referer:
                seed = referer.decode("utf-8") if isinstance(referer, bytes) else referer
                await page.goto(seed, wait_until="networkidle")
        # TODO: use fetch for POST
        return None

    async def get_open_page(self) -> Optional[Page]:
        """Return an existing non-closed page, or ``None``."""
        for page in self.context.pages:
            if not page.is_closed():
                return page
        return None


# ── Playwright facade ─────────────────────────────────────────────────

class Playwright:
    """High-level facade around the Playwright driver, browser, and
    context lifecycle.

    The Scrapy download handler creates one instance and delegates all
    browser work here.  No Scrapy-specific types (Twisted, signals, etc.)
    leak into this class — those stay in the handler.
    """

    def __init__(
        self,
        *,
        browser_type_name: str,
        launch_options: dict,
        camoufox_options: Optional[dict] = None,
        max_pages_per_context: int,
        navigation_timeout: Optional[float] = None,
        close_page_after_request: bool = False,
        pool_enabled: bool = True,
        pool_strategy: str = POOL_STRATEGY_REUSE_FIRST,
        stealth_init_scripts: Optional[list] = None,
        har_recording: bool = False,
        har_output_dir: pathlib.Path = pathlib.Path("har_recordings"),
        har_url_filter: Optional[str] = None,
        cdp_url: Optional[str] = None,
        cdp_kwargs: Optional[dict] = None,
        connect_url: Optional[str] = None,
        connect_kwargs: Optional[dict] = None,
        max_contexts: Optional[int] = None,
        restart_disconnected_browser: bool = True,
        process_request_headers: Optional[Callable] = None,
        abort_request: Optional[Callable] = None,
        cf_challenge_retry: bool = False,
        cf_max_retries: int = 3,
        cf_seed_url: Optional[str] = None,
        cf_seed_timeout: int = 90_000,
        cf_wait_timeout: int = 60_000,
    ) -> None:
        # Connection
        self._cdp_url = cdp_url
        self._cdp_kwargs = cdp_kwargs or {}
        self._connect_url = connect_url
        self._connect_kwargs = connect_kwargs or {}

        # Browser / context config
        self._browser_type_name = browser_type_name
        self._launch_options = launch_options
        self._camoufox_options = camoufox_options or {}
        self._max_pages_per_context = max_pages_per_context
        self._max_contexts = max_contexts
        self._navigation_timeout = navigation_timeout
        self._close_page_after_request = close_page_after_request
        self._pool_enabled = pool_enabled
        self._pool_strategy = pool_strategy
        self._stealth_init_scripts = stealth_init_scripts
        self._restart_disconnected_browser = restart_disconnected_browser

        # HAR
        self._har_recording = har_recording
        self._har_output_dir = har_output_dir
        self._har_url_filter = har_url_filter

        # Request processing
        self.process_request_headers = process_request_headers
        self.abort_request = abort_request

        # Runtime state
        self.playwright_context_manager: Optional[PlaywrightContextManager] = None
        self.playwright: Optional[AsyncPlaywright] = None
        self.browser_type: Optional[BrowserType] = None
        self._browser: Optional[Browser] = None
        self._camoufox_launchers: Dict[str, object] = {}
        self._driver_dead = False
        self._is_closing = False

        self._browser_launch_lock = asyncio.Lock()
        self._context_launch_lock = asyncio.Lock()
        self._restart_lock = asyncio.Lock()

        self.contexts: Dict[str, PlaywrightContext] = {}
        self._context_semaphore: Optional[asyncio.Semaphore] = (
            asyncio.Semaphore(value=max_contexts) if max_contexts else None
        )

        # Method-based dispatch — determines fetch vs page navigation.
        # Extend or override to add custom handlers per HTTP method.
        self.method_handlers: Dict[str, Callable] = {
            "GET": self.download_with_page,
        }
        self._default_handler = self.download_with_page

        # Cloudflare challenge state
        self._cf_enabled = cf_challenge_retry
        self._cf_max_retries = cf_max_retries
        self._cf_seed_url = cf_seed_url
        self._cf_seed_timeout = cf_seed_timeout
        self._cf_wait_timeout = cf_wait_timeout
        self._cf_challenge_info: Dict[str, ContextChallengeInfo] = {}
        self._cf_solve_lock = asyncio.Lock()

        # Callback hooks — the handler wires these after construction.
        self.on_stats_inc: Optional[Callable] = None
        self.on_stats_set: Optional[Callable] = None

    # -- property shortcuts ---------------------------------------------

    @property
    def is_closing(self) -> bool:
        return self._is_closing

    @is_closing.setter
    def is_closing(self, value: bool) -> None:
        self._is_closing = value

    @property
    def driver_dead(self) -> bool:
        return self._driver_dead

    @driver_dead.setter
    def driver_dead(self, value: bool) -> None:
        self._driver_dead = value

    # -- stats helpers --------------------------------------------------

    def _inc_stat(self, name: str) -> None:
        if self.on_stats_inc:
            self.on_stats_inc(name)

    def _set_stat(self, name: str, value) -> None:
        if self.on_stats_set:
            self.on_stats_set(name, value)

    @staticmethod
    def _sanitize_for_log(data):
        if isinstance(data, dict):
            sanitized = {}
            for key, value in data.items():
                key_lower = str(key).lower()
                if any(secret in key_lower for secret in ("password", "token", "secret", "authorization")):
                    sanitized[key] = "<redacted>"
                else:
                    sanitized[key] = Playwright._sanitize_for_log(value)
            return sanitized
        if isinstance(data, list):
            return [Playwright._sanitize_for_log(item) for item in data]
        return data

    @property
    def _uses_camoufox_backend(self) -> bool:
        return self._browser_type_name == CAMOUFOX_BROWSER_TYPE

    @staticmethod
    def _load_camoufox() -> type:
        try:
            from camoufox.async_api import AsyncCamoufox
        except ImportError as exc:
            raise RuntimeError(
                "Camoufox backend requested but the 'camoufox' package is not installed. "
                "Install it and run 'python -m camoufox fetch' before using this backend."
            ) from exc
        return AsyncCamoufox

    def _prepare_camoufox_launch_options(
        self,
        context_kwargs: Optional[dict] = None,
    ) -> dict:
        launch_kwargs = dict(self._launch_options)
        launch_kwargs.update(self._camoufox_options)
        if context_kwargs:
            launch_kwargs.update(context_kwargs)
        for unsupported_key in (
            "channel",
            "executable_path",
            "ignore_default_args",
            "args",
        ):
            if unsupported_key in launch_kwargs:
                logger.warning(
                    "[Lifecycle] Ignoring Camoufox-incompatible launch option %r",
                    unsupported_key,
                )
                launch_kwargs.pop(unsupported_key, None)
        if launch_kwargs.get(PERSISTENT_CONTEXT_PATH_KEY):
            launch_kwargs.setdefault("persistent_context", True)
        return launch_kwargs

    async def _close_camoufox_launchers(self) -> None:
        for name, launcher in list(self._camoufox_launchers.items()):
            with suppress(Exception):
                await launcher.__aexit__(None, None, None)
            self._camoufox_launchers.pop(name, None)

    async def _close_camoufox_launcher(self, name: str, launcher: object) -> None:
        with suppress(Exception):
            await launcher.__aexit__(None, None, None)
        self._camoufox_launchers.pop(name, None)

    # ── Public entry point ─────────────────────────────────────────────

    async def process(self, request: Request, spider: Spider) -> Response:
        """Single entry point for downloading a request.

        If ``playwright_fetch`` is set in request meta the fetch path is
        forced.  Otherwise ``method_handlers`` decides which strategy to
        use based on the HTTP method.

        When Cloudflare challenge retry is enabled, challenged responses
        trigger a reactive solve cycle on the page/context that encountered
        the challenge.  Other requests for the same context park behind the
        active cycle and resume after validation.
        """
        context_name = request.meta.get("playwright_context", DEFAULT_CONTEXT_NAME)

        # If a solve cycle is already running for this context, wait for it.
        if self._cf_enabled:
            info = self._cf_get_info(context_name)
            if info.state == ChallengeState.SOLVING:
                await self._cf_wait_for_solve(info, request)

        response = await self._dispatch(request, spider)

        if self._cf_enabled:
            challenge_type = self.classify_challenge(response)
            if challenge_type not in (ChallengeType.NONE, ChallengeType.PLAIN_403):
                self._cf_log_response_summary(
                    response, label="challenge_detected", request=request,
                )
                response = await self._cf_handle(
                    request, response, spider, context_name, challenge_type,
                )
        return response

    async def _dispatch(self, request: Request, spider: Spider) -> Response:
        """Dispatch to fetch or page-navigation handler."""
        if request.meta.get("playwright_fetch"):
            return await self.download_with_fetch(request=request, spider=spider)
        handler = self.method_handlers.get(request.method, self._default_handler)
        return await handler(request=request, spider=spider)

    # ── Cloudflare challenge classification ────────────────────────────

    @staticmethod
    def classify_challenge(response: Response) -> ChallengeType:
        """Inspect a Scrapy *response* and return its Cloudflare challenge type.

        Uses cloudscraper-derived markers: ``Server: cloudflare``,
        ``Cf-Mitigated: challenge``, orchestrate script paths, Turnstile
        DOM markers, and ``data-sitekey``.
        """
        server = response.headers.get(b"Server", b"")
        if isinstance(server, list):
            server = server[0]
        if isinstance(server, bytes):
            server = server.decode("utf-8", errors="replace")
        if not server.lower().startswith("cloudflare"):
            return ChallengeType.NONE

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

    @staticmethod
    def _body_has_challenge_markers(body: str) -> bool:
        """Return *True* if *body* still contains Cloudflare challenge markers."""
        if not body:
            return False
        return any(pat.search(body) for pat in _CF_CHALLENGE_BODY_PATTERNS)

    # ── Cloudflare per-context state helpers ───────────────────────────

    def _cf_get_info(self, context_name: str) -> ContextChallengeInfo:
        if context_name not in self._cf_challenge_info:
            self._cf_challenge_info[context_name] = ContextChallengeInfo()
        return self._cf_challenge_info[context_name]

    async def _cf_wait_for_solve(
        self, info: ContextChallengeInfo, request: Request,
    ) -> None:
        """Park the current request until the in-flight solve cycle finishes."""
        if info.solve_event is None:
            return
        logger.info(
            "[Cloudflare] %s %s → parking behind active solve cycle",
            request.method, request.url,
        )
        self._inc_stat("playwright/cloudflare/parked_count")
        await info.solve_event.wait()

    # ── Cloudflare reactive challenge handling ─────────────────────────

    async def _cf_handle(
        self,
        request: Request,
        response: Response,
        spider: Spider,
        context_name: str,
        challenge_type: ChallengeType,
    ) -> Response:
        """React to a Cloudflare challenge: start or join a solve cycle,
        then retry the original request."""
        info = self._cf_get_info(context_name)
        solver_marker = object()
        is_solver = False

        async with self._cf_solve_lock:
            if info.state == ChallengeState.SOLVING:
                logger.info(
                    "[Cloudflare] %s %s joined existing solve cycle "
                    "(context='%s', attempt=%d, type=%s)",
                    request.method, request.url, context_name,
                    info.attempt_count, info.challenge_type.value,
                )
            else:
                # We are the first to see the challenge — start the cycle.
                info.state = ChallengeState.SOLVING
                info.challenge_type = challenge_type
                info.attempt_count += 1
                info.solve_event = asyncio.Event()
                info.error = None
                info.solver_marker = solver_marker
                is_solver = True

        if not is_solver:
            await self._cf_wait_for_solve(info, request)
            if info.state == ChallengeState.VALIDATED:
                logger.info(
                    "[Cloudflare] Solve cycle finished validated; retrying parked request "
                    "%s %s (context='%s')",
                    request.method, request.url, context_name,
                )
                return await self._dispatch(request, spider)
            logger.warning(
                "[Cloudflare] Solve cycle finished without validation; "
                "returning original challenged response for %s %s (context='%s')",
                request.method, request.url, context_name,
            )
            return response

        # Check if another request already started solving.
        if info.solve_event and info.solve_event.is_set():
            # Solve cycle already finished (race condition window).
            if info.state == ChallengeState.VALIDATED:
                return await self._dispatch(request, spider)
            return response

        # Check max retries.
        if info.attempt_count > self._cf_max_retries:
            logger.warning(
                "[Cloudflare] Max attempts (%d) for context '%s' — giving up",
                self._cf_max_retries, context_name,
            )
            self._inc_stat("playwright/cloudflare/max_retries_count")
            info.state = ChallengeState.FAILED
            if info.solve_event:
                info.solve_event.set()
            return response

        logger.info(
            "[Cloudflare] Challenge %s on %s %s "
            "(context='%s', attempt %d/%d)",
            challenge_type.value, request.method, request.url,
            context_name, info.attempt_count, self._cf_max_retries,
        )
        self._inc_stat("playwright/cloudflare/challenge_count")
        self._inc_stat(f"playwright/cloudflare/challenge_type/{challenge_type.value}")

        # Extract Turnstile diagnostics before attempting solve.
        if challenge_type == ChallengeType.TURNSTILE:
            self._cf_log_turnstile_diagnostics(response)

        try:
            solved = await self._cf_solve(request, spider, context_name, info)
            if solved:
                info.state = ChallengeState.VALIDATED
                logger.info(
                    "[Cloudflare] Solve SUCCESS for context '%s' "
                    "(type=%s, attempts=%d, validated_url=%s)",
                    context_name, info.challenge_type.value,
                    info.attempt_count, info.last_validated_url,
                )
                self._inc_stat("playwright/cloudflare/solve_success_count")
            else:
                info.state = ChallengeState.FAILED
                logger.warning(
                    "[Cloudflare] Solve FAILED for context '%s' "
                    "(type=%s, attempts=%d)",
                    context_name, info.challenge_type.value,
                    info.attempt_count,
                )
                self._inc_stat("playwright/cloudflare/solve_failed_count")
        except Exception as exc:
            info.state = ChallengeState.FAILED
            info.error = exc
            logger.error(
                "[Cloudflare] Solve ERROR for context '%s': %s",
                context_name, exc, exc_info=True,
            )
            self._inc_stat("playwright/cloudflare/solve_failed_count")
        finally:
            info.solver_marker = None
            if info.solve_event:
                info.solve_event.set()  # wake all parked requests

        if info.state == ChallengeState.VALIDATED:
            # Human-scale delay before retry (cloudscraper-inspired).
            await asyncio.sleep(1.0 + (0.5 * info.attempt_count))
            return await self._dispatch(request, spider)
        return response

    # ── Cloudflare solve cycle ─────────────────────────────────────────

    async def _cf_solve(
        self,
        request: Request,
        spider: Spider,
        context_name: str,
        info: ContextChallengeInfo,
    ) -> bool:
        """Navigate a page to trigger and solve the Cloudflare challenge
        in-browser, then validate clearance with a protected probe."""
        pw_ctx = self.contexts.get(context_name)
        if pw_ctx is None:
            pw_ctx = await self.get_or_create_context(request, spider)

        # Acquire a carrier page (prefer an existing open page).
        page = info.carrier_page
        page_is_new = False
        if page is None or page.is_closed():
            existing = await pw_ctx.get_open_page()
            if existing is not None:
                page = existing
            else:
                page, page_is_new = await pw_ctx.acquire_page()
                if page_is_new:
                    self._inc_stat("playwright/page_count")
                    page.on("close", self._make_close_page_cb(pw_ctx.name))
                    page.on("crash", self._make_close_page_cb(pw_ctx.name))
            info.carrier_page = page

        solve_url = self._cf_determine_solve_url(request)
        logger.info(
            "[Cloudflare] Navigating carrier page → %s (challenge=%s, context='%s')",
            solve_url, info.challenge_type.value, context_name,
        )
        await self._cf_log_page_snapshot(
            page, pw_ctx, "pre_solve_snapshot", request=request,
        )
        self._cf_log_context_pages(pw_ctx, "pre_solve_pages")

        # Known Cloudflare challenge page title markers (case-insensitive).
        _CF_TITLE_MARKERS = ("just a moment", "um momento")

        try:
            # If the page is already on the challenge URL (e.g. seed is
            # solving the same challenge), skip re-navigation to avoid
            # resetting the Turnstile widget and disrupting in-progress solves.
            page_title = await page.title()
            title_lower = (page_title or "").lower()
            already_on_challenge = any(m in title_lower for m in _CF_TITLE_MARKERS)

            if already_on_challenge:
                logger.info(
                    "[Cloudflare] Page already on challenge ('%s') — skipping navigation",
                    page_title,
                )
                # Quick check — another coroutine may have already solved it.
                has_clearance = await self._cf_poll_clearance(
                    pw_ctx, timeout_ms=5_000,
                )
                if has_clearance:
                    logger.info(
                        "[Cloudflare] cf_clearance already present — skipping click"
                    )
                else:
                    logger.info(
                        "[Cloudflare] No clearance yet — attempting Turnstile click"
                    )
                    clicked = await self._cf_click_turnstile(page)
                    logger.info(
                        "[Cloudflare] Turnstile click result on existing challenge page: %s",
                        clicked,
                    )
                    handoff = await self._cf_wait_for_handoff(
                        page,
                        pw_ctx,
                        request,
                        timeout_ms=self._cf_wait_timeout,
                    )
                    has_clearance = handoff["has_clearance"]
                    if not has_clearance and not handoff["handoff_completed"]:
                        has_clearance = await self._cf_poll_clearance(
                            pw_ctx, timeout_ms=self._cf_wait_timeout,
                        )
            else:
                await page.goto(
                    solve_url,
                    wait_until="domcontentloaded",
                    timeout=self._cf_seed_timeout,
                )

                # Give Chromium time to render and execute challenge JS.
                await asyncio.sleep(2.0)
                await self._cf_log_page_snapshot(
                    page, pw_ctx, "post_goto_snapshot", request=request,
                )

                # Attempt to click the Turnstile verification checkbox.
                clicked = await self._cf_click_turnstile(page)
                logger.info(
                    "[Cloudflare] Turnstile click result after navigation: %s",
                    clicked,
                )

                handoff = await self._cf_wait_for_handoff(
                    page,
                    pw_ctx,
                    request,
                    timeout_ms=self._cf_wait_timeout,
                )
                has_clearance = handoff["has_clearance"]
                if not has_clearance and not handoff["handoff_completed"]:
                    # NOTE: cf_clearance is HttpOnly — document.cookie cannot see it.
                    # We poll via Playwright's context.cookies() API instead.
                    has_clearance = await self._cf_poll_clearance(
                        pw_ctx, timeout_ms=self._cf_wait_timeout,
                    )
                if not has_clearance:
                    logger.warning("[Cloudflare] cf_clearance not found after first wait")

                    # Retry clicking — the widget may have loaded late.
                    clicked = await self._cf_click_turnstile(page)
                    if clicked:
                        handoff = await self._cf_wait_for_handoff(
                            page,
                            pw_ctx,
                            request,
                            timeout_ms=self._cf_wait_timeout,
                        )
                        has_clearance = handoff["has_clearance"]
                        if not has_clearance and not handoff["handoff_completed"]:
                            has_clearance = await self._cf_poll_clearance(
                                pw_ctx, timeout_ms=self._cf_wait_timeout,
                            )

            if not has_clearance:
                # Check if the page has navigated away from challenge anyway.
                body = await page.content()
                if self._body_has_challenge_markers(body):
                    logger.warning(
                        "[Cloudflare] Page still shows challenge markers after wait",
                    )
                    self._cf_dump_challenge_html(body, context_name, info)
                    return False
                logger.info(
                    "[Cloudflare] Page no longer shows challenge markers — "
                    "proceeding to validation despite missing cookie",
                )

            # Log cookies for diagnostics.
            await self._cf_log_context_cookies(pw_ctx, "post_solve_cookie_snapshot")
            await self._cf_log_page_snapshot(
                page, pw_ctx, "post_solve_snapshot", request=request,
            )

            # Validate clearance with a protected probe.
            return await self._cf_validate(pw_ctx, page, request, info)

        except Exception as exc:
            logger.error(
                "[Cloudflare] Solve navigation failed: %s", exc, exc_info=True,
            )
            return False
        finally:
            # Return page to pool — do NOT close it.  It carries validated
            # browser state needed by subsequent fetch requests.
            if page_is_new and not page.is_closed():
                pw_ctx.return_page(page)

    def _cf_determine_solve_url(self, request: Request) -> str:
        """Pick the URL to navigate for challenge solving.

        For fetch-mode requests the API endpoint itself won't render a
        challenge page, so we use the Referer, Origin, or domain root.
        For page-mode requests, use the request URL directly.
        """
        if self._cf_seed_url:
            return self._cf_seed_url

        if request.meta.get("playwright_fetch"):
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

    # ── Cloudflare clearance validation ─────────────────────────────────

    async def _cf_validate(
        self,
        pw_ctx: "PlaywrightContext",
        page: Page,
        request: Request,
        info: ContextChallengeInfo,
    ) -> bool:
        """Probe a protected first-party URL from the same context and
        confirm the response no longer carries challenge markers."""
        probe_url = self._cf_determine_probe_url(request)
        logger.info("[Cloudflare] Validation probe → %s", probe_url)

        try:
            await self._cf_log_page_snapshot(
                page, pw_ctx, "pre_validation_snapshot", request=request,
            )
            current_url = page.url
            current_host = urlparse(current_url).netloc if current_url else ""
            probe_host = urlparse(probe_url).netloc
            if current_host == probe_host:
                body = await page.content()
                if not self._body_has_challenge_markers(body):
                    logger.info(
                        "[Cloudflare] Validation succeeded on current page without probe "
                        "(url=%s title=%r)",
                        current_url,
                        await page.title(),
                    )
                    await self._cf_log_context_cookies(
                        pw_ctx, "validation_cookie_snapshot",
                    )
                    await self._cf_log_page_snapshot(
                        page, pw_ctx, "post_validation_snapshot", request=request,
                    )
                    info.last_validated_url = current_url
                    self._set_stat(
                        "playwright/cloudflare/last_validated_url", current_url,
                    )
                    return True
            probe_resp = await page.goto(
                probe_url,
                wait_until="domcontentloaded",
                timeout=30_000,
            )
            if probe_resp is None:
                logger.warning("[Cloudflare] Validation probe returned None")
                return False

            status = probe_resp.status
            headers = await probe_resp.all_headers()
            cf_mitigated = headers.get("cf-mitigated", "")
            logger.info(
                "[Cloudflare] Validation probe response: status=%d cf-mitigated=%r "
                "server=%r url=%s title=%r",
                status,
                cf_mitigated,
                headers.get("server"),
                page.url,
                await page.title(),
            )
            await self._cf_log_context_cookies(pw_ctx, "validation_cookie_snapshot")

            if status in (403, 429, 503) and cf_mitigated.lower() == "challenge":
                logger.warning(
                    "[Cloudflare] Validation probe still challenged (status=%d)",
                    status,
                )
                return False

            body = await page.content()
            if self._body_has_challenge_markers(body):
                logger.warning(
                    "[Cloudflare] Validation probe body has challenge markers",
                )
                self._cf_dump_challenge_html(body, pw_ctx.name, info)
                return False

            await self._cf_log_page_snapshot(
                page, pw_ctx, "post_validation_snapshot", request=request,
            )
            info.last_validated_url = probe_url
            self._set_stat(
                "playwright/cloudflare/last_validated_url", probe_url,
            )
            return True

        except Exception as exc:
            logger.error(
                "[Cloudflare] Validation probe failed: %s", exc, exc_info=True,
            )
            return False

    async def _cf_wait_for_handoff(
        self,
        page: Page,
        pw_ctx: "PlaywrightContext",
        request: Request,
        timeout_ms: int = 60_000,
        poll_interval: float = 1.0,
    ) -> dict:
        """Wait for Cloudflare's post-challenge handoff before forcing navigation.

        A successful manual solve performs a final form-style document POST back to
        the protected page. If we call ``page.goto`` too early we can replace that
        in-progress handoff with our own GET and immediately get re-challenged.
        """
        target_host = urlparse(self._cf_determine_probe_url(request)).netloc
        timeout_s = timeout_ms / 1000.0
        elapsed = 0.0
        poll_count = 0
        last_document = {"url": "", "status": None, "method": ""}
        handoff_completed = False

        async def _capture_response(response: PlaywrightResponse) -> None:
            try:
                req = response.request
                if req.resource_type != "document":
                    return
                resp_url = response.url
                resp_host = urlparse(resp_url).netloc
                if resp_host != target_host:
                    return
                last_document["url"] = resp_url
                last_document["status"] = response.status
                last_document["method"] = req.method
                logger.info(
                    "[Cloudflare] Handoff document response observed: method=%s "
                    "status=%s url=%s",
                    req.method,
                    response.status,
                    resp_url,
                )
            except Exception as exc:
                logger.debug(
                    "[Cloudflare] Failed to inspect handoff response: %s", exc,
                )

        page.on("response", _capture_response)
        try:
            while elapsed < timeout_s:
                poll_count += 1
                cookies = await pw_ctx.context.cookies()
                has_clearance = any(c["name"] == "cf_clearance" for c in cookies)
                current_url = page.url
                current_host = urlparse(current_url).netloc if current_url else ""
                body = None

                if current_host == target_host:
                    body = await page.content()
                    if not self._body_has_challenge_markers(body):
                        handoff_completed = True

                if not handoff_completed:
                    status = last_document["status"]
                    if (
                        last_document["url"]
                        and status is not None
                        and status < 400
                        and current_host == target_host
                        and body is not None
                        and not self._body_has_challenge_markers(body)
                    ):
                        handoff_completed = True

                if poll_count == 1 or poll_count % 3 == 0 or has_clearance or handoff_completed:
                    logger.info(
                        "[Cloudflare] Handoff poll #%d: current_url=%s clearance=%s "
                        "handoff_completed=%s last_document=%s %s %s",
                        poll_count,
                        current_url,
                        has_clearance,
                        handoff_completed,
                        last_document["method"] or "-",
                        last_document["status"] if last_document["status"] is not None else "-",
                        last_document["url"] or "-",
                    )

                if has_clearance or handoff_completed:
                    return {
                        "has_clearance": has_clearance,
                        "handoff_completed": handoff_completed,
                        "final_url": current_url,
                    }

                await asyncio.sleep(poll_interval)
                elapsed += poll_interval

            logger.warning(
                "[Cloudflare] Handoff wait timed out after %.1fs: current_url=%s "
                "last_document=%s %s %s",
                timeout_s,
                page.url,
                last_document["method"] or "-",
                last_document["status"] if last_document["status"] is not None else "-",
                last_document["url"] or "-",
            )
            return {
                "has_clearance": False,
                "handoff_completed": False,
                "final_url": page.url,
            }
        finally:
            with suppress(Exception):
                page.remove_listener("response", _capture_response)

    def _cf_determine_probe_url(self, request: Request) -> str:
        """Pick a first-party URL to probe for clearance validation."""
        if self._cf_seed_url:
            return self._cf_seed_url
        referer = request.headers.get("Referer")
        if referer:
            return referer.decode("utf-8") if isinstance(referer, bytes) else referer
        parsed = urlparse(request.url)
        return f"{parsed.scheme}://{parsed.netloc}/"

    # ── Cloudflare Turnstile interaction ──────────────────────────────

    @staticmethod
    async def _cf_poll_clearance(
        pw_ctx: "PlaywrightContext",
        timeout_ms: int = 60_000,
        poll_interval: float = 1.0,
    ) -> bool:
        """Poll the browser context's cookie jar for ``cf_clearance``.

        Unlike ``document.cookie``, the Playwright ``context.cookies()``
        API can see HttpOnly cookies — which ``cf_clearance`` typically is.
        """
        elapsed = 0.0
        timeout_s = timeout_ms / 1000.0
        poll_count = 0
        while elapsed < timeout_s:
            cookies = await pw_ctx.context.cookies()
            poll_count += 1
            if poll_count == 1 or poll_count % 5 == 0:
                cf_cookie_names = [
                    c["name"] for c in cookies if "cf" in c["name"].lower()
                ]
                logger.debug(
                    "[Cloudflare] Clearance poll #%d (context='%s'): cf cookies=%s",
                    poll_count, pw_ctx.name, cf_cookie_names,
                )
            if any(c["name"] == "cf_clearance" for c in cookies):
                return True
            await asyncio.sleep(poll_interval)
            elapsed += poll_interval
        return False

    async def _cf_click_turnstile(self, page: Page) -> bool:
        """Locate the Turnstile iframe and click the verification checkbox.

        Cloudflare managed challenges embed a Turnstile iframe inside a
        **closed Shadow DOM**, so standard DOM selectors like
        ``page.wait_for_selector('iframe[src*=...]')`` will never find it.

        Instead we poll ``page.frames`` — Playwright tracks frames at the
        browser/CDP level regardless of Shadow DOM encapsulation.

        Returns *True* if the checkbox was found and clicked.
        """
        _CF_FRAME_MARKER = "challenges.cloudflare.com/cdn-cgi/challenge-platform"

        def _find_turnstile_frame():
            for frame in page.frames:
                if _CF_FRAME_MARKER in (frame.url or ""):
                    return frame
            return None

        try:
            page_url = page.url
            page_title = await page.title()
            logger.debug(
                "[Cloudflare][TurnstileClick] Page URL=%s  title=%r  frames=%d",
                page_url, page_title, len(page.frames),
            )

            # Poll page.frames for the Turnstile frame (up to 15 s).
            cf_frame = None
            for _ in range(30):  # 30 × 0.5 s = 15 s
                cf_frame = _find_turnstile_frame()
                if cf_frame is not None:
                    break
                await asyncio.sleep(0.5)

            if cf_frame is None:
                logger.debug(
                    "[Cloudflare][TurnstileClick] No Turnstile frame found after polling. "
                    "Frames: %s",
                    [f.url for f in page.frames],
                )
                return False

            logger.debug(
                "[Cloudflare][TurnstileClick] Found Turnstile frame: %s",
                cf_frame.url,
            )

            # Give the widget JS time to render the checkbox.
            await asyncio.sleep(random.uniform(1.5, 3.0))

            async def _click_point(target_x: float, target_y: float, attempt: int) -> bool:
                logger.debug(
                    "[Cloudflare][TurnstileClick] mouse.click (%.1f, %.1f) attempt=%d backend=%s",
                    target_x, target_y, attempt, self._browser_type_name,
                )
                await page.mouse.click(target_x, target_y)
                return True

            for attempt in range(1, 5):
                cf_frame = _find_turnstile_frame()
                if cf_frame is None:
                    logger.debug(
                        "[Cloudflare][TurnstileClick] Turnstile frame disappeared before click attempt=%d",
                        attempt,
                    )
                    await asyncio.sleep(0.5)
                    continue

                # Primary path: click the iframe element itself in top-page coordinates.
                try:
                    frame_element = await cf_frame.frame_element()
                    frame_box = await frame_element.bounding_box()
                    logger.debug(
                        "[Cloudflare][TurnstileClick] Frame element bounding_box=%s attempt=%d",
                        frame_box, attempt,
                    )
                    if frame_box and frame_box["width"] > 0 and frame_box["height"] > 0:
                        target_x = frame_box["x"] + min(
                            max(30.0, frame_box["width"] * 0.28),
                            frame_box["width"] - 12.0,
                        )
                        target_y = frame_box["y"] + (frame_box["height"] / 2) + random.uniform(-4, 4)
                        logger.debug(
                            "[Cloudflare][TurnstileClick] iframe box=(%s,%s %sx%s) target=(%.1f, %.1f) attempt=%d",
                            frame_box["x"], frame_box["y"], frame_box["width"], frame_box["height"],
                            target_x, target_y, attempt,
                        )
                        await _click_point(target_x, target_y, attempt)
                        logger.info(
                            "[Cloudflare] Clicked Turnstile iframe at (%.1f, %.1f) attempt=%d",
                            target_x, target_y, attempt,
                        )
                        self._inc_stat("playwright/cloudflare/turnstile_click_count")
                        await asyncio.sleep(3.0)
                        logger.info(
                            "[Cloudflare] Post-click page state: url=%s title=%r frames=%d",
                            page.url, await page.title(), len(page.frames),
                        )
                        return True
                    logger.debug(
                        "[Cloudflare][TurnstileClick] Frame element had invalid dimensions attempt=%d",
                        attempt,
                    )
                except Exception as frame_exc:
                    logger.debug(
                        "[Cloudflare][TurnstileClick] Frame element click failed on attempt=%d: %s",
                        attempt, frame_exc,
                    )

                # Secondary path: click likely controls inside the frame if Playwright can reach them.
                try:
                    for selector in (
                        "input[type='checkbox']",
                        "[role='checkbox']",
                        "label.ctp-checkbox-label",
                        "label[for]",
                        "button",
                    ):
                        locator = cf_frame.locator(selector)
                        if await locator.count() > 0:
                            await locator.first.click(timeout=5_000, force=True)
                            logger.info(
                                "[Cloudflare] Clicked Turnstile selector %r inside frame attempt=%d",
                                selector, attempt,
                            )
                            self._inc_stat("playwright/cloudflare/turnstile_click_count")
                            await asyncio.sleep(3.0)
                            return True
                except Exception as inner_exc:
                    logger.debug(
                        "[Cloudflare][TurnstileClick] Inner-frame fallback failed on attempt=%d: %s",
                        attempt, inner_exc,
                    )

                await asyncio.sleep(0.75)

            logger.debug("[Cloudflare][TurnstileClick] Frame found but click target missed")
            return False

        except Exception as exc:
            logger.debug(
                "[Cloudflare][TurnstileClick] Turnstile click failed: %s", exc,
                exc_info=True,
            )
            return False

    # ── Cloudflare diagnostics ─────────────────────────────────────────

    def _cf_log_turnstile_diagnostics(self, response: Response) -> None:
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
        self._set_stat(
            "playwright/cloudflare/turnstile_sitekey",
            sitekey_match.group(1) if sitekey_match else None,
        )

    def _cf_log_response_summary(
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
            self._body_has_challenge_markers(body_text),
            snippet,
        )

    def _cf_log_context_pages(self, pw_ctx: "PlaywrightContext", label: str) -> None:
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

    async def _cf_log_context_cookies(
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

    async def _cf_log_page_snapshot(
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

    def _cf_dump_challenge_html(self, body: str, context_name: str, info: ContextChallengeInfo) -> None:
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

    @staticmethod
    def _cf_should_log_network_event(
        url: str,
        method: str,
        resource_type: str,
        is_navigation_request: bool,
    ) -> bool:
        host = (urlparse(url).netloc or "").lower()
        is_cf = "cloudflare.com" in host
        is_target = "imovelweb.com.br" in host
        if not (is_cf or is_target):
            return False
        if "/cdn-cgi/challenge-platform/" in url:
            return True
        if is_navigation_request and resource_type == "document":
            return True
        return method != "GET"

    async def _cf_log_request_event(self, request: PlaywrightRequest) -> None:
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
                    self._sanitize_for_log(headers),
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

    async def _cf_log_response_event(self, response: PlaywrightResponse) -> None:
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
                    self._sanitize_for_log(headers),
                    ensure_ascii=False,
                    sort_keys=True,
                ),
            )
            if private_token_challenge is not None:
                self._inc_stat("playwright/cloudflare/private_token_challenge_count")
                self._set_stat(
                    "playwright/cloudflare/last_private_token_challenge_url",
                    private_token_challenge["url"],
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

    # ── Lifecycle ──────────────────────────────────────────────────────

    async def launch(self, startup_context_kwargs: Optional[dict] = None) -> None:
        """Start the Playwright driver and optionally create startup contexts."""
        logger.info("Starting Playwright facade")
        if self._uses_camoufox_backend:
            logger.info(
                "[Lifecycle] Using Camoufox backend with options=%s",
                _json.dumps(
                    self._sanitize_for_log(self._prepare_camoufox_launch_options()),
                    ensure_ascii=False,
                    sort_keys=True,
                ),
            )
            self.playwright_context_manager = None
            self.playwright = None
            self.browser_type = None
        else:
            self.playwright_context_manager = PlaywrightContextManager()
            self.playwright = await self.playwright_context_manager.start()
            self.browser_type = getattr(self.playwright, self._browser_type_name)

        if startup_context_kwargs:
            logger.info("Launching %i startup context(s)", len(startup_context_kwargs))
            await asyncio.gather(*[
                self.create_context(name=name, context_kwargs=kwargs)
                for name, kwargs in startup_context_kwargs.items()
            ])
            logger.info("Startup context(s) launched")

    async def restart(self) -> None:
        """Restart the Playwright driver after it has died."""
        async with self._restart_lock:
            if not self._driver_dead:
                return
            logger.info("[Lifecycle] restart ENTER")
            self.contexts.clear()
            self._browser = None
            if self._uses_camoufox_backend:
                await self._close_camoufox_launchers()
                self.playwright_context_manager = None
                self.playwright = None
                self.browser_type = None
            else:
                if self.playwright_context_manager:
                    with suppress(Exception):
                        await self.playwright_context_manager.__aexit__()
                if self.playwright:
                    with suppress(Exception):
                        await self.playwright.stop()
                self.playwright_context_manager = PlaywrightContextManager()
                self.playwright = await self.playwright_context_manager.start()
                self.browser_type = getattr(self.playwright, self._browser_type_name)
            self._driver_dead = False
            self._inc_stat("playwright/driver_restart_count")
            logger.info("[Lifecycle] restart DONE")

    async def close(self) -> None:
        """Tear down all contexts, browser and driver."""
        logger.info("[Lifecycle] Playwright.close() ENTER")
        self._is_closing = True
        for ctx in self.contexts.values():
            if ctx.recorder:
                ctx.recorder.finalize()
                self._set_stat("playwright/har_entry_count", ctx.recorder.entry_count)
        for ctx in self.contexts.values():
            await ctx.drain_pool()
        with suppress(TargetClosedError):
            await asyncio.gather(*[ctx.context.close() for ctx in self.contexts.values()])
        self.contexts.clear()
        if self._uses_camoufox_backend:
            await self._close_camoufox_launchers()
        else:
            if self._browser is not None:
                await self._browser.close()
            if self.playwright_context_manager:
                with suppress(Exception):
                    await self.playwright_context_manager.__aexit__()
            if self.playwright:
                with suppress(Exception):
                    await self.playwright.stop()
        logger.info("[Lifecycle] Playwright.close() DONE")

    # ── Browser ────────────────────────────────────────────────────────

    async def _ensure_browser(self) -> None:
        async with self._browser_launch_lock:
            if self._driver_dead:
                raise RuntimeError("Connection closed while reading from the driver")
            if self._browser is not None and not self._browser.is_connected():
                logger.warning("[Lifecycle] browser disconnected, clearing")
                self._browser = None
            if self._browser is None:
                if self._cdp_url:
                    logger.info("Connecting using CDP: %s", self._cdp_url)
                    self._browser = await self.browser_type.connect_over_cdp(
                        self._cdp_url, **self._cdp_kwargs,
                    )
                elif self._connect_url:
                    logger.info("Connecting to remote Playwright")
                    self._browser = await self.browser_type.connect(
                        self._connect_url, **self._connect_kwargs,
                    )
                else:
                    logger.info(
                        "Launching browser %s with options=%s",
                        self.browser_type.name,
                        _json.dumps(
                            self._sanitize_for_log(self._launch_options),
                            ensure_ascii=False,
                            sort_keys=True,
                        ),
                    )
                    self._browser = await self.browser_type.launch(**self._launch_options)
                self._inc_stat("playwright/browser_count")
                browser_version = getattr(self._browser, "version", None)
                if callable(browser_version):
                    with suppress(Exception):
                        browser_version = browser_version()
                logger.info(
                    "[Lifecycle] Browser connected name=%s version=%s connected=%s",
                    self.browser_type.name,
                    browser_version,
                    self._browser.is_connected(),
                )
                self._browser.on("disconnected", self._on_browser_disconnected)

    async def _on_browser_disconnected(self) -> None:
        logger.info("[Lifecycle] browser disconnected")
        self._driver_dead = True
        for ctx in self.contexts.values():
            if ctx.recorder:
                ctx.recorder.finalize()
        with suppress(TargetClosedError):
            await asyncio.gather(*[ctx.context.close() for ctx in self.contexts.values()])
        self.contexts.clear()
        if self._restart_disconnected_browser:
            self._browser = None

    # ── Context management ─────────────────────────────────────────────

    def _create_network_recorder(
        self, name: str, context_kwargs: dict,
    ) -> Optional[NetworkRecorder]:
        har_meta = context_kwargs.pop("_playwright_har", None)
        if har_meta is False:
            return None
        if not (har_meta is not None or self._har_recording):
            return None
        url_filter = self._har_url_filter
        if isinstance(har_meta, dict) and har_meta.get("url_filter"):
            url_filter = har_meta["url_filter"]
        return NetworkRecorder(
            output_dir=self._har_output_dir,
            context_name=name,
            url_filter=url_filter,
        )

    async def create_context(
        self,
        name: str,
        context_kwargs: Optional[dict] = None,
        spider: Optional[Spider] = None,
    ) -> PlaywrightContext:
        """Create, register and return a new :class:`PlaywrightContext`."""
        logger.debug("[Lifecycle] create_context ENTER name='%s'", name)
        if self._context_semaphore is not None:
            await self._context_semaphore.acquire()
        context_kwargs = context_kwargs or {}
        recorder = self._create_network_recorder(name, context_kwargs)
        if recorder:
            logger.info("Network recording enabled for context '%s'", name)
            self._inc_stat("playwright/har_recording_contexts")
        try:
            persistent = False
            if self._uses_camoufox_backend:
                camoufox_options = self._prepare_camoufox_launch_options(context_kwargs)
                persistent = bool(
                    camoufox_options.get("persistent_context")
                    or camoufox_options.get(PERSISTENT_CONTEXT_PATH_KEY)
                )
                logger.info(
                    "[Lifecycle] Launching Camoufox context '%s' with options=%s",
                    name,
                    _json.dumps(
                        self._sanitize_for_log(camoufox_options),
                        ensure_ascii=False,
                        sort_keys=True,
                    ),
                )
                AsyncCamoufox = self._load_camoufox()
                launcher = AsyncCamoufox(**camoufox_options)
                launched = await launcher.__aenter__()
                self._camoufox_launchers[name] = launcher
                if persistent:
                    context = launched
                else:
                    context = await launched.new_context(**context_kwargs)
            elif context_kwargs.get(PERSISTENT_CONTEXT_PATH_KEY):
                persistent_launch_kwargs = {**self._launch_options, **context_kwargs}
                logger.info(
                    "[Lifecycle] Launching persistent context '%s' with options=%s",
                    name,
                    _json.dumps(
                        self._sanitize_for_log(persistent_launch_kwargs),
                        ensure_ascii=False,
                        sort_keys=True,
                    ),
                )
                context = await self.browser_type.launch_persistent_context(
                    **persistent_launch_kwargs,
                )
                persistent = True
            else:
                await self._ensure_browser()
                logger.info(
                    "[Lifecycle] Creating browser context '%s' with options=%s",
                    name,
                    _json.dumps(
                        self._sanitize_for_log(context_kwargs),
                        ensure_ascii=False,
                        sort_keys=True,
                    ),
                )
                context = await self._browser.new_context(**context_kwargs)

            context.on("close", self._make_close_context_cb(name, persistent, recorder))
            self._inc_stat("playwright/context_count")

            if self._navigation_timeout is not None:
                context.set_default_navigation_timeout(self._navigation_timeout)

            pw_ctx = PlaywrightContext(
                name=name,
                context=context,
                max_pages=self._max_pages_per_context,
                persistent=persistent,
                pool_strategy=self._pool_strategy,
                pool_enabled=self._pool_enabled,
                init_scripts=self._stealth_init_scripts,
                recorder=recorder,
                navigation_timeout=self._navigation_timeout,
            )
            self.contexts[name] = pw_ctx
            for page in list(context.pages):
                self._register_page(page, pw_ctx, spider=spider)
                adopted = await pw_ctx.pool.adopt(page)
                if adopted:
                    self._inc_stat("playwright/page_count")
                    logger.info(
                        "[Lifecycle] Adopted existing page in context '%s': url=%s",
                        name,
                        page.url,
                    )
            logger.debug(
                "Browser context started: '%s' (persistent=%s backend=%s)",
                name, persistent, self._browser_type_name,
            )
            return pw_ctx
        except Exception:
            if self._uses_camoufox_backend:
                launcher = self._camoufox_launchers.pop(name, None)
                if launcher is not None:
                    with suppress(Exception):
                        await launcher.__aexit__(None, None, None)
            if self._context_semaphore is not None:
                self._context_semaphore.release()
            raise

    async def get_or_create_context(
        self,
        request: Request,
        spider: Spider,
    ) -> PlaywrightContext:
        """Return existing context for *request* or create one on the fly."""
        context_name = request.meta.setdefault("playwright_context", DEFAULT_CONTEXT_NAME)
        async with self._context_launch_lock:
            pw_ctx = self.contexts.get(context_name)
            if pw_ctx is None:
                context_kwargs = dict(request.meta.get("playwright_context_kwargs") or {})
                har_meta = request.meta.get("playwright_har")
                if har_meta is not None:
                    context_kwargs["_playwright_har"] = har_meta
                pw_ctx = await self.create_context(
                    name=context_name, context_kwargs=context_kwargs, spider=spider,
                )
        return pw_ctx

    def get_context(self, name: str) -> Optional[PlaywrightContext]:
        return self.contexts.get(name)

    # ── Page lifecycle helpers ─────────────────────────────────────────

    async def create_page(
        self, request: Request, spider: Spider,
    ) -> Tuple[Page, PlaywrightContext, bool]:
        """Acquire a page for *request*, creating context if needed.

        Returns ``(page, pw_context, is_new_page)``.
        """
        pw_ctx = await self.get_or_create_context(request, spider)
        page, is_new = await pw_ctx.acquire_page()

        if is_new:
            self._inc_stat("playwright/page_count")
            self._register_page(page, pw_ctx, spider=spider)

        return page, pw_ctx, is_new

    def should_close_page(self, request: Request) -> bool:
        return self._close_page_after_request and not request.meta.get(
            "playwright_include_page",
        )

    async def return_or_close_page(self, request: Request, page: Page) -> None:
        context_name = request.meta.get("playwright_context", DEFAULT_CONTEXT_NAME)
        pw_ctx = self.contexts.get(context_name)
        if pw_ctx is not None and pw_ctx.pool is not None and not page.is_closed():
            pw_ctx.return_page(page)
            self._inc_stat("playwright/page_count/returned_to_pool")
        else:
            if not page.is_closed():
                await page.close()
            self._inc_stat("playwright/page_count/closed")

    # ── Fetch-style download ──────────────────────────────────────────

    async def get_or_create_fetch_page(
        self, request: Request, spider: Spider,
    ) -> Page:
        """Return an open page for ``fetch()`` calls, creating one if needed."""
        context_name = (
            request.meta.get("playwright_fetch_context")
            or request.meta.get("playwright_context")
            or DEFAULT_CONTEXT_NAME
        )
        request.meta["playwright_context"] = context_name

        pw_ctx = await self.get_or_create_context(request, spider)

        existing = await pw_ctx.get_open_page()
        if existing is not None:
            return existing

        # Determine seed URL
        seed_url = request.meta.get("playwright_fetch_seed_url")
        if not seed_url:
            referer = request.headers.get("Referer")
            if referer:
                seed_url = referer.decode("utf-8") if isinstance(referer, bytes) else referer
        if not seed_url:
            origin = request.headers.get("Origin")
            if origin:
                seed_url = origin.decode("utf-8") if isinstance(origin, bytes) else origin
        if not seed_url:
            parsed = urlparse(request.url)
            seed_url = f"{parsed.scheme}://{parsed.netloc}/"

        logger.info(
            "[Fetch] No open page in context '%s', seeding with %s",
            context_name, seed_url,
        )
        page, is_new = await pw_ctx.acquire_page()
        if is_new:
            self._inc_stat("playwright/page_count")
            self._register_page(page, pw_ctx, spider=spider)
        await page.goto(seed_url, wait_until="domcontentloaded")
        return page

    def _register_page(
        self,
        page: Page,
        pw_ctx: PlaywrightContext,
        spider: Optional[Spider] = None,
    ) -> None:
        """Attach lifecycle and network handlers to a newly discovered page."""
        page.on("close", self._make_close_page_cb(pw_ctx.name))
        page.on("crash", self._make_close_page_cb(pw_ctx.name))
        page.on("request", self._on_playwright_request)
        page.on("response", self._on_playwright_response)
        if pw_ctx.recorder:
            page.on("response", pw_ctx.recorder.on_response)
        if logger.getEffectiveLevel() <= logging.DEBUG:
            page.on("request", _make_request_logger(pw_ctx.name, spider))
            page.on("response", _make_response_logger(pw_ctx.name, spider))

    @staticmethod
    def build_fetch_js(
        url: str, method: str, headers: dict, body: Optional[str],
    ) -> str:
        fetch_opts: dict = {
            "method": method,
            "headers": headers,
            "credentials": "include",
        }
        if body is not None:
            fetch_opts["body"] = body
        opts_json = _json.dumps(fetch_opts, ensure_ascii=False)
        return (
            f"async () => {{"
            f"  const r = await fetch({_json.dumps(url)}, {opts_json});"
            f"  const hdrs = {{}};"
            f"  r.headers.forEach((v, k) => {{ hdrs[k] = v; }});"
            f"  const text = await r.text();"
            f"  return {{status: r.status, statusText: r.statusText, headers: hdrs, body: text}};"
            f"}}"
        )

    @staticmethod
    def _sanitize_fetch_headers(headers: dict) -> dict:
        """Drop headers that must come from the live browser request context.

        Browser-executed ``fetch()`` already derives cookies, UA, client hints,
        fetch metadata, referer, host, and transfer details from the active page.
        Replaying Scrapy-side values here can create mismatched identities such as:
        - duplicate Cookie headers when ``credentials: include`` is set
        - Chromium UA on a Firefox/Camoufox browser session
        - stale Sec-Fetch / Content-Length / Host values
        """
        browser_owned = {
            "accept-encoding",
            "connection",
            "content-length",
            "cookie",
            "host",
            "origin",
            "priority",
            "referer",
            "sec-ch-ua",
            "sec-ch-ua-arch",
            "sec-ch-ua-bitness",
            "sec-ch-ua-full-version",
            "sec-ch-ua-full-version-list",
            "sec-ch-ua-mobile",
            "sec-ch-ua-model",
            "sec-ch-ua-platform",
            "sec-ch-ua-platform-version",
            "sec-fetch-dest",
            "sec-fetch-mode",
            "sec-fetch-site",
            "sec-fetch-user",
            "upgrade-insecure-requests",
            "user-agent",
        }
        sanitized = {}
        dropped = []
        for name, value in headers.items():
            if name.lower() in browser_owned:
                dropped.append(name)
                continue
            sanitized[name] = value
        if dropped:
            logger.info(
                "[Fetch] Dropping browser-managed headers before fetch(): %s",
                ", ".join(sorted(dropped, key=str.lower)),
            )
        return sanitized

    async def download_with_fetch(
        self, request: Request, spider: Spider,
    ) -> Response:
        """Execute *request* as a ``fetch()`` call inside the browser."""
        start_time = time()
        page = await self.get_or_create_fetch_page(request, spider)

        hdrs: dict = {}
        for key, values in request.headers.items():
            name = key.decode("utf-8") if isinstance(key, bytes) else key
            val = values[-1] if isinstance(values, list) else values
            hdrs[name] = val.decode("utf-8") if isinstance(val, bytes) else val
        hdrs = self._sanitize_fetch_headers(hdrs)

        body_str: Optional[str] = None
        if request.body:
            body_str = request.body.decode(request.encoding)

        js_code = self.build_fetch_js(
            url=request.url, method=request.method, headers=hdrs, body=body_str,
        )
        logger.info("[Fetch] %s %s", request.method, request.url)
        result = await page.evaluate(js_code)

        status = result["status"]
        resp_headers = Headers(result.get("headers") or {})
        resp_headers.pop("Content-Encoding", None)
        resp_body_text = result.get("body", "")
        body_bytes, encoding = _encode_body(headers=resp_headers, text=resp_body_text)
        request.meta["download_latency"] = time() - start_time

        respcls = responsetypes.from_args(headers=resp_headers, url=request.url, body=body_bytes)
        self._inc_stat("playwright/fetch_count")
        return respcls(
            url=request.url,
            status=status,
            headers=resp_headers,
            body=body_bytes,
            request=request,
            flags=["playwright", "playwright_fetch"],
            encoding=encoding,
        )

    # ── Navigation-style download ─────────────────────────────────────

    async def download_with_page(
        self, request: Request, spider: Spider,
    ) -> Response:
        """Full page-navigation download."""
        page = request.meta.get("playwright_page")
        if not isinstance(page, Page) or page.is_closed():
            page, pw_ctx, _ = await self.create_page(request=request, spider=spider)
        context_name = request.meta.setdefault("playwright_context", DEFAULT_CONTEXT_NAME)

        _attach_page_event_handlers(
            page=page, request=request, spider=spider, context_name=context_name,
        )
        initial_request_done = asyncio.Event()
        await page.unroute("**")
        await page.route(
            "**",
            self._make_route_handler(
                context_name=context_name,
                method=request.method,
                url=request.url,
                headers=request.headers,
                body=request.body,
                encoding=request.encoding,
                spider=spider,
                initial_request_done=initial_request_done,
            ),
        )
        await _maybe_execute_page_init_callback(
            page=page, request=request, context_name=context_name, spider=spider,
        )
        try:
            return await self._navigate_and_build_response(request, page, spider)
        except Exception:
            if self.should_close_page(request) and not page.is_closed():
                await self.return_or_close_page(request, page)
            raise

    async def _navigate_and_build_response(
        self, request: Request, page: Page, spider: Spider,
    ) -> Response:
        if request.meta.get("playwright_include_page"):
            request.meta["playwright_page"] = page

        start_time = time()
        response, download = await self._goto_and_handle_download(request, page, spider)

        if isinstance(response, PlaywrightResponse):
            await _set_redirect_meta(request=request, response=response)
            headers = Headers(await response.all_headers())
            headers.pop("Content-Encoding", None)
        elif not download:
            logger.warning("Navigating to %s returned None", request)
            headers = Headers()

        await self._apply_page_methods(page, request, spider)
        body_str = await _get_page_content(
            page=page,
            spider=spider,
            context_name=request.meta.get("playwright_context"),
            scrapy_request_url=request.url,
            scrapy_request_method=request.method,
        )
        request.meta["download_latency"] = time() - start_time

        server_ip_address = None
        if response is not None:
            request.meta["playwright_security_details"] = await response.security_details()
            with suppress(KeyError, TypeError, ValueError):
                server_addr = await response.server_addr()
                server_ip_address = ip_address(server_addr["ipAddress"])

        if download and download.exception:
            raise download.exception

        if self.should_close_page(request):
            await self.return_or_close_page(request, page)

        if download:
            request.meta["playwright_suggested_filename"] = download.suggested_filename
            respcls = responsetypes.from_args(url=download.url, body=download.body)
            dl_headers = Headers(download.headers)
            dl_headers.pop("Content-Encoding", None)
            return respcls(
                url=download.url,
                status=download.response_status,
                headers=dl_headers,
                body=download.body,
                request=request,
                flags=["playwright"],
            )

        body, encoding = _encode_body(headers=headers, text=body_str)
        respcls = responsetypes.from_args(headers=headers, url=page.url, body=body)
        return respcls(
            url=page.url,
            status=response.status if response is not None else 200,
            headers=headers,
            body=body,
            request=request,
            flags=["playwright"],
            encoding=encoding,
            ip_address=server_ip_address,
        )

    async def _goto_and_handle_download(
        self, request: Request, page: Page, spider: Spider,
    ) -> Tuple[Optional[PlaywrightResponse], Optional[Download]]:
        response: Optional[PlaywrightResponse] = None
        download = Download()
        download_started = asyncio.Event()
        download_ready = asyncio.Event()

        async def _handle_download(dwnld: PlaywrightDownload) -> None:
            download_started.set()
            self._inc_stat("playwright/download_count")
            try:
                if failure := await dwnld.failure():
                    raise RuntimeError(f"Failed to download {dwnld.url}: {failure}")
                download.body = (await dwnld.path()).read_bytes()
                download.url = dwnld.url
                download.suggested_filename = dwnld.suggested_filename
            except Exception as ex:
                download.exception = ex
            finally:
                download_ready.set()

        async def _handle_response(resp: PlaywrightResponse) -> None:
            download.response_status = resp.status
            download.headers = await resp.all_headers()
            download_started.set()

        page_goto_kwargs = request.meta.get("playwright_page_goto_kwargs") or {}
        page_goto_kwargs.pop("url", None)
        page.on("download", _handle_download)
        page.on("response", _handle_response)
        try:
            response = await page.goto(url=request.url, **page_goto_kwargs)
        except PlaywrightError as err:
            if not (
                "Download is starting" in err.message
                or self._browser_type_name == "chromium"
                and "net::ERR_ABORTED" in err.message
            ):
                raise
            await download_started.wait()
            if download.response_status == 204:
                raise err
            await download_ready.wait()
        finally:
            page.remove_listener("download", _handle_download)
            page.remove_listener("response", _handle_response)

        return response, download if download else None

    async def _apply_page_methods(
        self, page: Page, request: Request, spider: Spider,
    ) -> None:
        page_methods = request.meta.get("playwright_page_methods") or ()
        if isinstance(page_methods, dict):
            page_methods = page_methods.values()
        for pm in page_methods:
            if isinstance(pm, PageMethod):
                try:
                    if callable(pm.method):
                        method = partial(pm.method, page)
                    else:
                        method = getattr(page, pm.method)
                except AttributeError:
                    logger.warning("Ignoring %r: could not find method", pm, exc_info=True)
                else:
                    pm.result = await _maybe_await(method(*pm.args, **pm.kwargs))
                    await page.wait_for_load_state(timeout=self._navigation_timeout)
            else:
                logger.warning("Ignoring %r: expected PageMethod, got %r", pm, type(pm))

    # ── Route handler (request interception) ───────────────────────────

    def _make_route_handler(
        self,
        context_name: str,
        method: str,
        url: str,
        headers: Headers,
        body: Optional[bytes],
        encoding: str,
        spider: Spider,
        initial_request_done: asyncio.Event,
    ) -> Callable:
        async def _handler(route: Route, pw_request: PlaywrightRequest) -> None:
            if self.abort_request:
                should_abort = await _maybe_await(self.abort_request(pw_request))
                if should_abort:
                    await route.abort()
                    self._inc_stat("playwright/request_count/aborted")
                    return

            overrides: dict = {}
            if self.process_request_headers is None:
                final_headers = await pw_request.all_headers()
            else:
                overrides["headers"] = final_headers = await _maybe_await(
                    self.process_request_headers(
                        browser_type_name=self._browser_type_name,
                        playwright_request=pw_request,
                        scrapy_request_data={
                            "method": method,
                            "url": url,
                            "headers": headers,
                            "body": body,
                            "encoding": encoding,
                        },
                    )
                )

            if (
                pw_request.url.rstrip("/") == url.rstrip("/")
                and pw_request.is_navigation_request()
                and not initial_request_done.is_set()
            ):
                initial_request_done.set()
                if method.upper() != pw_request.method.upper():
                    overrides["method"] = method
                if body:
                    overrides["post_data"] = body.decode(encoding)
                headers.clear()
                headers.update(final_headers)

            del final_headers
            try:
                await route.continue_(**overrides)
            except PlaywrightError as ex:
                if not _is_safe_close_error(ex):
                    raise

        return _handler

    # ── Internal Playwright event callbacks ────────────────────────────

    def _on_playwright_request(self, request: PlaywrightRequest) -> None:
        self._inc_stat("playwright/request_count")
        self._inc_stat(f"playwright/request_count/resource_type/{request.resource_type}")
        self._inc_stat(f"playwright/request_count/method/{request.method}")
        if request.is_navigation_request():
            self._inc_stat("playwright/request_count/navigation")
        if self._cf_enabled and self._cf_should_log_network_event(
            request.url,
            request.method,
            request.resource_type,
            request.is_navigation_request(),
        ):
            asyncio.create_task(self._cf_log_request_event(request))

    def _on_playwright_response(self, response: PlaywrightResponse) -> None:
        self._inc_stat("playwright/response_count")
        self._inc_stat(f"playwright/response_count/resource_type/{response.request.resource_type}")
        self._inc_stat(f"playwright/response_count/method/{response.request.method}")
        if self._cf_enabled and self._cf_should_log_network_event(
            response.url,
            response.request.method,
            response.request.resource_type,
            response.request.is_navigation_request(),
        ):
            asyncio.create_task(self._cf_log_response_event(response))

    def _make_close_page_cb(self, context_name: str) -> Callable:
        def cb() -> None:
            pw_ctx = self.contexts.get(context_name)
            if pw_ctx is not None:
                pw_ctx.pool._semaphore.release()
        return cb

    def _make_close_context_cb(
        self,
        name: str,
        persistent: bool,
        recorder: Optional[NetworkRecorder],
    ) -> Callable:
        def cb() -> None:
            self.contexts.pop(name, None)
            if self._uses_camoufox_backend:
                launcher = self._camoufox_launchers.pop(name, None)
                if launcher is not None:
                    asyncio.create_task(self._close_camoufox_launcher(name, launcher))
            if self._context_semaphore is not None:
                self._context_semaphore.release()
            if recorder:
                recorder.finalize()
                self._set_stat("playwright/har_entry_count", recorder.entry_count)
            logger.debug("Browser context closed: '%s' (persistent=%s)", name, persistent)
        return cb

    # ── Helpers ────────────────────────────────────────────────────────

    def get_total_page_count(self) -> int:
        return sum(len(ctx.context.pages) for ctx in self.contexts.values())


# ── Free functions (page event helpers) ────────────────────────────────

def _attach_page_event_handlers(
    page: Page, request: Request, spider: Spider, context_name: str,
) -> None:
    event_handlers = request.meta.get("playwright_page_event_handlers") or {}
    for event, handler in event_handlers.items():
        if callable(handler):
            page.on(event, handler)
        elif isinstance(handler, str):
            try:
                page.on(event, getattr(spider, handler))
            except AttributeError:
                logger.warning(
                    "Spider '%s' does not have a '%s' attribute,"
                    " ignoring handler for event '%s'",
                    spider.name, handler, event, exc_info=True,
                )


async def _set_redirect_meta(request: Request, response: PlaywrightResponse) -> None:
    redirect_times: int = 0
    redirect_urls: list = []
    redirect_reasons: list = []
    redirected = response.request.redirected_from
    while redirected is not None:
        redirect_times += 1
        redirect_urls.append(redirected.url)
        redirected_response = await redirected.response()
        reason = None if redirected_response is None else redirected_response.status
        redirect_reasons.append(reason)
        redirected = redirected.redirected_from
    if redirect_times:
        request.meta["redirect_times"] = redirect_times
        request.meta["redirect_urls"] = list(reversed(redirect_urls))
        request.meta["redirect_reasons"] = list(reversed(redirect_reasons))


async def _maybe_execute_page_init_callback(
    page: Page, request: Request, context_name: str, spider: Spider,
) -> None:
    page_init_callback = request.meta.get("playwright_page_init_callback")
    if page_init_callback:
        try:
            page_init_callback = load_object(page_init_callback)
            await page_init_callback(page, request)
        except Exception:
            logger.warning(
                "[Context=%s] Page init callback exception for %s",
                context_name, repr(request), exc_info=True,
            )


def _make_request_logger(context_name: str, spider: Spider) -> Callable:
    async def _log_request(request: PlaywrightRequest) -> None:
        log_args = [context_name, request.method.upper(), request.url, request.resource_type]
        referrer = await _get_header_value(request, "referer")
        if referrer:
            log_args.append(referrer)
            log_msg = "[Context=%s] Request: <%s %s> (resource type: %s, referrer: %s)"
        else:
            log_msg = "[Context=%s] Request: <%s %s> (resource type: %s)"
        logger.debug(log_msg, *log_args)
    return _log_request


def _make_response_logger(context_name: str, spider: Spider) -> Callable:
    async def _log_response(response: PlaywrightResponse) -> None:
        log_args = [context_name, response.status, response.url]
        location = await _get_header_value(response, "location")
        if location:
            log_args.append(location)
            log_msg = "[Context=%s] Response: <%i %s> (location: %s)"
        else:
            log_msg = "[Context=%s] Response: <%i %s>"
        logger.debug(log_msg, *log_args)
    return _log_response
