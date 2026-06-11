"""Playwright engine — manages browser contexts, page pools, and request
execution (both navigation and fetch modes).

``handler.py`` instantiates a :class:`PlaywrightEngine` engine and delegates all
browser work to it.  Everything Scrapy-specific (Twisted deferreds,
``download_request`` branching, ``Config.from_settings``) stays in the handler.
"""

import asyncio
import json as _json
import logging
import pathlib
import re
from contextlib import suppress
from functools import partial
from ipaddress import ip_address
from time import time
from typing import Callable, Dict, Optional, Tuple
from urllib.parse import urlparse

from playwright._impl._errors import TargetClosedError, TimeoutError as PlaywrightTimeoutError
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

from scrapy_playwright import signals as pw_signals
from scrapy_playwright.headers import use_scrapy_headers
from scrapy_playwright.network_recorder import NetworkRecorder
from scrapy_playwright.page import PageMethod, PagePool, POOL_STRATEGY_REUSE_FIRST
from scrapy_playwright.tiling import WindowTilingManager, Rect
from scrapy_playwright._utils import (
    _encode_body,
    _get_header_value,
    _get_page_content,
    _is_safe_close_error,
    _maybe_await,
)
from scrapy_playwright.cloudflare.bypass import CloudflareBypass
from scrapy_playwright.cloudflare.diagnostics import CloudflareDiagnostics
from scrapy_playwright.cloudflare.remediation import RemediationInput, RemediationManager
from scrapy_playwright import camoufox_backend

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


# ── PlaywrightContext ──────────────────────────────────────────────────

class PlaywrightContext:
    """Wraps a single Playwright browser context with its page pool,
    semaphore, and optional network recorder.
    """

    def __init__(
        self,
        name: str,
        context: BrowserContext,
        max_pages: int,
        *,
        persistent: bool = False,
        pool_strategy: str = POOL_STRATEGY_REUSE_FIRST,
        init_scripts: Optional[list] = None,
        recorder: Optional[NetworkRecorder] = None,
        navigation_timeout: Optional[float] = None,
    ) -> None:
        self.name = name
        self.context = context
        self.persistent = persistent
        self.recorder = recorder
        self._navigation_timeout = navigation_timeout
        self._retained_pages: set[Page] = set()
        # Serializes use of the shared retained "carrier" page: only one
        # ``fetch()`` (and its seed navigation) may drive that page at a time.
        # Without this, concurrent ``playwright_fetch`` requests race the same
        # page — one issuing ``page.goto(seed)`` while another is mid-fetch —
        # which surfaces as ``Page.goto: Timeout 30000ms exceeded``.
        self.fetch_lock = asyncio.Lock()

        self.pool = PagePool(
            max_pages,
            strategy=pool_strategy,
            init_scripts=init_scripts,
        )

    # -- page lifecycle -------------------------------------------------

    async def acquire_page(self) -> Tuple[Page, bool]:
        """Return ``(page, is_new)``."""
        return await self.pool.acquire(self.context)

    def return_page(self, page: Page) -> None:
        self.pool.release(page)

    def retain_page(self, page: Page) -> None:
        if not page.is_closed():
            self._retained_pages.add(page)

    def forget_page(self, page: Optional[Page]) -> None:
        if page is not None:
            self._retained_pages.discard(page)

    def is_retained_page(self, page: Page) -> bool:
        return page in self._retained_pages

    async def get_retained_page(self, seed_url: Optional[str] = None) -> Optional[Page]:
        """Return a retained non-closed page, preferring the seed host."""
        seed_host = urlparse(seed_url or "").netloc
        fallback = None
        for page in list(self._retained_pages):
            if page.is_closed():
                self._retained_pages.discard(page)
                continue
            if fallback is None:
                fallback = page
            page_host = urlparse(page.url or "").netloc
            if seed_host and page_host == seed_host:
                return page
            if seed_host and (page.url or "") == "about:blank":
                fallback = page
        return fallback

    async def drain_pool(self) -> None:
        await self.pool.drain()

    async def get_open_page(self) -> Optional[Page]:
        """Return an existing non-closed page, or ``None``."""
        for page in self.context.pages:
            if not page.is_closed():
                return page
        return None


# ── Playwright engine ─────────────────────────────────────────────────

class PlaywrightEngine:
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
        pool_strategy: str = POOL_STRATEGY_REUSE_FIRST,
        stealth_init_scripts: Optional[list] = None,
        tiling_enabled: bool = False,
        tiling_screen_width: Optional[int] = None,
        tiling_screen_height: Optional[int] = None,
        tiling_margin: int = 0,
        tiling_adjust_viewport: bool = True,
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
        antibot_remediation_enabled: bool = False,
        antibot_remediation_actions: Optional[list] = None,
        profile_rotation_archive_dir: Optional[str] = None,
        fetch_failure_artifacts_enabled: bool = False,
        fetch_failure_artifacts_dir: pathlib.Path = pathlib.Path("playwright_fetch_failures"),
        signal_dispatcher: Optional[Callable] = None,
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
        self._pool_strategy = pool_strategy
        self._stealth_init_scripts = stealth_init_scripts
        self._restart_disconnected_browser = restart_disconnected_browser

        # Tiling
        self._tiling_enabled = tiling_enabled
        self._tiling_manager: Optional[WindowTilingManager] = None
        if self._tiling_enabled and not self._launch_options.get("headless", False) and not self._camoufox_options.get("headless", False):
            self._tiling_manager = WindowTilingManager(
                pool_size=self._max_pages_per_context * (self._max_contexts or 1),
                screen_width=tiling_screen_width,
                screen_height=tiling_screen_height,
                margin=tiling_margin,
                adjust_viewport=tiling_adjust_viewport,
            )

        # Carrier-page seeding (fetch mode). A Cloudflare interstitial can hang
        # the seed ``page.goto`` for the full timeout; retry a bounded number of
        # times with a short backoff instead of dropping the request.
        self._fetch_seed_timeout_ms = int(
            navigation_timeout if navigation_timeout else 45_000
        )
        self._fetch_seed_max_attempts = 2
        self._fetch_seed_retry_delay_s = 1.5

        # HAR
        self._har_recording = har_recording
        self._har_output_dir = har_output_dir
        self._har_url_filter = har_url_filter

        # Request processing
        self.process_request_headers = process_request_headers
        self.abort_request = abort_request
        self._signal_dispatcher = signal_dispatcher
        self._profile_rotation_archive_dir = profile_rotation_archive_dir
        self._fetch_failure_artifacts_enabled = fetch_failure_artifacts_enabled
        self._fetch_failure_artifacts_dir = fetch_failure_artifacts_dir

        # Runtime state
        self.playwright_context_manager: Optional[PlaywrightContextManager] = None
        self.playwright: Optional[AsyncPlaywright] = None
        self.browser_type: Optional[BrowserType] = None
        self._browser: Optional[Browser] = None
        self._camoufox_launchers: Dict[str, object] = {}
        self._context_kwargs_by_name: Dict[str, dict] = {}
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

        # Cloudflare challenge bypass
        if cf_challenge_retry:
            self._cf_bypass: Optional[CloudflareBypass] = CloudflareBypass(
                engine=self,
                max_retries=cf_max_retries,
                seed_url=cf_seed_url,
                seed_timeout=cf_seed_timeout,
                wait_timeout=cf_wait_timeout,
            )
        else:
            self._cf_bypass = None
        # Set by the handler after construction; lets the bypass re-close the
        # serialization gate when a post-clearance challenge is observed.
        self._cf_gate = None
        self._remediation = RemediationManager(
            engine=self,
            enabled=antibot_remediation_enabled,
            actions=antibot_remediation_actions,
        )

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

    def _emit_signal(self, signal, **kwargs) -> None:
        if self._signal_dispatcher is not None:
            self._signal_dispatcher(signal, **kwargs)

    async def remediate_antibot_block(self, event: RemediationInput) -> None:
        await self._remediation.remediate(event)

    def get_context_user_data_dir(self, context_name: str) -> Optional[str]:
        context_kwargs = self._context_kwargs_by_name.get(context_name) or {}
        value = context_kwargs.get(PERSISTENT_CONTEXT_PATH_KEY)
        if value is not None:
            return str(value)
        if self._uses_camoufox_backend:
            camoufox_options = camoufox_backend.prepare_launch_options(
                self._launch_options,
                self._camoufox_options,
                context_kwargs,
            )
            value = camoufox_options.get(PERSISTENT_CONTEXT_PATH_KEY)
            if value is not None:
                return str(value)
        return None

    def get_context_name_for_playwright_context(self, playwright_context: BrowserContext) -> str:
        for name, pw_ctx in self.contexts.items():
            if pw_ctx.context is playwright_context:
                return name
        return "unknown"

    @staticmethod
    def _sanitize_for_log(data):
        if isinstance(data, dict):
            sanitized = {}
            for key, value in data.items():
                key_lower = str(key).lower()
                if any(secret in key_lower for secret in ("password", "token", "secret", "authorization")):
                    sanitized[key] = "<redacted>"
                else:
                    sanitized[key] = PlaywrightEngine._sanitize_for_log(value)
            return sanitized
        if isinstance(data, list):
            return [PlaywrightEngine._sanitize_for_log(item) for item in data]
        return data

    @property
    def _uses_camoufox_backend(self) -> bool:
        return self._browser_type_name == CAMOUFOX_BROWSER_TYPE

    # ── Public entry point ─────────────────────────────────────────────

    async def process(self, request: Request, spider: Spider) -> Response:
        """Single entry point for downloading a request.

        When Cloudflare challenge retry is enabled, challenged responses
        trigger a reactive solve cycle via :class:`CloudflareBypass`.
        """
        context_name = request.meta.get("playwright_context", DEFAULT_CONTEXT_NAME)

        if self._cf_bypass is not None:
            await self._cf_bypass.wait_if_solving(context_name, request)

        response = await self._dispatch(request, spider)

        if self._cf_bypass is not None:
            response = await self._cf_bypass.handle_if_challenged(
                request, response, spider, context_name,
                dispatch_fn=self._dispatch,
            )
        return response

    async def _dispatch(self, request: Request, spider: Spider) -> Response:
        """Dispatch to fetch or page-navigation handler."""
        if request.meta.get("playwright_fetch"):
            return await self.download_with_fetch(request=request, spider=spider)
        handler = self.method_handlers.get(request.method, self._default_handler)
        return await handler(request=request, spider=spider)


    # ── Lifecycle ──────────────────────────────────────────────────────

    async def launch(self, startup_context_kwargs: Optional[dict] = None) -> None:
        """Start the Playwright driver and optionally create startup contexts."""
        logger.info("Starting Playwright facade")
        if self._uses_camoufox_backend:
            logger.info(
                "[Lifecycle] Using Camoufox backend with options=%s",
                _json.dumps(
                    self._sanitize_for_log(camoufox_backend.prepare_launch_options(self._launch_options, self._camoufox_options)),
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
            # A restart drops all in-memory contexts (and their warmed carrier
            # pages). Re-close the serialization gate so the next requests
            # re-serialize and re-warm the fresh context one at a time instead
            # of stampeding a cold context and re-triggering challenges.
            if self._cf_gate is not None:
                self._cf_gate.rearm(reason="driver_restart")
            self._browser = None
            if self._uses_camoufox_backend:
                await camoufox_backend.close_all_launchers(self._camoufox_launchers)
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
            self._emit_signal(
                pw_signals.driver_restarted,
                browser_type=self._browser_type_name,
            )
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
            await camoufox_backend.close_all_launchers(self._camoufox_launchers)
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
        self._emit_signal(
            pw_signals.browser_disconnected,
            restart_enabled=self._restart_disconnected_browser,
        )
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
        self._context_kwargs_by_name[name] = dict(context_kwargs)
        recorder = self._create_network_recorder(name, context_kwargs)
        if recorder:
            logger.info("Network recording enabled for context '%s'", name)
            self._inc_stat("playwright/har_recording_contexts")
        try:
            persistent = False
            if self._uses_camoufox_backend:
                camoufox_options = camoufox_backend.prepare_launch_options(self._launch_options, self._camoufox_options, context_kwargs)
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
                AsyncCamoufox = camoufox_backend.load_camoufox()
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
                init_scripts=self._stealth_init_scripts,
                recorder=recorder,
                navigation_timeout=self._navigation_timeout,
            )
            self.contexts[name] = pw_ctx
            self._emit_signal(
                pw_signals.context_created,
                context_name=name,
                persistent=persistent,
                remote=bool(self._cdp_url or self._connect_url),
            )
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
                    if self._tiling_manager is not None:
                        rect = await self._tiling_manager.assign_page(page)
                        if rect is not None:
                            cols, rows = self._tiling_manager._grid.cols, self._tiling_manager._grid.rows
                            self._emit_signal(
                                pw_signals.page_tiled,
                                context_name=pw_ctx.name,
                                slot_index=self._tiling_manager._slots.get(page),
                                rect=(rect.x, rect.y, rect.width, rect.height),
                                grid_size=(cols, rows),
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
                context_kwargs = dict(
                    request.meta.get("playwright_context_kwargs")
                    or self._context_kwargs_by_name.get(context_name)
                    or {}
                )
                har_meta = request.meta.get("playwright_har")
                if har_meta is not None:
                    context_kwargs["_playwright_har"] = har_meta
                pw_ctx = await self.create_context(
                    name=context_name, context_kwargs=context_kwargs, spider=spider,
                )
        return pw_ctx

    async def close_context(self, name: str) -> None:
        pw_ctx = self.contexts.get(name)
        if pw_ctx is None:
            return
        if pw_ctx.recorder:
            pw_ctx.recorder.finalize()
            self._set_stat("playwright/har_entry_count", pw_ctx.recorder.entry_count)
        await pw_ctx.drain_pool()
        with suppress(TargetClosedError):
            await pw_ctx.context.close()
        launcher = self._camoufox_launchers.pop(name, None)
        if launcher is not None:
            await camoufox_backend.close_launcher(name, launcher, self._camoufox_launchers)
        self.contexts.pop(name, None)

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
            self._emit_signal(
                pw_signals.page_created,
                context_name=pw_ctx.name,
                context_page_count=len(pw_ctx.context.pages),
                total_page_count=self.get_total_page_count(),
            )
            
            if self._tiling_manager is not None:
                rect = await self._tiling_manager.assign_page(page)
                if rect is not None:
                    cols, rows = self._tiling_manager._grid.cols, self._tiling_manager._grid.rows
                    self._emit_signal(
                        pw_signals.page_tiled,
                        context_name=pw_ctx.name,
                        slot_index=self._tiling_manager._slots.get(page),
                        rect=(rect.x, rect.y, rect.width, rect.height),
                        grid_size=(cols, rows),
                    )

        return page, pw_ctx, is_new

    def should_close_page(self, request: Request) -> bool:
        if request.meta.get("playwright_include_page"):
            return False
        return self._close_page_after_request

    async def return_or_close_page(self, request: Request, page: Page) -> None:
        context_name = request.meta.get("playwright_context", DEFAULT_CONTEXT_NAME)
        pw_ctx = self.contexts.get(context_name)
        if page.is_closed():
            self._inc_stat("playwright/page_count/closed")
            return
        if self.should_close_page(request):
            import traceback
            stack = "".join(traceback.format_stack()[:-1])
            logger.warning(
                "[Lifecycle] VERY VERBOSE LOG: Closing page after request due to config "
                f"(context='{context_name}', url={page.url}). Stack:\n{stack}"
            )
            await page.close()
            self._inc_stat("playwright/page_count/closed")
        elif pw_ctx is not None and pw_ctx.is_retained_page(page):
            logger.debug(
                "[Lifecycle] Page retained by response; not returning to pool "
                "(context='%s', url=%s)",
                context_name,
                page.url,
            )
        elif pw_ctx is not None and pw_ctx.pool is not None:
            logger.debug(
                "[Lifecycle] Returning page to pool after request "
                "(context='%s', url=%s)",
                context_name,
                page.url,
            )
            pw_ctx.return_page(page)
            self._inc_stat("playwright/page_count/returned_to_pool")
            logger.info(
                "[Lifecycle] Page returned to pool and ready for reuse "
                "(context='%s', url=%s)",
                context_name,
                page.url,
            )
        else:
            import traceback
            stack = "".join(traceback.format_stack()[:-1])
            logger.warning(
                "[Lifecycle] VERY VERBOSE LOG: Closing page after request because it could not be returned to pool or retained "
                f"(context='{context_name}', url={page.url}). Stack:\n{stack}"
            )
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

        seed_url = request.meta.get("playwright_fetch_seed_url")
        if not seed_url:
            referer = request.headers.get("Referer")
            if referer:
                seed_url = self._header_value_to_str(referer)
        if not seed_url:
            origin = request.headers.get("Origin")
            if origin:
                seed_url = self._header_value_to_str(origin)
                if seed_url:
                    seed_url = seed_url.rstrip("/") + "/"
        if not seed_url:
            parsed = urlparse(request.url)
            seed_url = f"{parsed.scheme}://{parsed.netloc}/"

        page = await pw_ctx.get_retained_page(seed_url)
        is_new = False
        if page is None:
            logger.info(
                "[Fetch] No retained page in context '%s', seeding with %s",
                context_name, seed_url,
            )
            page, is_new = await pw_ctx.acquire_page()
        else:
            current_url = page.url or ""
            current_host = urlparse(current_url).netloc
            seed_host = urlparse(seed_url).netloc
            if current_url == "about:blank" or (seed_host and current_host != seed_host):
                logger.info(
                    "[Fetch] Retained page in context '%s' is not fetch-ready "
                    "(page=%s seed=%s); navigating before fetch",
                    context_name,
                    current_url,
                    seed_url,
                )
                await self._seed_carrier_page(page, pw_ctx, seed_url, context_name)
            else:
                logger.debug(
                    "[Fetch] Reusing retained page in context '%s' for fetch "
                    "(page=%s seed=%s)",
                    context_name,
                    current_url,
                    seed_url,
                )
            return page

        if is_new:
            self._inc_stat("playwright/page_count")
            self._register_page(page, pw_ctx, spider=spider)
        await self._seed_carrier_page(page, pw_ctx, seed_url, context_name)
        pw_ctx.retain_page(page)
        logger.debug(
            "[Fetch] Retained new fetch carrier page (context='%s', url=%s)",
            context_name,
            page.url,
        )
        return page

    async def _seed_carrier_page(
        self,
        page: Page,
        pw_ctx: PlaywrightContext,
        seed_url: str,
        context_name: str,
    ) -> None:
        """Navigate the carrier *page* to *seed_url* with bounded retries.

        A Cloudflare interstitial on the seed navigation can hang ``page.goto``
        for the full navigation timeout. Rather than letting that single
        timeout drop the request (the spider would lose that listing's
        pagination), retry a couple of times with a short backoff. On final
        failure the broken page is forgotten/closed so the next request seeds a
        fresh one instead of reusing a stuck page.
        """
        attempts = self._fetch_seed_max_attempts
        timeout = self._fetch_seed_timeout_ms
        last_exc: Optional[Exception] = None
        for attempt in range(1, attempts + 1):
            try:
                await page.goto(
                    seed_url, wait_until="domcontentloaded", timeout=timeout,
                )
                return
            except PlaywrightTimeoutError as exc:
                last_exc = exc
                self._inc_stat("playwright/fetch_seed_timeout_count")
                is_final = attempt >= attempts
                logger.warning(
                    "[Fetch] Seed navigation timed out (context=%s seed=%s "
                    "attempt=%d/%d timeout=%dms)",
                    context_name, seed_url, attempt, attempts, timeout,
                )
                self._emit_signal(
                    pw_signals.fetch_seed_timeout,
                    context_name=context_name,
                    seed_url=seed_url,
                    attempt=attempt,
                    max_attempts=attempts,
                    timeout_ms=timeout,
                    final=is_final,
                )
                if not is_final:
                    await asyncio.sleep(self._fetch_seed_retry_delay_s)

        # Exhausted retries: drop the stuck carrier page so the next request
        # starts from a clean one, then surface the failure to the caller.
        pw_ctx.forget_page(page)
        with suppress(Exception):
            await page.close()
        raise last_exc  # type: ignore[misc]

    def _register_page(
        self,
        page: Page,
        pw_ctx: PlaywrightContext,
        spider: Optional[Spider] = None,
    ) -> None:
        """Attach lifecycle and network handlers to a newly discovered page."""
        page.on("close", self._make_close_page_cb(pw_ctx.name, page))
        page.on("crash", self._make_close_page_cb(pw_ctx.name, page))
        page.on("request", self._on_playwright_request)
        page.on("response", self._on_playwright_response)
        page.on("requestfailed", _make_requestfailed_logger(pw_ctx.name))
        page.on("console", _make_console_logger(pw_ctx.name))
        page.on("pageerror", _make_pageerror_logger(pw_ctx.name))
        if pw_ctx.recorder:
            page.on("response", pw_ctx.recorder.on_response)
            page.on("requestfailed", pw_ctx.recorder.on_request_failed)
        if logger.getEffectiveLevel() <= logging.DEBUG:
            page.on("request", _make_request_logger(pw_ctx.name, spider))
            page.on("response", _make_response_logger(pw_ctx.name, spider))

    @staticmethod
    def build_fetch_js(
        url: str,
        method: str,
        headers: dict,
        body: Optional[str],
        credentials: str,
    ) -> str:
        fetch_opts: dict = {
            "method": method,
            "headers": headers,
            "credentials": credentials,
        }
        if body is not None:
            fetch_opts["body"] = body
        opts_json = _json.dumps(fetch_opts, ensure_ascii=False)
        # Wrap in try/catch so a thrown TypeError ("NetworkError when attempting
        # to fetch resource", CORS denial, mixed content, etc.) doesn't propagate
        # out of page.evaluate as an opaque Playwright Error. Returning a tagged
        # payload lets the Python side log the actual browser-side cause and the
        # opaque/redirect response type when fetch resolved but the body wasn't
        # readable.
        return (
            f"async () => {{"
            f"  try {{"
            f"    const r = await fetch({_json.dumps(url)}, {opts_json});"
            f"    const hdrs = {{}};"
            f"    r.headers.forEach((v, k) => {{ hdrs[k] = v; }});"
            f"    let text = '';"
            f"    let bodyError = null;"
            f"    try {{ text = await r.text(); }}"
            f"    catch (be) {{ bodyError = {{name: be.name, message: String(be)}}; }}"
            f"    return {{ok: true, status: r.status, statusText: r.statusText,"
            f"             type: r.type, redirected: r.redirected, finalUrl: r.url,"
            f"             headers: hdrs, body: text, bodyError: bodyError}};"
            f"  }} catch (e) {{"
            f"    return {{ok: false, errorName: e.name || 'UnknownError',"
            f"             errorMessage: String(e), errorStack: e.stack || null}};"
            f"  }}"
            f"}}"
        )

    # Headers that *must* be dropped: passing them to fetch() corrupts the
    # request because the browser also sets them and the two versions would
    # collide. ``Cookie`` duplicates the jar attached via ``credentials:
    # include``; ``Content-Length`` and ``Host`` are derived from the body /
    # URL and a stale spider-side value desyncs the request frame.
    _FETCH_DROP_HEADERS = frozenset({
        "accept-encoding",
        "connection",
        "content-length",
        "cookie",
        "host",
        "origin",
        "priority",
        "referer",
        "te",
        "user-agent",
    })

    # Headers Firefox treats as "forbidden" in the Fetch spec — it silently
    # ignores any value we pass and uses its own derived from the live page
    # context (Origin/Referer/Sec-Fetch-*/Sec-Ch-Ua-*/UA/Accept-Encoding/
    # Connection/etc.). We let them through anyway so the spider's intent is
    # visible in the log line and so future Firefox versions that lift the
    # restriction would honor the spider value automatically.
    _FETCH_BROWSER_MANAGED_HEADERS = frozenset({
        "accept-encoding",
        "connection",
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
    })

    _FETCH_COOKIE_RELEVANT_RE = re.compile(
        r"(?:vivareal|grupozap|cf|zap|datadome|dd)", re.IGNORECASE
    )

    _FETCH_STORAGE_RELEVANT_RE = re.compile(
        r"(?:vivareal|grupozap|zap|cf|clearance|token|session)", re.IGNORECASE
    )

    @classmethod
    def _sanitize_fetch_headers(cls, headers: dict) -> dict:
        """Prepare spider headers for an in-page ``fetch()``.

        Strategy: forward almost everything. Only drop the small set that
        actively breaks the request when both the spider and the browser try
        to set it (``Cookie`` / ``Content-Length`` / ``Host``). The larger
        "browser-managed" group is passed through — Firefox will override it
        from the live page context per the Fetch spec, but the spider's
        intended values stay visible in the log.
        """
        sanitized: dict = {}
        dropped: list = []
        browser_managed: list = []
        for name, value in headers.items():
            lname = name.lower()
            if lname in cls._FETCH_DROP_HEADERS:
                dropped.append(name)
                continue
            if lname in cls._FETCH_BROWSER_MANAGED_HEADERS:
                browser_managed.append(name)
            sanitized[name] = value
        if dropped:
            logger.info(
                "[Fetch] Dropping conflicting headers before fetch(): %s",
                ", ".join(sorted(dropped, key=str.lower)),
            )
        if browser_managed:
            logger.debug(
                "[Fetch] Forwarding browser-managed headers (Firefox may "
                "override from page context): %s",
                ", ".join(sorted(browser_managed, key=str.lower)),
            )
        return sanitized

    @staticmethod
    def _header_value_to_str(value) -> Optional[str]:
        if isinstance(value, list):
            if not value:
                return None
            value = value[-1]
        if isinstance(value, bytes):
            return value.decode("utf-8")
        if value is None:
            return None
        return str(value)

    async def _describe_fetch_state(self, page: Page, url: str) -> dict:
        """Collect a snapshot of page + cookie state for fetch diagnostics.

        Always swallows exceptions: telemetry must never break the request.
        Returns the loosely-typed dict consumed by the log lines in
        ``download_with_fetch`` (also reused on the failure path so a
        ``NetworkError`` log line shows the page origin and the cookies that
        *would* have been attached to the request).
        """
        snapshot: dict = {
            "page_url": None,
            "cookies_for_url": [],
            "context_cookies": [],
            "context_cookie_count": None,
            "document_cookie_len": None,
            "cross_origin": None,
            "storage": {"local": [], "session": []},
        }
        try:
            snapshot["page_url"] = page.url
        except Exception:
            pass
        try:
            target = urlparse(url)
            page_parsed = urlparse(snapshot["page_url"] or "")
            if target.netloc and page_parsed.netloc:
                snapshot["cross_origin"] = target.netloc != page_parsed.netloc
        except Exception:
            pass
        try:
            ctx_cookies = await page.context.cookies()
            snapshot["context_cookie_count"] = len(ctx_cookies)
            # Keep only fields a reader actually needs while debugging CF/CORS:
            # the cookie name, domain (so we can spot scope drift), and value
            # length (raw value is sensitive and noisy).  ``partitionKey`` is
            # only present on Chromium today, but include defensively so we'd
            # notice if Firefox ever surfaces it.
            def _describe(c: dict) -> dict:
                return {
                    "name": c.get("name"),
                    "domain": c.get("domain"),
                    "path": c.get("path"),
                    "value_len": len(c.get("value", "") or ""),
                    "http_only": c.get("httpOnly"),
                    "secure": c.get("secure"),
                    "same_site": c.get("sameSite"),
                    "partition_key": c.get("partitionKey"),
                    "expires": c.get("expires"),
                }
            snapshot["context_cookies"] = [_describe(c) for c in ctx_cookies]
            url_cookies = await page.context.cookies(url)
            snapshot["cookies_for_url"] = [_describe(c) for c in url_cookies]
        except Exception as exc:
            logger.debug("[Fetch] Cookie snapshot failed: %s", exc)
        try:
            doc_cookie = await page.evaluate("() => document.cookie || ''")
            snapshot["document_cookie_len"] = len(doc_cookie or "")
        except Exception:
            pass
        try:
            storage = await page.evaluate(
                """() => {
                    const summarize = (storage) => {
                        const items = [];
                        for (let i = 0; i < storage.length; i += 1) {
                            const key = storage.key(i);
                            const value = key == null ? '' : (storage.getItem(key) || '');
                            items.push({key, valueLength: value.length});
                        }
                        return items;
                    };
                    return {
                        local: summarize(window.localStorage),
                        session: summarize(window.sessionStorage),
                    };
                }"""
            )
            snapshot["storage"] = {
                "local": [
                    item for item in storage.get("local", [])
                    if self._FETCH_STORAGE_RELEVANT_RE.search(item.get("key") or "")
                ],
                "session": [
                    item for item in storage.get("session", [])
                    if self._FETCH_STORAGE_RELEVANT_RE.search(item.get("key") or "")
                ],
            }
        except Exception:
            pass
        return snapshot

    @staticmethod
    def _body_snippet(text: Optional[str], limit: int = 500) -> str:
        if not text:
            return ""
        compact = re.sub(r"\s+", " ", text)
        return compact[:limit]

    @classmethod
    def _detect_block_indicators(cls, *, text: str, headers: dict, page_url: Optional[str]) -> list[str]:
        indicators: list[str] = []
        text_lower = (text or "").lower()
        header_blob = _json.dumps(headers or {}, ensure_ascii=False).lower()
        page_lower = (page_url or "").lower()
        checks = {
            "cloudflare_challenge": (
                "cf-mitigated" in header_blob
                or "cdn-cgi/challenge-platform" in text_lower
                or "cf challenge" in text_lower
                or "just a moment" in text_lower
                or "attention required" in text_lower
            ),
            "access_denied": "access denied" in text_lower,
            "captcha": "captcha" in text_lower or "turnstile" in text_lower,
            "challenge_page_url": "cdn-cgi/challenge-platform" in page_lower,
        }
        for name, matched in checks.items():
            if matched:
                indicators.append(name)
        return indicators

    @classmethod
    def _filter_relevant_cookies(cls, cookies: list[dict]) -> list[dict]:
        return [
            cookie for cookie in cookies
            if cls._FETCH_COOKIE_RELEVANT_RE.search(cookie.get("name") or "")
            or cls._FETCH_COOKIE_RELEVANT_RE.search(cookie.get("domain") or "")
        ]

    async def _capture_fetch_failure_artifacts(
        self,
        *,
        page: Page,
        request: Request,
        pre_state: dict,
        post_state: dict,
        hdrs: dict,
        err_name: str,
        err_msg: str,
        err_stack: Optional[str],
        elapsed: float,
    ) -> Optional[pathlib.Path]:
        if not self._fetch_failure_artifacts_enabled:
            return None

        artifact_dir = self._fetch_failure_artifacts_dir / f"fetch_failure_{int(time() * 1000)}"
        artifact_dir.mkdir(parents=True, exist_ok=True)

        screenshot_path = artifact_dir / "page.png"
        html_path = artifact_dir / "page.html"
        metadata_path = artifact_dir / "metadata.json"

        html = None
        try:
            await page.screenshot(path=str(screenshot_path), full_page=True)
        except Exception as exc:
            logger.debug("[Fetch] Failed to capture screenshot for %s: %s", request.url, exc)
        try:
            html = await page.content()
            html_path.write_text(html, encoding="utf-8")
        except Exception as exc:
            logger.debug("[Fetch] Failed to capture HTML for %s: %s", request.url, exc)

        metadata = {
            "request": {
                "url": request.url,
                "method": request.method,
                "headers": hdrs,
                "credentials": request.meta.get("playwright_fetch_credentials", "include"),
                "meta": {
                    "playwright_context": request.meta.get("playwright_context"),
                    "playwright_fetch_seed_url": request.meta.get("playwright_fetch_seed_url"),
                    "listing_type": request.meta.get("listing_type"),
                    "business": request.meta.get("business"),
                    "state": request.meta.get("state"),
                    "city": request.meta.get("city"),
                    "neighborhood": request.meta.get("neighborhood"),
                },
            },
            "error": {
                "name": err_name,
                "message": err_msg,
                "stack": err_stack,
                "elapsed": elapsed,
            },
            "page": {
                "final_url": post_state.get("page_url") or pre_state.get("page_url"),
                "block_indicators": self._detect_block_indicators(
                    text=html or "",
                    headers={},
                    page_url=post_state.get("page_url") or pre_state.get("page_url"),
                ),
            },
            "cookies": {
                "pre_context": self._filter_relevant_cookies(pre_state.get("context_cookies", [])),
                "pre_for_url": self._filter_relevant_cookies(pre_state.get("cookies_for_url", [])),
                "post_context": self._filter_relevant_cookies(post_state.get("context_cookies", [])),
                "post_for_url": self._filter_relevant_cookies(post_state.get("cookies_for_url", [])),
            },
            "storage": {
                "pre": pre_state.get("storage", {}),
                "post": post_state.get("storage", {}),
            },
            "artifacts": {
                "screenshot": screenshot_path.name if screenshot_path.exists() else None,
                "html": html_path.name if html_path.exists() else None,
            },
        }
        metadata_path.write_text(
            _json.dumps(metadata, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )
        return artifact_dir

    async def download_with_fetch(
        self, request: Request, spider: Spider,
    ) -> Response:
        """Execute *request* as a ``fetch()`` call inside the browser.

        Serialized per context on the carrier page: seeding the page and
        running the in-page ``fetch()`` must not interleave with another
        request driving the same retained page.
        """
        pw_ctx = await self.get_or_create_context(request, spider)
        async with pw_ctx.fetch_lock:
            return await self._download_with_fetch_locked(request, spider)

    async def _download_with_fetch_locked(
        self, request: Request, spider: Spider,
    ) -> Response:
        start_time = time()
        page = await self.get_or_create_fetch_page(request, spider)
        credentials_mode = request.meta.get("playwright_fetch_credentials", "include")
        if credentials_mode not in {"include", "same-origin", "omit"}:
            logger.warning(
                "[Fetch] Invalid credentials mode %r for %s; falling back to 'include'",
                credentials_mode,
                request.url,
            )
            credentials_mode = "include"

        hdrs: dict = {}
        for key, values in request.headers.items():
            name = key.decode("utf-8") if isinstance(key, bytes) else key
            val = self._header_value_to_str(values)
            if val is not None:
                hdrs[name] = val
        hdrs = self._sanitize_fetch_headers(hdrs)

        body_str: Optional[str] = None
        if request.body:
            body_str = request.body.decode(request.encoding)
        body_size = len(request.body or b"")

        pre_state = await self._describe_fetch_state(page, request.url)

        js_code = self.build_fetch_js(
            url=request.url,
            method=request.method,
            headers=hdrs,
            body=body_str,
            credentials=credentials_mode,
        )
        cookie_names = [c["name"] for c in pre_state["cookies_for_url"]]
        logger.info(
            "[Fetch] -> %s %s page=%s cross_origin=%s credentials=%s headers=%d body=%dB "
            "ctx_cookies=%s cookies_for_url=%d (%s) doc_cookie_len=%s",
            request.method, request.url,
            pre_state["page_url"], pre_state["cross_origin"], credentials_mode,
            len(hdrs), body_size,
            pre_state["context_cookie_count"],
            len(cookie_names), ",".join(cookie_names) or "-",
            pre_state["document_cookie_len"],
        )
        if pre_state["cross_origin"] and credentials_mode == "include":
            logger.warning(
                "[Fetch] Cross-origin request is using credentials='include' for %s; "
                "this requires Access-Control-Allow-Credentials: true",
                request.url,
            )
        logger.debug(
            "[Fetch] Request headers after sanitize (%d): %s",
            len(hdrs), hdrs,
        )
        logger.debug(
            "[Fetch] Cookies that would be attached to %s: %s",
            request.url, pre_state["cookies_for_url"],
        )
        # Full context jar — when ``cookies_for_url`` comes back empty but the
        # context has cookies, this is what tells us whether the right ones
        # exist but got partitioned/scope-filtered (Firefox ETP / Total Cookie
        # Protection / SameSite) vs. genuinely never being set.
        logger.debug(
            "[Fetch] Full context cookie jar (%d): %s",
            pre_state["context_cookie_count"] or 0,
            pre_state["context_cookies"],
        )
        logger.debug(
            "[Fetch] Relevant storage keys before request: %s",
            pre_state.get("storage"),
        )

        result = await page.evaluate(js_code)
        elapsed = time() - start_time

        if not result.get("ok", True):
            # The in-page fetch() threw before producing a Response. Surface
            # the actual browser-side error rather than letting Playwright's
            # generic "NetworkError when attempting to fetch resource" mask it.
            err_name = result.get("errorName", "UnknownError")
            err_msg = result.get("errorMessage", "")
            err_stack = result.get("errorStack")
            post_state = await self._describe_fetch_state(page, request.url)
            logger.error(
                "[Fetch] <- THROW %s %s name=%s message=%s elapsed=%.3fs "
                "page=%s ctx_cookies=%s cookies_for_url=%d",
                request.method, request.url,
                err_name, err_msg, elapsed,
                post_state["page_url"], post_state["context_cookie_count"],
                len(post_state["cookies_for_url"]),
            )
            if err_stack:
                logger.debug("[Fetch] In-page error stack: %s", err_stack)
            artifact_dir = await self._capture_fetch_failure_artifacts(
                page=page,
                request=request,
                pre_state=pre_state,
                post_state=post_state,
                hdrs=hdrs,
                err_name=err_name,
                err_msg=err_msg,
                err_stack=err_stack,
                elapsed=elapsed,
            )
            logger.error(
                "[Fetch] Failure diagnostics url=%s page=%s relevant_cookies=%s storage=%s artifacts=%s",
                request.url,
                post_state.get("page_url"),
                self._filter_relevant_cookies(post_state.get("context_cookies", [])),
                post_state.get("storage"),
                str(artifact_dir) if artifact_dir else "disabled",
            )
            self._inc_stat("playwright/fetch_error_count")
            self._inc_stat(f"playwright/fetch_error/{err_name}")
            raise RuntimeError(
                f"playwright_fetch: in-page fetch() threw {err_name}: {err_msg} "
                f"(url={request.url})"
            )

        status = result["status"]
        resp_headers_raw = result.get("headers") or {}
        content_encoding = (
            resp_headers_raw.get("content-encoding")
            or resp_headers_raw.get("Content-Encoding")
        )
        set_cookie_raw = (
            resp_headers_raw.get("set-cookie")
            or resp_headers_raw.get("Set-Cookie")
            or ""
        )
        # Set-Cookie values come back joined by ", " in fetch() — best-effort
        # split on cookie boundaries (commas inside Expires=... attr would
        # confuse a naive split; this is only for diagnostic logging).
        set_cookie_names = [
            piece.split("=", 1)[0].strip()
            for piece in set_cookie_raw.split("\n")
            if "=" in piece
        ] if set_cookie_raw else []

        logger.info(
            "[Fetch] <- %s %s status=%s type=%s redirected=%s finalUrl=%s "
            "elapsed=%.3fs body=%dB resp_headers=%d enc=%s set_cookie=%s",
            request.method, request.url,
            status, result.get("type"), result.get("redirected"),
            result.get("finalUrl"), elapsed,
            len(result.get("body", "") or ""), len(resp_headers_raw),
            content_encoding or "-",
            ",".join(set_cookie_names) or "-",
        )
        logger.debug(
            "[Fetch] In-page fetch() resolved: statusText=%r headers=%s",
            result.get("statusText"), resp_headers_raw,
        )
        if result.get("bodyError"):
            logger.warning(
                "[Fetch] response.text() failed for %s: %s",
                request.url, result["bodyError"],
            )
        resp_headers = Headers(resp_headers_raw)
        resp_headers.pop("Content-Encoding", None)
        resp_body_text = result.get("body", "")
        body_bytes, encoding = _encode_body(headers=resp_headers, text=resp_body_text)
        request.meta["download_latency"] = elapsed
        self._emit_signal(
            pw_signals.fetch_executed,
            url=request.url,
            method=request.method,
            status=status,
            duration=elapsed,
        )

        respcls = responsetypes.from_args(headers=resp_headers, url=request.url, body=body_bytes)
        self._inc_stat("playwright/fetch_count")
        self._inc_stat(f"playwright/fetch_status/{status // 100}xx")
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
            if not request.meta.get("playwright_include_page") and not page.is_closed():
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

        if request.meta.get("playwright_include_page"):
            context_name = request.meta.get("playwright_context", DEFAULT_CONTEXT_NAME)
            pw_ctx = self.contexts.get(context_name)
            if pw_ctx is not None:
                pw_ctx.retain_page(page)
                logger.debug(
                    "[Lifecycle] Retained page for response "
                    "(context='%s', url=%s)",
                    context_name,
                    page.url,
                )
        else:
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
                # Merge Scrapy-set headers on top of browser headers so custom
                # headers (e.g. X-Domain, Accept) reach the server.
                if self.process_request_headers is None and headers:
                    merged = dict(final_headers)
                    for key, values in headers.items():
                        name = key.decode("utf-8") if isinstance(key, bytes) else key
                        val = self._header_value_to_str(values)
                        if val is not None:
                            merged[name.lower()] = val
                    overrides["headers"] = merged
                    final_headers = merged
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
        if self._cf_bypass is not None and CloudflareDiagnostics.should_log_network_event(request):
            asyncio.create_task(self._cf_bypass._diagnostics.log_request_event(request))

    def _on_playwright_response(self, response: PlaywrightResponse) -> None:
        self._inc_stat("playwright/response_count")
        self._inc_stat(f"playwright/response_count/resource_type/{response.request.resource_type}")
        self._inc_stat(f"playwright/response_count/method/{response.request.method}")
        if self._cf_bypass is not None and CloudflareDiagnostics.should_log_network_event(response.request):
            asyncio.create_task(self._cf_bypass._diagnostics.log_response_event(response))

    def _make_close_page_cb(
        self, context_name: str, tracked_page: Optional[Page] = None
    ) -> Callable:
        def cb(page: Optional[Page] = None) -> None:
            p = page or tracked_page
            url = p.url if p else "unknown"
            logger.warning(
                "[Lifecycle] VERY VERBOSE LOG: PAGE CLOSED EVENT RECEIVED FROM BROWSER. "
                "The browser window was closed internally by the browser itself, the OS, or a user clicking the X button. "
                "Page closed: '%s' (context: '%s')",
                url, context_name
            )
            if page and self._tiling_manager is not None:
                self._tiling_manager.release_page(page)
            pw_ctx = self.contexts.get(context_name)
            if pw_ctx is not None:
                pw_ctx.forget_page(page or tracked_page)
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
                    asyncio.create_task(camoufox_backend.close_launcher(name, launcher, self._camoufox_launchers))
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


def _make_requestfailed_logger(context_name: str) -> Callable:
    """Log every Camoufox/Firefox-level request failure (DNS, TLS, CORS abort,
    NS_ERROR_*, NS_BINDING_ABORTED, etc.) — these never surface as an exception
    in Playwright's `page.evaluate`/`fetch()` flow on their own."""
    def _log_requestfailed(request: PlaywrightRequest) -> None:
        failure = getattr(request, "failure", None)
        logger.error(
            "[Context=%s] RequestFailed: <%s %s> resource_type=%s failure=%s",
            context_name, request.method, request.url, request.resource_type, failure,
        )
    return _log_requestfailed


def _make_console_logger(context_name: str) -> Callable:
    """Forward browser console messages into Python logs — Firefox prints CORS
    rejections, mixed-content blocks, and CSP violations to the console even
    when fetch() raises a generic NetworkError."""
    def _log_console(message) -> None:
        try:
            level = (message.type or "").lower()
            text = message.text
        except Exception:
            level, text = "log", str(message)
        py_level = {
            "error": logging.ERROR,
            "warning": logging.WARNING,
            "warn": logging.WARNING,
            "info": logging.INFO,
            "debug": logging.DEBUG,
        }.get(level, logging.INFO)
        logger.log(py_level, "[Context=%s] Console[%s]: %s", context_name, level or "log", text)
    return _log_console


def _make_pageerror_logger(context_name: str) -> Callable:
    """Log uncaught JS errors emitted by the page (helps when an in-page fetch
    chain throws before/after our wrapped try/catch)."""
    def _log_pageerror(error) -> None:
        logger.error("[Context=%s] PageError: %s", context_name, error)
    return _log_pageerror


# Back-compat alias — external code may import this name.
Playwright = PlaywrightEngine
