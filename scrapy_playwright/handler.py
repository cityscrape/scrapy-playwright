import asyncio
import logging
import os
import pathlib
import platform
from dataclasses import dataclass
from time import time
from typing import Awaitable, Optional, Type, TypeVar

from playwright._impl._errors import TargetClosedError
from scrapy import Spider, signals, version_info as scrapy_version_info
from scrapy.core.downloader.handlers.http11 import HTTP11DownloadHandler
from scrapy.crawler import Crawler
from scrapy.exceptions import NotSupported
from scrapy.http import Request, Response
from scrapy.settings import Settings
from scrapy.utils.defer import deferred_from_coro
from scrapy.utils.misc import load_object
from scrapy.utils.reactor import verify_installed_reactor
from twisted.internet.defer import Deferred, inlineCallbacks

from scrapy_playwright import signals as pw_signals
from scrapy_playwright.headers import use_scrapy_headers
from scrapy_playwright.playwright import (
    Playwright,
    PlaywrightContext,  # noqa: F401 (back-compat re-export)
    Download,  # noqa: F401 (back-compat re-export)
    DEFAULT_CONTEXT_NAME,  # noqa: F401 (back-compat re-export)
)
from scrapy_playwright.page import PagePool  # noqa: F401 (back-compat re-export)
from scrapy_playwright._utils import (
    _ThreadedLoopAdapter,
    _get_float_setting,
    _is_safe_close_error,
)

__all__ = ["ScrapyPlaywrightDownloadHandler"]

_SCRAPY_ASYNC_API = scrapy_version_info >= (2, 14, 0)

PlaywrightHandler = TypeVar("PlaywrightHandler", bound="ScrapyPlaywrightDownloadHandler")

logger = logging.getLogger("scrapy-playwright")

DEFAULT_BROWSER_TYPE = "chromium"
VALID_BROWSER_TYPES = {"chromium", "firefox", "webkit", "camoufox"}

# Back-compat re-exports (tests & memusage import these from handler)
POOL_STRATEGY_REUSE_FIRST = "reuse_first"
POOL_STRATEGY_CREATE_FIRST = "create_first"
VALID_POOL_STRATEGIES = {POOL_STRATEGY_REUSE_FIRST, POOL_STRATEGY_CREATE_FIRST}
PERSISTENT_CONTEXT_PATH_KEY = "user_data_dir"


@dataclass
class Config:
    cdp_url: Optional[str]
    cdp_kwargs: dict
    connect_url: Optional[str]
    connect_kwargs: dict
    browser_type_name: str
    launch_options: dict
    camoufox_options: dict
    max_pages_per_context: int
    max_contexts: Optional[int]
    startup_context_kwargs: dict
    navigation_timeout: Optional[float]
    restart_disconnected_browser: bool
    close_page_after_request: bool
    page_pooling: bool = False
    page_pool_strategy: str = POOL_STRATEGY_REUSE_FIRST
    target_closed_max_retries: int = 3
    use_threaded_loop: bool = False
    har_recording: bool = False
    har_output_dir: str = "har_recordings"
    har_url_filter: Optional[str] = None
    stealth_init_scripts: Optional[list] = None
    cf_challenge_retry: bool = False
    cf_max_retries: int = 3
    cf_seed_url: Optional[str] = None
    cf_seed_timeout: int = 90_000
    cf_wait_timeout: int = 60_000

    @classmethod
    def from_settings(cls, settings: Settings) -> "Config":
        if settings.get("PLAYWRIGHT_CDP_URL") and settings.get("PLAYWRIGHT_CONNECT_URL"):
            msg = "Setting both PLAYWRIGHT_CDP_URL and PLAYWRIGHT_CONNECT_URL is not supported"
            logger.error(msg)
            raise NotSupported(msg)
        cfg = cls(
            cdp_url=settings.get("PLAYWRIGHT_CDP_URL"),
            cdp_kwargs=settings.getdict("PLAYWRIGHT_CDP_KWARGS") or {},
            connect_url=settings.get("PLAYWRIGHT_CONNECT_URL"),
            connect_kwargs=settings.getdict("PLAYWRIGHT_CONNECT_KWARGS") or {},
            browser_type_name=settings.get("PLAYWRIGHT_BROWSER_TYPE") or DEFAULT_BROWSER_TYPE,
            launch_options=settings.getdict("PLAYWRIGHT_LAUNCH_OPTIONS") or {},
            camoufox_options=settings.getdict("PLAYWRIGHT_CAMOUFOX_OPTIONS") or {},
            max_pages_per_context=settings.getint("PLAYWRIGHT_MAX_PAGES_PER_CONTEXT"),
            max_contexts=settings.getint("PLAYWRIGHT_MAX_CONTEXTS") or None,
            startup_context_kwargs=settings.getdict("PLAYWRIGHT_CONTEXTS"),
            navigation_timeout=_get_float_setting(
                settings, "PLAYWRIGHT_DEFAULT_NAVIGATION_TIMEOUT"
            ),
            restart_disconnected_browser=settings.getbool(
                "PLAYWRIGHT_RESTART_DISCONNECTED_BROWSER", default=True
            ),
            close_page_after_request=settings.getbool(
                "PLAYWRIGHT_CLOSE_PAGE_AFTER_REQUEST", default=False
            ),
            page_pooling=settings.getbool("PLAYWRIGHT_PAGE_POOLING", default=False),
            page_pool_strategy=settings.get(
                "PLAYWRIGHT_PAGE_POOL_STRATEGY", POOL_STRATEGY_REUSE_FIRST
            ),
            use_threaded_loop=platform.system() == "Windows"
                              or settings.getbool("_PLAYWRIGHT_THREADED_LOOP", False),
            har_recording=settings.getbool("PLAYWRIGHT_HAR_RECORDING", default=False),
            har_output_dir=settings.get(
                "PLAYWRIGHT_HAR_OUTPUT_DIR",
                os.path.join(settings.get("OUTPUT_DIRECTORY", "output"), "har_recordings"),
            ),
            har_url_filter=settings.get("PLAYWRIGHT_HAR_URL_FILTER"),
            stealth_init_scripts=settings.getlist("PLAYWRIGHT_STEALTH_INIT_SCRIPTS", None),
            cf_challenge_retry=settings.getbool(
                "PLAYWRIGHT_CLOUDFLARE_CHALLENGE_RETRY", default=False
            ),
            cf_max_retries=settings.getint("PLAYWRIGHT_CLOUDFLARE_MAX_RETRIES", 3),
            cf_seed_url=settings.get("PLAYWRIGHT_CLOUDFLARE_SEED_URL"),
            cf_seed_timeout=settings.getint("PLAYWRIGHT_CLOUDFLARE_SEED_TIMEOUT", 90_000),
            cf_wait_timeout=settings.getint("PLAYWRIGHT_CLOUDFLARE_WAIT_TIMEOUT", 60_000),
        )
        cfg.cdp_kwargs.pop("endpoint_url", None)
        cfg.connect_kwargs.pop("ws_endpoint", None)
        if cfg.browser_type_name not in VALID_BROWSER_TYPES:
            raise NotSupported(
                f"Invalid PLAYWRIGHT_BROWSER_TYPE: {cfg.browser_type_name!r}. "
                f"Valid values: {', '.join(sorted(VALID_BROWSER_TYPES))}"
            )
        if not cfg.max_pages_per_context:
            cfg.max_pages_per_context = settings.getint("CONCURRENT_REQUESTS")
        if (cfg.cdp_url or cfg.connect_url) and cfg.launch_options:
            logger.warning("Connecting to remote browser, ignoring PLAYWRIGHT_LAUNCH_OPTIONS")
        if cfg.browser_type_name == "camoufox" and (cfg.cdp_url or cfg.connect_url):
            raise NotSupported(
                "PLAYWRIGHT_CDP_URL and PLAYWRIGHT_CONNECT_URL are not supported with Camoufox"
            )
        if cfg.page_pool_strategy not in VALID_POOL_STRATEGIES:
            raise NotSupported(
                f"Invalid PLAYWRIGHT_PAGE_POOL_STRATEGY: {cfg.page_pool_strategy!r}. "
                f"Valid values: {', '.join(sorted(VALID_POOL_STRATEGIES))}"
            )
        return cfg


class ScrapyPlaywrightDownloadHandler(HTTP11DownloadHandler):
    _shared_by_crawler_id: dict[int, dict] = {}

    def __init__(self, crawler: Crawler) -> None:
        verify_installed_reactor("twisted.internet.asyncioreactor.AsyncioSelectorReactor")
        if _SCRAPY_ASYNC_API:
            super().__init__(crawler=crawler)
        else:
            super().__init__(  # pylint: disable=unexpected-keyword-arg
                settings=crawler.settings, crawler=crawler
            )
        self.stats = crawler.stats
        self.config = Config.from_settings(crawler.settings)
        self._crawler_id = id(crawler)

        if self.config.use_threaded_loop:
            _ThreadedLoopAdapter.start(id(self))

        if _SCRAPY_ASYNC_API:
            crawler.signals.connect(self._maybe_launch_in_thread, signals.engine_started)
        else:
            crawler.signals.connect(self._engine_started, signals.engine_started)

        crawler.signals.connect(self._spider_closed, signals.spider_closed)

        # Resolve header/abort callables from settings
        if "PLAYWRIGHT_PROCESS_REQUEST_HEADERS" in crawler.settings:
            if crawler.settings["PLAYWRIGHT_PROCESS_REQUEST_HEADERS"] is None:
                process_request_headers = None
            else:
                process_request_headers = load_object(
                    crawler.settings["PLAYWRIGHT_PROCESS_REQUEST_HEADERS"]
                )
        else:
            process_request_headers = use_scrapy_headers

        abort_request = None
        if crawler.settings.get("PLAYWRIGHT_ABORT_REQUEST"):
            abort_request = load_object(crawler.settings["PLAYWRIGHT_ABORT_REQUEST"])

        shared = self._shared_by_crawler_id.get(self._crawler_id)
        if shared is None:
            pw = Playwright(
                browser_type_name=self.config.browser_type_name,
                launch_options=self.config.launch_options,
                camoufox_options=self.config.camoufox_options,
                max_pages_per_context=self.config.max_pages_per_context,
                navigation_timeout=self.config.navigation_timeout,
                close_page_after_request=self.config.close_page_after_request,
                pool_enabled=self.config.page_pooling,
                pool_strategy=self.config.page_pool_strategy,
                stealth_init_scripts=self.config.stealth_init_scripts,
                har_recording=self.config.har_recording,
                har_output_dir=pathlib.Path(self.config.har_output_dir),
                har_url_filter=self.config.har_url_filter,
                cdp_url=self.config.cdp_url,
                cdp_kwargs=self.config.cdp_kwargs,
                connect_url=self.config.connect_url,
                connect_kwargs=self.config.connect_kwargs,
                max_contexts=self.config.max_contexts,
                restart_disconnected_browser=self.config.restart_disconnected_browser,
                process_request_headers=process_request_headers,
                abort_request=abort_request,
                cf_challenge_retry=self.config.cf_challenge_retry,
                cf_max_retries=self.config.cf_max_retries,
                cf_seed_url=self.config.cf_seed_url,
                cf_seed_timeout=self.config.cf_seed_timeout,
                cf_wait_timeout=self.config.cf_wait_timeout,
            )
            pw.on_stats_inc = self.stats.inc_value
            pw.on_stats_set = self.stats.set_value

            cf_gate = asyncio.Event()
            if not self.config.cf_challenge_retry:
                cf_gate.set()

            shared = {
                "pw": pw,
                "cf_gate": cf_gate,
                "cf_serial_lock": asyncio.Lock(),
                "launch_lock": asyncio.Lock(),
                "close_lock": asyncio.Lock(),
                "launched": False,
                "refcount": 0,
            }
            self._shared_by_crawler_id[self._crawler_id] = shared
        else:
            logger.info(
                "[Lifecycle] Reusing shared Playwright facade for duplicate handler "
                "(crawler_id=%s, refcount=%d)",
                self._crawler_id, shared["refcount"],
            )

        shared["refcount"] += 1
        self._shared_state = shared
        self.pw = shared["pw"]
        self._cf_gate = shared["cf_gate"]
        self._cf_serial_lock = shared["cf_serial_lock"]

    def _spider_closed(self, spider: Spider, reason: str) -> None:
        logger.info("[Lifecycle] _spider_closed reason=%s", reason)
        self.pw.is_closing = True

    @classmethod
    def from_crawler(cls: Type[PlaywrightHandler], crawler: Crawler) -> PlaywrightHandler:
        return cls(crawler)

    def _deferred_from_coro(self, coro: Awaitable) -> Deferred:
        if self.config.use_threaded_loop:
            return _ThreadedLoopAdapter._deferred_from_coro(coro)
        return deferred_from_coro(coro)

    def _maybe_future_from_coro(self, coro: Awaitable) -> Awaitable | asyncio.Future:
        if self.config.use_threaded_loop:
            return _ThreadedLoopAdapter._future_from_coro(coro)
        return coro

    def _engine_started(self) -> Deferred:
        return self._deferred_from_coro(self._launch())

    async def _maybe_launch_in_thread(self) -> None:
        await self._maybe_future_from_coro(self._launch())

    async def _launch(self) -> None:
        """Launch the Playwright facade and configured startup context(s)."""
        async with self._shared_state["launch_lock"]:
            if self._shared_state["launched"]:
                logger.info(
                    "[Lifecycle] Shared Playwright facade already launched "
                    "(crawler_id=%s)",
                    self._crawler_id,
                )
                return
            await self.pw.launch(
                startup_context_kwargs=self.config.startup_context_kwargs or None,
            )
            self._shared_state["launched"] = True
            self.stats.set_value("playwright/page_count", self.pw.get_total_page_count())
            self._crawler.signals.send_catch_log(
                pw_signals.handler_ready,
                browser_type=self.config.browser_type_name,
            )

    # ── Twisted close / download_request branching ──────────────────────

    if _SCRAPY_ASYNC_API:

        async def close(self) -> None:
            await super().close()
            await self._maybe_future_from_coro(self._close())
            if self.config.use_threaded_loop:
                _ThreadedLoopAdapter.stop(id(self))

    else:

        @inlineCallbacks
        def close(self) -> Deferred:  # pylint: disable=invalid-overridden-method
            yield super().close()
            yield self._deferred_from_coro(self._close())
            if self.config.use_threaded_loop:
                _ThreadedLoopAdapter.stop(id(self))

    async def _close(self) -> None:
        async with self._shared_state["close_lock"]:
            self._shared_state["refcount"] -= 1
            if self._shared_state["refcount"] > 0:
                logger.info(
                    "[Lifecycle] Shared Playwright facade still in use; skipping close "
                    "(crawler_id=%s, remaining_refs=%d)",
                    self._crawler_id, self._shared_state["refcount"],
                )
                return
            self.pw.is_closing = True
            self._crawler.signals.send_catch_log(
                pw_signals.handler_closing,
                context_count=len(self.pw.contexts),
                page_count=self.pw.get_total_page_count(),
            )
            await self.pw.close()
            self._shared_state["launched"] = False
            self._shared_by_crawler_id.pop(self._crawler_id, None)

    if _SCRAPY_ASYNC_API:

        async def download_request(self, request: Request) -> Response:
            if request.meta.get("playwright"):
                coro = self._download_request(request)
                return await self._maybe_future_from_coro(coro)
            return await super().download_request(  # pylint: disable=no-value-for-parameter
                request
            )

    else:

        def download_request(
                # type: ignore[misc] # pylint: disable=invalid-overridden-method,arguments-differ # noqa: E501
                self, request: Request, spider: Spider
        ) -> Deferred:
            if request.meta.get("playwright"):
                return self._deferred_from_coro(self._download_request(request, spider))
            return super().download_request(  # pylint: disable=unexpected-keyword-arg
                request=request, spider=spider
            )

    # ── Serialization gate (CF clearance) ───────────────────────────

    async def _download_request(self, request: Request, spider: Spider | None = None) -> Response:
        """Entry point for Playwright downloads.

        When Cloudflare challenge-retry is enabled, requests are serialized
        (one at a time) until ``cf_clearance`` is found in the browser
        context.  After that the gate opens and requests proceed in parallel.
        """
        spider = spider or self._crawler.spider

        if not self._cf_gate.is_set():
            async with self._cf_serial_lock:
                # Re-check: another request may have opened the gate while
                # we were waiting for the lock.
                if not self._cf_gate.is_set():
                    logger.debug(
                        "[CfGate] Serialized request through: %s %s",
                        request.method, request.url,
                    )
                    response = await self._do_download(request, spider)
                    await self._maybe_open_cf_gate()
                    return response

        return await self._do_download(request, spider)

    async def _maybe_open_cf_gate(self) -> None:
        """Open the serialization gate if any context has ``cf_clearance``."""
        if self._cf_gate.is_set():
            return
        for pw_ctx in self.pw.contexts.values():
            cookies = await pw_ctx.context.cookies()
            if any(c["name"] == "cf_clearance" for c in cookies):
                logger.info(
                    "[CfGate] cf_clearance found — allowing parallel requests"
                )
                self._cf_gate.set()
                return

    # ── Retry loop with driver-dead detection ─────────────────────────

    async def _do_download(self, request: Request, spider: Spider) -> Response:
        from scrapy.exceptions import IgnoreRequest

        download_mode = "fetch" if request.meta.get("playwright_fetch") else "page"
        download_start_time = time()
        self._crawler.signals.send_catch_log(
            pw_signals.download_started,
            url=request.url,
            method=request.method,
            mode=download_mode,
        )
        counter = 0
        while True:
            if self.pw.is_closing:
                raise IgnoreRequest("Playwright handler is shutting down")
            try:
                result = await self.pw.process(request=request, spider=spider)
                self._crawler.signals.send_catch_log(
                    pw_signals.download_completed,
                    url=request.url,
                    method=request.method,
                    mode=download_mode,
                    status=result.status,
                    duration=time() - download_start_time,
                )
                return result
            except Exception as ex:
                ex_str = str(ex)
                if "Connection closed while reading from the driver" in ex_str:
                    if self.pw.is_closing:
                        raise IgnoreRequest(
                            "Playwright connection closed (shutting down)"
                        ) from ex

                    self.pw.driver_dead = True
                    counter += 1
                    logger.warning(
                        "[Lifecycle] _download_request DRIVER_DEAD %s %s "
                        "retry=%d/%d",
                        request.method, request.url,
                        counter, self.config.target_closed_max_retries,
                    )
                    if counter > self.config.target_closed_max_retries:
                        self.pw.is_closing = True
                        self._crawler.signals.send_catch_log(
                            pw_signals.download_failed,
                            url=request.url,
                            method=request.method,
                            mode=download_mode,
                            error_type=type(ex).__name__,
                            duration=time() - download_start_time,
                        )
                        raise IgnoreRequest(
                            "Playwright driver dead and max retries reached"
                        ) from ex
                    try:
                        await self.pw.restart()
                        continue
                    except Exception as restart_ex:
                        self.pw.is_closing = True
                        raise IgnoreRequest(
                            "Playwright driver restart failed"
                        ) from restart_ex

                if isinstance(ex, TargetClosedError) or _is_safe_close_error(ex):
                    counter += 1
                    if counter > self.config.target_closed_max_retries:
                        self._crawler.signals.send_catch_log(
                            pw_signals.download_failed,
                            url=request.url,
                            method=request.method,
                            mode=download_mode,
                            error_type=type(ex).__name__,
                            duration=time() - download_start_time,
                        )
                        raise TargetClosedError(
                            "Target closed and max retries reached"
                        ) from ex
                else:
                    self._crawler.signals.send_catch_log(
                        pw_signals.download_failed,
                        url=request.url,
                        method=request.method,
                        mode=download_mode,
                        error_type=type(ex).__name__,
                        duration=time() - download_start_time,
                    )
                    raise
