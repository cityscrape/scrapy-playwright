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
from scrapy_playwright.cloudflare.gate import CfGate
from scrapy_playwright.headers import use_scrapy_headers
from scrapy_playwright.playwright import (
    PlaywrightEngine,
    PlaywrightEngine as Playwright,  # noqa: F401 (back-compat re-export)
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
    page_pool_strategy: str = POOL_STRATEGY_REUSE_FIRST
    tiling_enabled: bool = True
    tiling_screen_width: Optional[int] = None
    tiling_screen_height: Optional[int] = None
    tiling_margin: int = 0
    tiling_adjust_viewport: bool = True
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
    cf_gate_cooldown_s: float = 120.0
    antibot_remediation_enabled: bool = False
    antibot_remediation_actions: Optional[list] = None
    profile_rotation_archive_dir: Optional[str] = None
    fetch_failure_artifacts_enabled: bool = False
    fetch_failure_artifacts_dir: str = "playwright_fetch_failures"

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
            page_pool_strategy=settings.get(
                "PLAYWRIGHT_PAGE_POOL_STRATEGY", POOL_STRATEGY_REUSE_FIRST
            ),
            tiling_enabled=settings.getbool("PLAYWRIGHT_TILING_ENABLED", default=True),
            tiling_screen_width=settings.getint("PLAYWRIGHT_TILING_SCREEN_WIDTH") or None,
            tiling_screen_height=settings.getint("PLAYWRIGHT_TILING_SCREEN_HEIGHT") or None,
            tiling_margin=settings.getint("PLAYWRIGHT_TILING_MARGIN", 0),
            tiling_adjust_viewport=settings.getbool("PLAYWRIGHT_TILING_ADJUST_VIEWPORT", default=True),
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
            cf_gate_cooldown_s=settings.getfloat(
                "PLAYWRIGHT_CLOUDFLARE_GATE_COOLDOWN_S", 120.0
            ),
            antibot_remediation_enabled=settings.getbool(
                "PLAYWRIGHT_ANTIBOT_REMEDIATION_ENABLED",
                default=settings.getbool("PLAYWRIGHT_CLOUDFLARE_CHALLENGE_RETRY", default=False),
            ),
            antibot_remediation_actions=settings.getlist(
                "PLAYWRIGHT_ANTIBOT_REMEDIATION_ACTIONS",
                ["profile_rotation"],
            ),
            profile_rotation_archive_dir=settings.get("PLAYWRIGHT_PROFILE_ROTATION_ARCHIVE_DIR"),
            fetch_failure_artifacts_enabled=settings.getbool(
                "PLAYWRIGHT_FETCH_FAILURE_ARTIFACTS_ENABLED", default=False
            ),
            fetch_failure_artifacts_dir=settings.get(
                "PLAYWRIGHT_FETCH_FAILURE_ARTIFACTS_DIR",
                os.path.join(
                    settings.get("OUTPUT_DIRECTORY", "output"),
                    "playwright_fetch_failures",
                ),
            ),
        )
        cfg.cdp_kwargs.pop("endpoint_url", None)
        cfg.connect_kwargs.pop("ws_endpoint", None)
        if cfg.browser_type_name not in VALID_BROWSER_TYPES:
            raise NotSupported(
                f"Invalid PLAYWRIGHT_BROWSER_TYPE: {cfg.browser_type_name!r}. "
                f"Valid values: {', '.join(sorted(VALID_BROWSER_TYPES))}"
            )
        
        if "PLAYWRIGHT_PAGE_POOLING" in settings:
            logger.warning(
                "PLAYWRIGHT_PAGE_POOLING is deprecated and ignored. "
                "Page pooling is always active. Use PLAYWRIGHT_CLOSE_PAGE_AFTER_REQUEST "
                "to control page lifecycle."
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


def _context_name_from_session(session_key: Optional[str]) -> Optional[str]:
    """Map a readiness ``session`` value to a Playwright context name.

    Accepts ``"browser:<name>"`` (the readiness convention) or a bare context
    name. Returns ``None`` when no session was declared.
    """
    if not session_key:
        return None
    if session_key.startswith("browser:"):
        return session_key.split(":", 1)[1] or DEFAULT_CONTEXT_NAME
    return session_key


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
            pw = PlaywrightEngine(
                browser_type_name=self.config.browser_type_name,
                launch_options=self.config.launch_options,
                camoufox_options=self.config.camoufox_options,
                max_pages_per_context=self.config.max_pages_per_context,
                navigation_timeout=self.config.navigation_timeout,
                close_page_after_request=self.config.close_page_after_request,
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
                antibot_remediation_enabled=self.config.antibot_remediation_enabled,
                antibot_remediation_actions=self.config.antibot_remediation_actions,
                profile_rotation_archive_dir=self.config.profile_rotation_archive_dir,
                fetch_failure_artifacts_enabled=self.config.fetch_failure_artifacts_enabled,
                fetch_failure_artifacts_dir=pathlib.Path(self.config.fetch_failure_artifacts_dir),
                signal_dispatcher=self._emit_signal,
            )
            pw.on_stats_inc = self.stats.inc_value
            pw.on_stats_set = self.stats.set_value

            cf_gate = CfGate(
                enabled=self.config.cf_challenge_retry,
                emit_signal=self._emit_signal,
            )
            # Let the engine's bypass re-close the gate on re-challenge.
            pw._cf_gate = cf_gate

            shared = {
                "pw": pw,
                "cf_gate": cf_gate,
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
        self._cf_gate: CfGate = shared["cf_gate"]

        # Expose Playwright runtime handles to Scrapy's readiness framework so
        # readiness checks can inspect the live browser context/page for the
        # session a parked request is waiting on.
        crawler.readiness_runtime_provider = self._readiness_runtime_provider

    def _readiness_runtime_provider(self, session_key, request=None) -> dict:
        """Resolve the Playwright context/page handles for a readiness session.

        ``session_key`` follows the readiness convention ``"browser:<name>"``;
        a bare context name is also accepted. When the session-derived name
        does not match a live context (e.g. the readiness config names
        ``default`` but the project runs a custom-named context), this falls
        back to the request's ``playwright_context`` meta, the configured
        ``PLAYWRIGHT_DEFAULT_CONTEXT``, and finally the sole live context.

        Returns an empty dict while no context has been created yet, so
        readiness checks simply stay not-ready until the session is warmed.
        """
        contexts = self.pw.contexts
        context_name = _context_name_from_session(session_key)
        if context_name not in contexts:
            fallbacks = []
            if request is not None:
                fallbacks.append(request.meta.get("playwright_context"))
            fallbacks.append(self._crawler.settings.get("PLAYWRIGHT_DEFAULT_CONTEXT"))
            fallbacks.append(DEFAULT_CONTEXT_NAME)
            context_name = next(
                (name for name in fallbacks if name and name in contexts),
                # As a last resort, if exactly one context exists, use it.
                next(iter(contexts)) if len(contexts) == 1 else None,
            )
        pw_ctx = contexts.get(context_name) if context_name else None
        if pw_ctx is None:
            return {}
        browser_context = pw_ctx.context
        pages = list(getattr(browser_context, "pages", []) or [])
        return {"context": browser_context, "page": pages[0] if pages else None}

    def _spider_closed(self, spider: Spider, reason: str) -> None:
        logger.info("[Lifecycle] _spider_closed reason=%s", reason)
        self.pw.is_closing = True

    def _emit_signal(self, signal, **kwargs) -> None:
        self._crawler.signals.send_catch_log(signal, **kwargs)

    @classmethod
    def from_crawler(cls: Type[PlaywrightHandler], crawler: Crawler) -> PlaywrightHandler:
        return cls(crawler)

    @classmethod
    def shared_engine(cls, crawler: Crawler) -> Optional[PlaywrightEngine]:
        """Return the crawler's shared Playwright engine, if launched."""
        shared = cls._shared_by_crawler_id.get(id(crawler))
        return shared["pw"] if shared else None

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
            logger.info(
                "[Lifecycle] Spider startup spider=%s version=%s browser_type=%s crawler_id=%s",
                getattr(self._crawler.spider, "name", "unknown"),
                self._crawler.settings.get("VERSION") or "unknown",
                self.config.browser_type_name,
                self._crawler_id,
            )
            try:
                await self.pw.launch(
                    startup_context_kwargs=self.config.startup_context_kwargs or None,
                )
            except Exception:
                # Without a browser the spider can only idle browserless and
                # then "succeed" with zero items (this shipped once: camoufox
                # baked out of the image → InvalidAddonPath swallowed by the
                # signal dispatcher, Argo workflow green). Exit non-zero so
                # the pod/workflow fails visibly.
                # ponytail: hard exit, no retry — a broken browser install
                # never heals within the pod's lifetime.
                logger.critical(
                    "[Lifecycle] Playwright facade failed to launch; "
                    "terminating (crawler_id=%s)",
                    self._crawler_id,
                    exc_info=True,
                )
                os._exit(78)  # EX_CONFIG: unrecoverable image/runtime misconfiguration
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
        (one at a time) until an unchallenged response is observed. After
        that the gate opens and requests proceed in parallel.
        """
        spider = spider or self._crawler.spider

        # Count traffic served under the current clearance (reported when the
        # gate re-arms) so experiments can read off the request budget that a
        # clearance sustained before Cloudflare re-flagged the session.
        self._cf_gate.note_request()

        if not self._cf_gate.is_open:
            # Watchdog: a persistent challenge with no solve path (private-token
            # being the prime case) keeps the gate latched closed and serializes
            # every request at ~0 throughput. After a cooldown of continuous
            # closure, force the gate open so traffic re-probes (at the reduced
            # concurrency configured for the CF context) instead of collapsing to
            # zero. Force-resetting mid-solve is safe: parallel requests park
            # behind the active solve via ``wait_if_solving``. Set the cooldown to
            # 0 to disable.
            cooldown = self.config.cf_gate_cooldown_s
            if cooldown > 0 and self._cf_gate.cooldown_elapsed(cooldown):
                if self._cf_gate.force_reset(reason="watchdog_cooldown"):
                    self.stats.inc_value(
                        "playwright/cloudflare/gate_watchdog_reset_count"
                    )
                    return await self._do_download(request, spider)

            async with self._cf_gate.serial_lock:
                if not self._cf_gate.is_open:
                    logger.debug(
                        "[CfGate] Serialized request through: %s %s",
                        request.method, request.url,
                    )
                    response = await self._do_download(request, spider)
                    await self._cf_gate.maybe_open(
                        response,
                        url=request.url,
                        method=request.method,
                        mode="fetch" if request.meta.get("playwright_fetch") else "page",
                        context_name=request.meta.get("playwright_context", "default"),
                    )
                    return response

        return await self._do_download(request, spider)

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
