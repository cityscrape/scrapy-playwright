"""Context-seed page loan semantics.

Covers the invariant that broke googlemaps distributed mode: a
``playwright_context_seed`` page is retained through the callback and MUST
come back to the page pool afterwards, or a 1-page pool starves forever.
"""

import asyncio
from types import SimpleNamespace
from unittest import IsolatedAsyncioTestCase, TestCase
from unittest.mock import AsyncMock, MagicMock, patch

from scrapy import Request

from scrapy_playwright.playwright import (
    PlaywrightContext,
    PlaywrightEngine,
    page_loaned_to_callback,
)
from scrapy_playwright.seed import SeedPageReleaseMiddleware


def _make_mock_page(closed=False):
    page = MagicMock()
    page.is_closed.return_value = closed
    page.close = AsyncMock()
    page.url = "https://www.google.com/maps"
    return page


def _make_pw_ctx(max_pages=1):
    return PlaywrightContext("cloudflare", MagicMock(), max_pages)


def _release(pw_ctx, request, page):
    engine = SimpleNamespace(
        contexts={pw_ctx.name: pw_ctx},
        _inc_stat=lambda *args, **kwargs: None,
    )
    PlaywrightEngine.release_loaned_page(engine, request, page)


def _seed_request(**extra_meta):
    meta = {"playwright_context_seed": True, "playwright_context": "cloudflare"}
    meta.update(extra_meta)
    return Request("https://www.google.com/maps", meta=meta)


class TestPageLoanedToCallback(TestCase):
    def test_flags(self):
        assert not page_loaned_to_callback(Request("https://a.example"))
        assert page_loaned_to_callback(
            Request("https://a.example", meta={"playwright_include_page": True})
        )
        assert page_loaned_to_callback(
            Request("https://a.example", meta={"playwright_context_seed": True})
        )


class TestReleaseLoanedPage(IsolatedAsyncioTestCase):
    async def test_retained_seed_page_returns_to_pool(self):
        pw_ctx = _make_pw_ctx()
        page = _make_mock_page()
        pw_ctx.retain_page(page)
        assert pw_ctx.is_retained_page(page)
        assert pw_ctx.pool.idle_count == 0

        _release(pw_ctx, _seed_request(), page)

        assert not pw_ctx.is_retained_page(page)
        assert pw_ctx.pool.idle_count == 1

    async def test_release_is_idempotent(self):
        pw_ctx = _make_pw_ctx()
        page = _make_mock_page()
        pw_ctx.retain_page(page)

        request = _seed_request()
        _release(pw_ctx, request, page)
        _release(pw_ctx, request, page)

        # Second call is a no-op: page not double-queued
        assert pw_ctx.pool.idle_count == 1

    async def test_closed_page_is_forgotten_not_pooled(self):
        pw_ctx = _make_pw_ctx()
        page = _make_mock_page()
        pw_ctx.retain_page(page)
        page.is_closed.return_value = True

        _release(pw_ctx, _seed_request(), page)

        assert not pw_ctx.is_retained_page(page)
        assert pw_ctx.pool.idle_count == 0

    async def test_unknown_context_is_noop(self):
        pw_ctx = _make_pw_ctx()
        page = _make_mock_page()
        request = Request(
            "https://a.example",
            meta={"playwright_context_seed": True, "playwright_context": "nope"},
        )
        _release(pw_ctx, request, page)  # must not raise
        assert pw_ctx.pool.idle_count == 0


class TestSeedPageReleaseMiddleware(IsolatedAsyncioTestCase):
    def _mw_and_ctx(self):
        pw_ctx = _make_pw_ctx()
        engine = SimpleNamespace(
            contexts={pw_ctx.name: pw_ctx},
            _inc_stat=lambda *args, **kwargs: None,
        )
        engine.release_loaned_page = (
            lambda request, page: PlaywrightEngine.release_loaned_page(
                engine, request, page
            )
        )
        crawler = MagicMock()
        mw = SeedPageReleaseMiddleware(crawler)
        patcher = patch(
            "scrapy_playwright.seed.ScrapyPlaywrightDownloadHandler.shared_engine",
            return_value=engine,
        )
        patcher.start()
        self.addCleanup(patcher.stop)
        return mw, pw_ctx

    def _seed_response(self, pw_ctx):
        page = _make_mock_page()
        pw_ctx.retain_page(page)
        request = _seed_request(playwright_page=page)
        response = SimpleNamespace(request=request, meta=request.meta)
        return response, page

    async def test_sync_output_releases_after_consumption(self):
        mw, pw_ctx = self._mw_and_ctx()
        response, page = self._seed_response(pw_ctx)

        consumed = list(mw.process_spider_output(response, iter(["item"]), None))
        assert consumed == ["item"]
        assert not pw_ctx.is_retained_page(page)
        assert pw_ctx.pool.idle_count == 1

    async def test_async_output_releases_after_consumption(self):
        mw, pw_ctx = self._mw_and_ctx()
        response, page = self._seed_response(pw_ctx)

        async def agen():
            yield "item"

        consumed = [
            item
            async for item in mw.process_spider_output_async(response, agen(), None)
        ]
        assert consumed == ["item"]
        assert not pw_ctx.is_retained_page(page)
        assert pw_ctx.pool.idle_count == 1

    async def test_exception_path_releases(self):
        mw, pw_ctx = self._mw_and_ctx()
        response, page = self._seed_response(pw_ctx)

        assert mw.process_spider_exception(response, RuntimeError("boom"), None) is None
        assert not pw_ctx.is_retained_page(page)
        assert pw_ctx.pool.idle_count == 1

    async def test_non_seed_request_untouched(self):
        mw, pw_ctx = self._mw_and_ctx()
        page = _make_mock_page()
        pw_ctx.retain_page(page)
        request = Request(
            "https://a.example",
            meta={
                "playwright_include_page": True,
                "playwright_context": "cloudflare",
                "playwright_page": page,
            },
        )
        response = SimpleNamespace(request=request, meta=request.meta)

        list(mw.process_spider_output(response, iter([]), None))
        # include_page ownership stays with the spider
        assert pw_ctx.is_retained_page(page)
        assert pw_ctx.pool.idle_count == 0

    async def test_pool_starvation_regression(self):
        """The googlemaps deadlock: seed holds the only slot of a 1-page pool.

        After the middleware releases the seed page, the next acquire must
        succeed without blocking.
        """
        mw, pw_ctx = self._mw_and_ctx()
        response, page = self._seed_response(pw_ctx)
        # Simulate the seed page having consumed the pool's only slot.
        await pw_ctx.pool._semaphore.acquire()

        list(mw.process_spider_output(response, iter([]), None))

        acquired, is_new = await asyncio.wait_for(
            pw_ctx.acquire_page(), timeout=1.0
        )
        assert acquired is page
        assert is_new is False
