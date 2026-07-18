"""Automatic release of context-seed pages.

A request with ``playwright_context_seed: True`` warms a browser context
(cookies, Cloudflare clearance) through the normal download path. Its page is
loaned to the callback — injected as ``response.meta["playwright_page"]`` and
retained so no concurrent request can grab or navigate it — and this
middleware returns it to the context's page pool once the callback's output
has been fully consumed. Spiders never manage a seed page themselves.

This is the intrinsic seeding idiom for both spider styles:
  - navigation spiders get their pool slot back the moment the seed callback
    finishes (no starved page pool);
  - fetch spiders lazily promote the released, already-warmed page from the
    pool back into a retained carrier via ``get_or_create_fetch_page``.

Known ceiling: if a ``playwright_fetch`` re-retains the same page as its
carrier *while the seed callback is still running*, this release un-retains
the active carrier; the next fetch self-heals by re-acquiring it from the
pool under ``fetch_lock``. Only mixed navigation+fetch spiders could race
beyond that, and none exist today.
"""

import logging

from scrapy_playwright.handler import ScrapyPlaywrightDownloadHandler

logger = logging.getLogger("scrapy-playwright")


class SeedPageReleaseMiddleware:
    """Spider middleware returning ``playwright_context_seed`` pages to the pool."""

    def __init__(self, crawler):
        self._crawler = crawler

    @classmethod
    def from_crawler(cls, crawler):
        return cls(crawler)

    def process_spider_output(self, response, result, spider):
        for item in result:
            yield item
        self._release(response)

    async def process_spider_output_async(self, response, result, spider):
        async for item in result:
            yield item
        self._release(response)

    def process_spider_exception(self, response, exception, spider):
        # The callback blew up mid-iteration; reclaim the loaned page anyway.
        self._release(response)
        return None

    def _release(self, response):
        request = getattr(response, "request", None)
        if request is None or not request.meta.get("playwright_context_seed"):
            return
        page = request.meta.get("playwright_page")
        if page is None:
            return
        engine = ScrapyPlaywrightDownloadHandler.shared_engine(self._crawler)
        if engine is None:
            return
        engine.release_loaned_page(request, page)
        logger.info(
            "[Seed] Context-seed page released after callback (url=%s)",
            request.url,
        )
