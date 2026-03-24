import asyncio
from unittest import IsolatedAsyncioTestCase
from unittest.mock import AsyncMock, MagicMock, patch

from scrapy_playwright.handler import PagePool


def _make_mock_page(closed=False):
    page = MagicMock()
    page.is_closed.return_value = closed
    page.close = AsyncMock()
    return page


def _make_mock_context():
    ctx = MagicMock()

    async def new_page():
        return _make_mock_page()

    ctx.new_page = new_page
    return ctx


class TestPagePool(IsolatedAsyncioTestCase):
    async def test_acquire_creates_new_page_when_empty(self):
        sem = asyncio.Semaphore(4)
        pool = PagePool(sem)
        ctx = _make_mock_context()

        page, is_new = await pool.acquire(ctx)
        assert is_new is True
        assert not page.is_closed()
        # Semaphore was consumed (4 -> 3)
        assert sem._value == 3

    async def test_release_and_acquire_reuses_page(self):
        sem = asyncio.Semaphore(4)
        pool = PagePool(sem)
        ctx = _make_mock_context()

        page1, _ = await pool.acquire(ctx)
        assert sem._value == 3

        pool.release(page1)
        assert pool.idle_count == 1
        # Semaphore still at 3 — page is alive and holds its slot
        assert sem._value == 3

        page2, is_new = await pool.acquire(ctx)
        assert is_new is False
        assert page2 is page1
        assert pool.idle_count == 0

    async def test_release_closed_page_is_noop(self):
        sem = asyncio.Semaphore(4)
        pool = PagePool(sem)

        closed_page = _make_mock_page(closed=True)
        pool.release(closed_page)
        assert pool.idle_count == 0

    async def test_acquire_skips_dead_pages(self):
        sem = asyncio.Semaphore(4)
        pool = PagePool(sem)
        ctx = _make_mock_context()

        # Acquire and release a page
        page1, _ = await pool.acquire(ctx)
        pool.release(page1)
        # Simulate external close
        page1.is_closed.return_value = True
        # Page is still in queue but dead

        # Next acquire should skip the dead page and create a new one
        page2, is_new = await pool.acquire(ctx)
        assert is_new is True
        assert page2 is not page1
        assert pool.idle_count == 0

    async def test_drain_closes_idle_pages(self):
        sem = asyncio.Semaphore(4)
        pool = PagePool(sem)
        ctx = _make_mock_context()

        pages = []
        for _ in range(3):
            page, _ = await pool.acquire(ctx)
            pages.append(page)

        for page in pages:
            pool.release(page)
        assert pool.idle_count == 3

        await pool.drain()
        assert pool.idle_count == 0
        for page in pages:
            page.close.assert_called_once()

    async def test_drain_skips_already_closed(self):
        sem = asyncio.Semaphore(4)
        pool = PagePool(sem)
        ctx = _make_mock_context()

        page, _ = await pool.acquire(ctx)
        pool.release(page)
        page.is_closed.return_value = True

        await pool.drain()
        page.close.assert_not_called()

    async def test_idle_count(self):
        sem = asyncio.Semaphore(4)
        pool = PagePool(sem)
        ctx = _make_mock_context()

        assert pool.idle_count == 0
        page, _ = await pool.acquire(ctx)
        assert pool.idle_count == 0
        pool.release(page)
        assert pool.idle_count == 1
        await pool.acquire(ctx)
        assert pool.idle_count == 0


class TestPagePoolCreateFirst(IsolatedAsyncioTestCase):
    """Tests for the 'create_first' strategy."""

    async def test_creates_new_pages_while_capacity_available(self):
        sem = asyncio.Semaphore(3)
        pool = PagePool(sem, strategy="create_first")
        ctx = _make_mock_context()

        # Create 3 pages, filling all slots
        pages = []
        for i in range(3):
            page, is_new = await pool.acquire(ctx)
            assert is_new is True
            pages.append(page)
        assert sem._value == 0

        # Return all to pool
        for page in pages:
            pool.release(page)
        assert pool.idle_count == 3
        # Semaphore still 0 — idle pages hold their slots
        assert sem._value == 0

    async def test_reuses_only_when_at_capacity(self):
        sem = asyncio.Semaphore(2)
        pool = PagePool(sem, strategy="create_first")
        ctx = _make_mock_context()

        # Fill both slots
        page1, _ = await pool.acquire(ctx)
        page2, _ = await pool.acquire(ctx)
        assert sem._value == 0

        # Return one
        pool.release(page1)
        assert pool.idle_count == 1

        # Next acquire: semaphore locked → must reuse idle page
        page3, is_new = await pool.acquire(ctx)
        assert is_new is False
        assert page3 is page1
        assert pool.idle_count == 0

    async def test_prefers_new_over_idle(self):
        sem = asyncio.Semaphore(3)
        pool = PagePool(sem, strategy="create_first")
        ctx = _make_mock_context()

        # Create one page, then return it
        page1, _ = await pool.acquire(ctx)
        pool.release(page1)
        assert pool.idle_count == 1
        assert sem._value == 2  # still 2 free slots

        # With capacity remaining, should create NEW even though idle exists
        page2, is_new = await pool.acquire(ctx)
        assert is_new is True
        assert page2 is not page1
        # The idle page1 is still in the queue
        assert pool.idle_count == 1
        assert sem._value == 1

    async def test_skips_dead_idle_pages(self):
        sem = asyncio.Semaphore(1)
        pool = PagePool(sem, strategy="create_first")
        ctx = _make_mock_context()

        page1, _ = await pool.acquire(ctx)
        pool.release(page1)
        # Mark it as dead
        page1.is_closed.return_value = True

        # Semaphore is locked (value=0), so it tries idle → dead → blocks on sem
        # But the dead page's close callback would have released the sem in real code.
        # Simulate that: manually release semaphore as the close callback would.
        sem.release()

        page2, is_new = await pool.acquire(ctx)
        assert is_new is True
        assert page2 is not page1

    async def test_reuse_first_default(self):
        """Verify default strategy is reuse_first."""
        sem = asyncio.Semaphore(3)
        pool = PagePool(sem)
        assert pool.strategy == "reuse_first"
        ctx = _make_mock_context()

        page1, _ = await pool.acquire(ctx)
        pool.release(page1)

        # reuse_first should return the idle page despite capacity
        page2, is_new = await pool.acquire(ctx)
        assert is_new is False
        assert page2 is page1
