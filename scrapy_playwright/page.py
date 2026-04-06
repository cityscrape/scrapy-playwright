import asyncio
import logging
from contextlib import suppress
from logging import Logger
from typing import Any
from typing import Callable, Dict, Tuple, Type, Union

from playwright.async_api import (
    Page,
)

__all__ = ["PageMethod"]

from playwright.async_api import BrowserContext


class PageMethod:
    """
    Represents a method to be called (and awaited if necessary) on a
    Playwright page, such as "click", "screenshot", "evaluate", etc.

    If a callable is received, it will be called with the page as its first argument.
    Any additional arguments are passed to the callable after the page.
    """

    def __init__(self, method: Union[str, Callable], *args, **kwargs) -> None:
        self.method: Union[str, Callable] = method
        self.args: tuple = args
        self.kwargs: dict = kwargs
        self.result: Any = None

    def __str__(self) -> str:
        return f"<{self.__class__.__name__} for method '{self.method}'>"

    __repr__ = __str__


# Pool strategies
POOL_STRATEGY_REUSE_FIRST = "reuse_first"
POOL_STRATEGY_CREATE_FIRST = "create_first"
VALID_POOL_STRATEGIES = {POOL_STRATEGY_REUSE_FIRST, POOL_STRATEGY_CREATE_FIRST}


class PoolStrategy:
    """Abstract base for page-pool acquisition strategies.

    Subclasses implement :meth:`acquire` to decide whether to reuse an
    idle page or create a new one.
    """

    name: str  # human-readable strategy name, set by each subclass

    async def acquire(
            self,
            idle: asyncio.Queue,
            semaphore: asyncio.Semaphore,
            context: BrowserContext,
            logger: Logger,
    ) -> Tuple[Page, bool]:
        """Return ``(page, is_new)`` from the pool.

        *is_new* is ``False`` when the page came from the idle queue.
        """
        raise NotImplementedError


class ReuseFirstStrategy(PoolStrategy):
    """Prefer recycling idle pages; only create new ones when the idle
    queue is empty."""

    name = "reuse_first"

    async def acquire(
            self,
            idle: asyncio.Queue,
            semaphore: asyncio.Semaphore,
            context: BrowserContext,
            logger: Logger,
    ) -> Tuple[Page, bool]:
        _idle_sz = idle.qsize()
        _discarded = 0

        # Drain any dead pages that were externally closed
        while not idle.empty():
            page = idle.get_nowait()
            if not page.is_closed():
                logger.debug(
                    "[PagePool:%s] ACQUIRE reuse_first → REUSED idle page "
                    "(idle_before=%d, idle_after=%d, sem_value=%d, discarded_dead=%d)",
                    self.name, _idle_sz, idle.qsize(),
                    semaphore._value, _discarded,
                )
                return page, False
            # Page died while idle — its close callback already released
            # the semaphore, so we just discard it.
            _discarded += 1

        # No reusable page — create a fresh one (acquires semaphore)
        logger.debug(
            "[PagePool:%s] ACQUIRE reuse_first → no idle pages, WAITING for semaphore "
            "(idle_before=%d, sem_value=%d, sem_locked=%s, discarded_dead=%d)",
            self.name, _idle_sz, semaphore._value,
            semaphore.locked(), _discarded,
        )
        await semaphore.acquire()
        logger.debug(
            "[PagePool:%s] ACQUIRE reuse_first → semaphore ACQUIRED, creating new page "
            "(sem_value=%d)",
            self.name, semaphore._value,
        )
        page = await context.new_page()
        return page, True


class CreateFirstStrategy(PoolStrategy):
    """Prefer creating new pages while semaphore capacity remains; only
    recycle idle pages once the max is reached."""

    name = "create_first"

    async def acquire(
            self,
            idle: asyncio.Queue,
            semaphore: asyncio.Semaphore,
            context: BrowserContext,
            logger: Logger,
    ) -> Tuple[Page, bool]:
        _sem_val = semaphore._value
        _idle_sz = idle.qsize()
        _sem_locked = semaphore.locked()

        # Create a new page while semaphore has capacity (non-blocking check)
        if not _sem_locked:
            await semaphore.acquire()
            page = await context.new_page()
            logger.debug(
                "[PagePool:%s] ACQUIRE create_first → CREATED new page "
                "(sem_was=%d, sem_now=%d, idle_available=%d)",
                self.name, _sem_val, semaphore._value, _idle_sz,
            )
            return page, True

        # At max capacity — take an idle page instead
        _discarded = 0
        while not idle.empty():
            page = idle.get_nowait()
            if not page.is_closed():
                logger.debug(
                    "[PagePool:%s] ACQUIRE create_first → sem full, REUSED idle page "
                    "(sem_value=%d, idle_before=%d, idle_after=%d, discarded_dead=%d)",
                    self.name, semaphore._value, _idle_sz,
                    idle.qsize(), _discarded,
                )
                return page, False
            _discarded += 1

        # No idle pages available — block until a slot opens
        # ⚠ THIS IS THE SUSPECTED DEADLOCK POINT: if all semaphore slots
        # are held by pages that were returned to the pool (pool.release
        # does NOT release the semaphore), we will wait here forever.
        logger.warning(
            "[PagePool:%s] ACQUIRE create_first → sem full AND idle empty, "
            "BLOCKING on semaphore.acquire() — POTENTIAL DEADLOCK "
            "(sem_value=%d, sem_locked=%s, idle_before=%d, idle_after=%d, "
            "discarded_dead=%d, context_pages=%d)",
            self.name, semaphore._value, semaphore.locked(),
            _idle_sz, idle.qsize(), _discarded,
            len(context.pages),
        )
        await semaphore.acquire()
        logger.debug(
            "[PagePool:%s] ACQUIRE create_first → semaphore UNBLOCKED after wait, "
            "creating new page (sem_value=%d, idle=%d)",
            self.name, semaphore._value, idle.qsize(),
        )
        page = await context.new_page()
        return page, True


# Maps strategy name → concrete strategy class for backward-compat construction.
_STRATEGY_REGISTRY: Dict[str, Type[PoolStrategy]] = {
    POOL_STRATEGY_REUSE_FIRST: ReuseFirstStrategy,
    POOL_STRATEGY_CREATE_FIRST: CreateFirstStrategy,
}


class PagePool:
    """Per-context async pool of reusable Playwright pages.

    Pages returned to the pool are kept alive and can be re-acquired by
    future requests instead of creating a brand-new page each time.
    The pool shares the context's existing semaphore — a page sitting idle
    in the pool still occupies a semaphore slot.

    Strategies:
      - ``reuse_first``: prefer recycling idle pages; only create new ones
        when the idle queue is empty (default).
      - ``create_first``: prefer creating new pages while semaphore capacity
        remains; only recycle idle pages once the max is reached.
    """

    def __init__(
            self,
            max_pages: int,
            strategy: str = POOL_STRATEGY_REUSE_FIRST,
            init_scripts=None
    ) -> None:
        if init_scripts is None:
            init_scripts = []
        self._idle: asyncio.Queue[Page] = asyncio.Queue()
        self._semaphore = asyncio.Semaphore(max_pages)
        self.logger = logging.getLogger(__name__)
        self.init_scripts = init_scripts

        strategy_cls = _STRATEGY_REGISTRY.get(strategy)
        if strategy_cls is None:
            raise ValueError(
                f"Unknown pool strategy {strategy!r}. "
                f"Valid values: {', '.join(sorted(_STRATEGY_REGISTRY))}"
            )
        self._strategy: PoolStrategy = strategy_cls()

    @property
    def strategy(self) -> str:
        """Return the strategy name (backward-compat with code that reads
        ``pool.strategy``)."""
        return self._strategy.name

    @property
    def idle_count(self) -> int:
        return self._idle.qsize()

    async def acquire(self, context: BrowserContext) -> Tuple[Page, bool]:
        """Return an idle page or create a new one.

        Returns ``(page, is_new)`` where *is_new* is ``False`` when the
        page came from the idle pool (a "pool hit").
        """
        tup = await self._strategy.acquire(self._idle, self._semaphore, context, self.logger, )
        (page, is_new) = tup
        if is_new:
            await self.init_page(page)
        return tup

    async def adopt(self, page: Page) -> bool:
        """Register an already-open page with the pool as an idle page.

        Persistent contexts can start with one or more tabs already open.
        Those pages consume real browser slots, so the pool must account for
        them instead of immediately creating additional tabs.
        """
        if page.is_closed():
            return False
        if self._semaphore._value <= 0:
            self.logger.warning(
                "[PagePool:%s] ADOPT → semaphore exhausted, cannot adopt page "
                "(idle=%d)",
                self.strategy, self._idle.qsize(),
            )
            return False
        await self._semaphore.acquire()
        await self.init_page(page)
        self._idle.put_nowait(page)
        self.logger.debug(
            "[PagePool:%s] ADOPT → existing page added to idle queue "
            "(sem_now=%d, idle_after=%d)",
            self.strategy, self._semaphore._value, self._idle.qsize(),
        )
        return True

    def release(self, page: Page) -> None:
        """Return a page to the pool (or release the semaphore if dead)."""
        if page.is_closed():
            # Already closed — semaphore was released by the close callback.
            self.logger.debug(
                "[PagePool:%s] RELEASE → page already closed, skipping "
                "(sem_value=%d, idle=%d)",
                self.strategy, self._semaphore._value, self._idle.qsize(),
            )
            return
        self._idle.put_nowait(page)
        self.logger.debug(
            "[PagePool:%s] RELEASE → page returned to idle queue "
            "(sem_value=%d, idle_after=%d, sem_locked=%s)",
            self.strategy, self._semaphore._value, self._idle.qsize(),
            self._semaphore.locked(),
        )

    async def drain(self) -> None:
        """Close all idle pages (for shutdown)."""
        _count = self._idle.qsize()
        _closed = 0
        while not self._idle.empty():
            page = self._idle.get_nowait()
            if not page.is_closed():
                with suppress(Exception):
                    await page.close()
                _closed += 1
        self.logger.debug(
            "[PagePool:%s] DRAIN → closed %d/%d idle pages (sem_value=%d)",
            self.strategy, _closed, _count, self._semaphore._value,
        )

    async def init_page(self, page):
        for init_script in self.init_scripts:
            await page.add_init_script(init_script)
