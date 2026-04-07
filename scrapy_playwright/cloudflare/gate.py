"""Cloudflare pre-clearance serialization gate.

Requests pass one at a time until we observe an unchallenged response.
Cookie presence alone is not enough because stale ``cf_clearance`` values
can coexist with an active Cloudflare challenge.
"""

import asyncio
import logging
from typing import TYPE_CHECKING, Optional

from scrapy_playwright.cloudflare.types import ChallengeType, classify_challenge

if TYPE_CHECKING:
    from scrapy.http import Response

logger = logging.getLogger("scrapy-playwright")


class CfGate:
    """Pre-clearance serialization gate.

    If the gate is closed, the handler acquires the serial lock and runs
    exactly one request. After it completes, the handler calls
    :meth:`maybe_open` with the final Scrapy response. The gate opens only
    after the response is no longer classified as a Cloudflare challenge.
    """

    def __init__(self, enabled: bool) -> None:
        self._event = asyncio.Event()
        self._serial_lock = asyncio.Lock()
        if not enabled:
            # Gate starts open when CF retry is disabled.
            self._event.set()

    @property
    def is_open(self) -> bool:
        return self._event.is_set()

    @property
    def serial_lock(self) -> asyncio.Lock:
        return self._serial_lock

    def open(self) -> None:
        logger.info("[CfGate] clearance validated — allowing parallel requests")
        self._event.set()

    async def maybe_open(self, response: Optional["Response"]) -> None:
        """Open the gate if *response* is no longer challenged by Cloudflare."""
        if self._event.is_set():
            return
        if response is None:
            return

        challenge_type = classify_challenge(response)
        if challenge_type == ChallengeType.NONE:
            self.open()
            return

        logger.info(
            "[CfGate] keeping gate closed after %s response status=%s url=%s",
            challenge_type.value,
            response.status,
            response.url,
        )
