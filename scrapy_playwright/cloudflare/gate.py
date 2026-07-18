"""Cloudflare pre-clearance serialization gate.

Requests pass one at a time until we observe an unchallenged response.
Cookie presence alone is not enough because stale ``cf_clearance`` values
can coexist with an active Cloudflare challenge.
"""

import asyncio
import logging
import time
from typing import TYPE_CHECKING, Callable, Optional

from scrapy_playwright.cloudflare.types import ChallengeType, classify_challenge
from scrapy_playwright import signals as pw_signals

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

    def __init__(self, enabled: bool, emit_signal: Optional[Callable] = None) -> None:
        self._enabled = enabled
        self._event = asyncio.Event()
        self._serial_lock = asyncio.Lock()
        self._emit_signal = emit_signal
        # Telemetry: how much traffic a clearance sustained before re-flagging.
        self._opened_at: Optional[float] = None
        self._requests_since_open = 0
        # When the gate is closed, the monotonic timestamp of the closure. Used
        # by the watchdog to force a re-probe after a cooldown so a persistent
        # challenge (notably private-token, which has no solve path) cannot
        # latch the gate closed forever and collapse throughput to zero.
        self._closed_at: Optional[float] = None
        if not enabled:
            # Gate starts open when CF retry is disabled.
            self._event.set()
        else:
            # Gate starts closed when CF retry is enabled.
            self._closed_at = time.monotonic()

    def note_request(self) -> None:
        """Count a download served while the gate is open.

        Called once per Playwright download by the handler. The running total
        is reported as ``requests_since_clearance`` when the gate re-arms, so
        experiments can read off how many requests a clearance survived.
        """
        if self._event.is_set():
            self._requests_since_open += 1

    @staticmethod
    def _header_to_str(value) -> Optional[str]:
        if value is None:
            return None
        if isinstance(value, list):
            value = value[0] if value else None
        if isinstance(value, bytes):
            return value.decode("utf-8", errors="replace")
        return str(value)

    @property
    def is_open(self) -> bool:
        return self._event.is_set()

    @property
    def serial_lock(self) -> asyncio.Lock:
        return self._serial_lock

    @property
    def seconds_closed(self) -> Optional[float]:
        """Seconds the gate has been continuously closed, or ``None`` if open."""
        if self._event.is_set() or self._closed_at is None:
            return None
        return time.monotonic() - self._closed_at

    def cooldown_elapsed(self, threshold_s: float) -> bool:
        """True if the gate has been closed for at least *threshold_s* seconds."""
        closed_for = self.seconds_closed
        return closed_for is not None and closed_for >= threshold_s

    def open(self, **event_attrs) -> None:
        logger.info("[CfGate] clearance validated — allowing parallel requests")
        self._event.set()
        self._opened_at = time.monotonic()
        self._requests_since_open = 0
        self._closed_at = None
        if self._emit_signal is not None:
            self._emit_signal(pw_signals.cloudflare_gate_opened, **event_attrs)

    def force_reset(self, *, reason: str = "forced", **event_attrs) -> bool:
        """Force the gate open so traffic re-probes, regardless of state.

        The gate is otherwise a latch that only opens on an unchallenged
        response (:meth:`maybe_open`). A persistent challenge with no solve
        path — private-token (PAT) being the prime case — keeps every response
        classified as a challenge, so the gate would stay closed forever and
        every request serializes at ~0 throughput. Remediation completion and
        the closed-gate watchdog call this to break that latch and let a fresh
        profile/clearance attempt run. No-op when CF retry is disabled (the gate
        is intentionally always-open in that mode) or already open.

        Returns ``True`` if the gate was actually re-opened by this call.
        """
        if not self._enabled:
            return False
        if self._event.is_set():
            return False
        seconds_closed = self.seconds_closed
        logger.warning(
            "[CfGate] force-reset (reason=%s) — re-opening gate after %s s closed "
            "to allow a re-probe",
            reason,
            f"{seconds_closed:.1f}" if seconds_closed is not None else "?",
        )
        self.open(**event_attrs)
        return True

    def rearm(self, *, reason: str = "re_challenge", **event_attrs) -> None:
        """Re-close an open gate after a fresh challenge is observed.

        The gate is a latch that opens once clearance is validated; without
        re-closing it, a later Cloudflare re-escalation (passive → interstitial
        Turnstile) lets every concurrent request keep hammering the carrier
        page. Re-closing forces subsequent requests back through the serial
        solve cycle, restoring backpressure. No-op when CF retry is disabled
        (the gate is intentionally always-open in that mode).

        Emits :data:`cloudflare_gate_rearmed` with the request/time budget the
        clearance sustained — the core measurement for aggressiveness tests.
        """
        if not self._enabled:
            return
        if not self._event.is_set():
            return
        seconds_since_clearance = (
            time.monotonic() - self._opened_at if self._opened_at is not None else None
        )
        requests_since_clearance = self._requests_since_open
        logger.warning(
            "[CfGate] re-challenge observed (reason=%s) — re-closing gate to "
            "re-serialize requests (served %d requests over %s s since clearance)",
            reason,
            requests_since_clearance,
            f"{seconds_since_clearance:.1f}" if seconds_since_clearance is not None else "?",
        )
        self._event.clear()
        self._opened_at = None
        self._requests_since_open = 0
        self._closed_at = time.monotonic()
        if self._emit_signal is not None:
            self._emit_signal(
                pw_signals.cloudflare_gate_rearmed,
                reason=reason,
                requests_since_clearance=requests_since_clearance,
                seconds_since_clearance=seconds_since_clearance,
                **event_attrs,
            )

    async def maybe_open(self, response: Optional["Response"], **event_attrs) -> None:
        """Open the gate if *response* is no longer challenged by Cloudflare."""
        if self._event.is_set():
            return
        if response is None:
            return

        challenge_type = classify_challenge(response)
        if challenge_type == ChallengeType.NONE:
            self.open(
                **event_attrs,
                challenge_type=challenge_type.value,
                status=response.status,
                resp_url=response.url,
            )
            return

        logger.info(
            "[CfGate] keeping gate closed after %s response status=%s url=%s",
            challenge_type.value,
            response.status,
            response.url,
        )
        if self._emit_signal is not None:
            self._emit_signal(
                pw_signals.cloudflare_gate_blocked,
                **event_attrs,
                challenge_type=challenge_type.value,
                status=response.status,
                resp_url=response.url,
                cf_mitigated=self._header_to_str(response.headers.get(b"Cf-Mitigated")),
            )
