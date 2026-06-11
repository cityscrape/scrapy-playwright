"""Cloudflare challenge bypass — detection, solve, validation, and Turnstile interaction.

One instance per ``PlaywrightEngine``.  The engine delegates challenge
handling here after every response when CF retry is enabled.
"""

import asyncio
import logging
import random
from contextlib import suppress
from time import time
from typing import TYPE_CHECKING, Dict, Optional
from urllib.parse import urlparse

from playwright.async_api import Page, Response as PlaywrightResponse
from scrapy import Spider
from scrapy.http import Request, Response

from scrapy_playwright.cloudflare.types import (
    ChallengeState,
    ChallengeType,
    ContextChallengeInfo,
    CF_TITLE_MARKERS,
    body_has_challenge_markers,
    classify_challenge,
    has_cf_clearance,
    resolve_first_party_url,
)
from scrapy_playwright.cloudflare.remediation import RemediationInput
from scrapy_playwright.cloudflare.diagnostics import CloudflareDiagnostics
from scrapy_playwright import signals as pw_signals

if TYPE_CHECKING:
    from scrapy_playwright.playwright import PlaywrightContext, PlaywrightEngine

logger = logging.getLogger("scrapy-playwright")


class CloudflareBypass:
    """Owns challenge detection, wait/solve/retry flow, and related helpers.

    Does **not** own browser launch logic or know about the Scrapy handler
    lifecycle.  It receives a reference to the engine for context/page
    access and stats.
    """

    def __init__(
        self,
        *,
        engine: "PlaywrightEngine",
        max_retries: int = 3,
        seed_url: Optional[str] = None,
        seed_timeout: int = 90_000,
        wait_timeout: int = 60_000,
    ) -> None:
        self._engine = engine
        self._max_retries = max_retries
        self._seed_url = seed_url
        self._seed_timeout = seed_timeout
        self._wait_timeout = wait_timeout
        self._challenge_info: Dict[str, ContextChallengeInfo] = {}
        self._solve_lock = asyncio.Lock()
        self._diagnostics = CloudflareDiagnostics(engine)

    @staticmethod
    def _mode_for_request(request: Request) -> str:
        return "fetch" if request.meta.get("playwright_fetch") else "page"

    @staticmethod
    def _header_to_str(value) -> Optional[str]:
        if value is None:
            return None
        if isinstance(value, list):
            value = value[0] if value else None
        if isinstance(value, bytes):
            return value.decode("utf-8", errors="replace")
        return str(value)

    def _event_attrs(
        self,
        request: Request,
        *,
        context_name: str,
        challenge_type: Optional[ChallengeType] = None,
        attempt_count: Optional[int] = None,
        response: Optional[Response] = None,
        blocked_reason: Optional[str] = None,
        outcome: Optional[str] = None,
        duration: Optional[float] = None,
        error_type: Optional[str] = None,
        probe_url: Optional[str] = None,
        validated_url: Optional[str] = None,
    ) -> dict:
        attrs = {
            "request": request,
            "url": request.url,
            "method": request.method,
            "mode": self._mode_for_request(request),
            "context_name": context_name,
        }
        if challenge_type is not None:
            attrs["challenge_type"] = challenge_type.value
        if attempt_count is not None:
            attrs["attempt_count"] = attempt_count
        if response is not None:
            attrs["status"] = response.status
            attrs["resp_url"] = response.url
            attrs["cf_mitigated"] = self._header_to_str(
                response.headers.get(b"Cf-Mitigated"),
            )
        if blocked_reason is not None:
            attrs["blocked_reason"] = blocked_reason
        if outcome is not None:
            attrs["outcome"] = outcome
        if duration is not None:
            attrs["duration"] = duration
        if error_type is not None:
            attrs["error_type"] = error_type
        if probe_url is not None:
            attrs["probe_url"] = probe_url
        if validated_url is not None:
            attrs["validated_url"] = validated_url
        return attrs

    async def _emit_blocked_and_remediate(self, **attrs) -> None:
        self._engine._emit_signal(pw_signals.playwright_blocked, **attrs)
        await self._engine.remediate_antibot_block(
            RemediationInput(
                request=attrs.get("request"),
                context_name=attrs.get("context_name", "default"),
                mode=attrs.get("mode", "page"),
                challenge_type=attrs.get("challenge_type"),
                blocked_reason=attrs.get("blocked_reason"),
                status=attrs.get("status"),
                resp_url=attrs.get("resp_url"),
                attempt_count=attrs.get("attempt_count"),
                error_type=attrs.get("error_type"),
            )
        )

    # ── Public API used by the engine ──────────────────────────────────

    async def wait_if_solving(self, context_name: str, request: Request) -> None:
        """If a solve cycle is active for *context_name*, park the request."""
        info = self._get_info(context_name)
        if info.state == ChallengeState.SOLVING:
            await self._wait_for_solve(info, request)

    async def handle_if_challenged(
        self,
        request: Request,
        response: Response,
        spider: Spider,
        context_name: str,
        dispatch_fn,
    ) -> Response:
        """Classify *response* and trigger a solve cycle if challenged.

        *dispatch_fn* is an async callable ``(request, spider) -> Response``
        used to retry the original request after solving.
        """
        challenge_type = classify_challenge(response)
        if challenge_type == ChallengeType.NONE:
            return response

        # A challenge after the gate already opened means Cloudflare
        # re-escalated mid-run. Re-close the gate so subsequent requests
        # serialize through a solve cycle instead of racing the carrier page.
        cf_gate = getattr(self._engine, "_cf_gate", None)
        if cf_gate is not None:
            cf_gate.rearm(
                **self._event_attrs(
                    request,
                    context_name=context_name,
                    challenge_type=challenge_type,
                    response=response,
                ),
            )

        if challenge_type == ChallengeType.PLAIN_403:
            await self._emit_blocked_and_remediate(
                **self._event_attrs(
                    request,
                    context_name=context_name,
                    challenge_type=challenge_type,
                    response=response,
                    blocked_reason="cloudflare_plain_403",
                ),
            )
            return response
        if challenge_type == ChallengeType.PRIVATE_TOKEN:
            self._engine._emit_signal(
                pw_signals.cloudflare_challenge_detected,
                **self._event_attrs(
                    request,
                    context_name=context_name,
                    challenge_type=challenge_type,
                    response=response,
                ),
            )
            await self._emit_blocked_and_remediate(
                **self._event_attrs(
                    request,
                    context_name=context_name,
                    challenge_type=challenge_type,
                    response=response,
                    blocked_reason="private_token_challenge",
                ),
            )
            return response
        self._engine._emit_signal(
            pw_signals.cloudflare_challenge_detected,
            **self._event_attrs(
                request,
                context_name=context_name,
                challenge_type=challenge_type,
                response=response,
            ),
        )
        self._diagnostics.log_response_summary(
            response, label="challenge_detected", request=request,
        )
        return await self._handle(
            request, response, spider, context_name, challenge_type, dispatch_fn,
        )

    # ── Per-context state helpers ──────────────────────────────────────

    def _get_info(self, context_name: str) -> ContextChallengeInfo:
        if context_name not in self._challenge_info:
            self._challenge_info[context_name] = ContextChallengeInfo()
        return self._challenge_info[context_name]

    async def _wait_for_solve(
        self, info: ContextChallengeInfo, request: Request,
    ) -> None:
        if info.solve_event is None:
            return
        logger.info(
            "[Cloudflare] %s %s → parking behind active solve cycle",
            request.method, request.url,
        )
        self._engine._inc_stat("playwright/cloudflare/parked_count")
        parked_start = time()
        await info.solve_event.wait()
        self._engine._emit_signal(
            pw_signals.cloudflare_request_parked,
            **self._event_attrs(
                request,
                context_name=request.meta.get("playwright_context", "default"),
                duration=time() - parked_start,
            ),
        )

    # ── Reactive challenge handling ────────────────────────────────────

    async def _handle(
        self,
        request: Request,
        response: Response,
        spider: Spider,
        context_name: str,
        challenge_type: ChallengeType,
        dispatch_fn,
    ) -> Response:
        info = self._get_info(context_name)
        solver_marker = object()
        is_solver = False

        async with self._solve_lock:
            if info.state == ChallengeState.SOLVING:
                logger.info(
                    "[Cloudflare] %s %s joined existing solve cycle "
                    "(context='%s', attempt=%d, type=%s)",
                    request.method, request.url, context_name,
                    info.attempt_count, info.challenge_type.value,
                )
            else:
                info.state = ChallengeState.SOLVING
                info.challenge_type = challenge_type
                info.attempt_count += 1
                info.solve_event = asyncio.Event()
                info.error = None
                info.solver_marker = solver_marker
                is_solver = True

        if not is_solver:
            await self._wait_for_solve(info, request)
            if info.state == ChallengeState.VALIDATED:
                logger.info(
                    "[Cloudflare] Solve cycle finished validated; retrying parked request "
                    "%s %s (context='%s')",
                    request.method, request.url, context_name,
                )
                return await dispatch_fn(request, spider)
            logger.warning(
                "[Cloudflare] Solve cycle finished without validation; "
                "returning original challenged response for %s %s (context='%s')",
                request.method, request.url, context_name,
            )
            return response

        if info.solve_event and info.solve_event.is_set():
            if info.state == ChallengeState.VALIDATED:
                return await dispatch_fn(request, spider)
            return response

        if info.attempt_count > self._max_retries:
            logger.warning(
                "[Cloudflare] Max attempts (%d) for context '%s' — giving up",
                self._max_retries, context_name,
            )
            self._engine._inc_stat("playwright/cloudflare/max_retries_count")
            info.state = ChallengeState.FAILED
            if info.solve_event:
                info.solve_event.set()
            await self._emit_blocked_and_remediate(
                **self._event_attrs(
                    request,
                    context_name=context_name,
                    challenge_type=challenge_type,
                    response=response,
                    attempt_count=info.attempt_count,
                    blocked_reason="cloudflare_max_retries",
                ),
            )
            return response

        logger.info(
            "[Cloudflare] Challenge %s on %s %s "
            "(context='%s', attempt %d/%d)",
            challenge_type.value, request.method, request.url,
            context_name, info.attempt_count, self._max_retries,
        )
        self._engine._inc_stat("playwright/cloudflare/challenge_count")
        self._engine._inc_stat(f"playwright/cloudflare/challenge_type/{challenge_type.value}")
        self._engine._emit_signal(
            pw_signals.cloudflare_solve_started,
            **self._event_attrs(
                request,
                context_name=context_name,
                challenge_type=challenge_type,
                attempt_count=info.attempt_count,
            ),
        )

        if challenge_type == ChallengeType.TURNSTILE:
            self._diagnostics.log_turnstile_diagnostics(response)

        solve_started_at = time()
        try:
            solved = await self._solve(request, spider, context_name, info)
            if solved:
                info.state = ChallengeState.VALIDATED
                logger.info(
                    "[Cloudflare] Solve SUCCESS for context '%s' "
                    "(type=%s, attempts=%d, validated_url=%s)",
                    context_name, info.challenge_type.value,
                    info.attempt_count, info.last_validated_url,
                )
                self._engine._inc_stat("playwright/cloudflare/solve_success_count")
                self._engine._emit_signal(
                    pw_signals.cloudflare_solve_completed,
                    **self._event_attrs(
                        request,
                        context_name=context_name,
                        challenge_type=info.challenge_type,
                        attempt_count=info.attempt_count,
                        duration=time() - solve_started_at,
                        outcome="validated",
                        validated_url=info.last_validated_url,
                    ),
                )
            else:
                info.state = ChallengeState.FAILED
                logger.warning(
                    "[Cloudflare] Solve FAILED for context '%s' "
                    "(type=%s, attempts=%d)",
                    context_name, info.challenge_type.value,
                    info.attempt_count,
                )
                self._engine._inc_stat("playwright/cloudflare/solve_failed_count")
                self._engine._emit_signal(
                    pw_signals.cloudflare_solve_failed,
                    **self._event_attrs(
                        request,
                        context_name=context_name,
                        challenge_type=info.challenge_type,
                        attempt_count=info.attempt_count,
                        duration=time() - solve_started_at,
                        outcome="solve_failed",
                    ),
                )
        except Exception as exc:
            info.state = ChallengeState.FAILED
            info.error = exc
            logger.error(
                "[Cloudflare] Solve ERROR for context '%s': %s",
                context_name, exc, exc_info=True,
            )
            self._engine._inc_stat("playwright/cloudflare/solve_failed_count")
            self._engine._emit_signal(
                pw_signals.cloudflare_solve_failed,
                **self._event_attrs(
                    request,
                    context_name=context_name,
                    challenge_type=info.challenge_type,
                    attempt_count=info.attempt_count,
                    duration=time() - solve_started_at,
                    outcome="solve_error",
                    error_type=type(exc).__name__,
                ),
            )
        finally:
            info.solver_marker = None
            if info.solve_event:
                info.solve_event.set()

        if info.state == ChallengeState.VALIDATED:
            await asyncio.sleep(1.0 + (0.5 * info.attempt_count))
            return await dispatch_fn(request, spider)
        await self._emit_blocked_and_remediate(
            **self._event_attrs(
                request,
                context_name=context_name,
                challenge_type=info.challenge_type,
                response=response,
                attempt_count=info.attempt_count,
                blocked_reason=(
                    "cloudflare_solve_error"
                    if info.error is not None
                    else "cloudflare_solve_failed"
                ),
                error_type=type(info.error).__name__ if info.error else None,
            ),
        )
        return response

    # ── Solve cycle ────────────────────────────────────────────────────

    async def _solve(
        self,
        request: Request,
        spider: Spider,
        context_name: str,
        info: ContextChallengeInfo,
    ) -> bool:
        pw_ctx = self._engine.contexts.get(context_name)
        if pw_ctx is None:
            pw_ctx = await self._engine.get_or_create_context(request, spider)

        page = info.carrier_page
        page_is_new = False
        if page is None or page.is_closed():
            existing = await pw_ctx.get_open_page()
            if existing is not None:
                page = existing
            else:
                page, page_is_new = await pw_ctx.acquire_page()
                if page_is_new:
                    self._engine._inc_stat("playwright/page_count")
                    page.on("close", self._engine._make_close_page_cb(pw_ctx.name, page))
                    page.on("crash", self._engine._make_close_page_cb(pw_ctx.name, page))
            info.carrier_page = page

        solve_url = resolve_first_party_url(request, self._seed_url)
        logger.info(
            "[Cloudflare] Navigating carrier page → %s (challenge=%s, context='%s')",
            solve_url, info.challenge_type.value, context_name,
        )
        await self._diagnostics.log_page_snapshot(
            page, pw_ctx, "pre_solve_snapshot", request=request,
        )
        self._diagnostics.log_context_pages(pw_ctx, "pre_solve_pages")

        try:
            page_title = await page.title()
            title_lower = (page_title or "").lower()
            already_on_challenge = any(m in title_lower for m in CF_TITLE_MARKERS)

            if already_on_challenge:
                logger.info(
                    "[Cloudflare] Page already on challenge ('%s') — skipping navigation",
                    page_title,
                )
                has_clearance = await self._poll_clearance(
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
                    clicked = await self._click_turnstile(page)
                    logger.info(
                        "[Cloudflare] Turnstile click result on existing challenge page: %s",
                        clicked,
                    )
                    handoff = await self._wait_for_handoff(
                        page, pw_ctx, request, timeout_ms=self._wait_timeout,
                    )
                    has_clearance = handoff["has_clearance"]
                    if not has_clearance and not handoff["handoff_completed"]:
                        has_clearance = await self._poll_clearance(
                            pw_ctx, timeout_ms=self._wait_timeout,
                        )
            else:
                await page.goto(
                    solve_url,
                    wait_until="domcontentloaded",
                    timeout=self._seed_timeout,
                )

                await asyncio.sleep(2.0)
                await self._diagnostics.log_page_snapshot(
                    page, pw_ctx, "post_goto_snapshot", request=request,
                )

                clicked = await self._click_turnstile(page)
                logger.info(
                    "[Cloudflare] Turnstile click result after navigation: %s",
                    clicked,
                )

                handoff = await self._wait_for_handoff(
                    page, pw_ctx, request, timeout_ms=self._wait_timeout,
                )
                has_clearance = handoff["has_clearance"]
                if not has_clearance and not handoff["handoff_completed"]:
                    has_clearance = await self._poll_clearance(
                        pw_ctx, timeout_ms=self._wait_timeout,
                    )
                if not has_clearance:
                    logger.warning("[Cloudflare] cf_clearance not found after first wait")

                    clicked = await self._click_turnstile(page)
                    if clicked:
                        handoff = await self._wait_for_handoff(
                            page, pw_ctx, request, timeout_ms=self._wait_timeout,
                        )
                        has_clearance = handoff["has_clearance"]
                        if not has_clearance and not handoff["handoff_completed"]:
                            has_clearance = await self._poll_clearance(
                                pw_ctx, timeout_ms=self._wait_timeout,
                            )

            if not has_clearance:
                body = await page.content()
                if body_has_challenge_markers(body):
                    logger.warning(
                        "[Cloudflare] Page still shows challenge markers after wait",
                    )
                    self._diagnostics.dump_challenge_html(body, context_name, info)
                    return False
                logger.info(
                    "[Cloudflare] Page no longer shows challenge markers — "
                    "proceeding to validation despite missing cookie",
                )

            await self._diagnostics.log_context_cookies(pw_ctx, "post_solve_cookie_snapshot")
            await self._diagnostics.log_page_snapshot(
                page, pw_ctx, "post_solve_snapshot", request=request,
            )

            return await self._validate(pw_ctx, page, request, info)

        except Exception as exc:
            logger.error(
                "[Cloudflare] Solve navigation failed: %s", exc, exc_info=True,
            )
            return False
        finally:
            if page_is_new and not page.is_closed():
                pw_ctx.return_page(page)

    # ── Clearance validation ───────────────────────────────────────────

    async def _validate(
        self,
        pw_ctx: "PlaywrightContext",
        page: Page,
        request: Request,
        info: ContextChallengeInfo,
    ) -> bool:
        probe_url = resolve_first_party_url(request, self._seed_url, for_fetch=True)
        logger.info("[Cloudflare] Validation probe → %s", probe_url)
        validation_started_at = time()
        self._engine._emit_signal(
            pw_signals.cloudflare_validation_started,
            **self._event_attrs(
                request,
                context_name=pw_ctx.name,
                challenge_type=info.challenge_type,
                attempt_count=info.attempt_count,
                probe_url=probe_url,
            ),
        )

        try:
            await self._diagnostics.log_page_snapshot(
                page, pw_ctx, "pre_validation_snapshot", request=request,
            )
            current_url = page.url
            current_host = urlparse(current_url).netloc if current_url else ""
            probe_host = urlparse(probe_url).netloc
            if current_host == probe_host:
                body = await page.content()
                if not body_has_challenge_markers(body):
                    logger.info(
                        "[Cloudflare] Validation succeeded on current page without probe "
                        "(url=%s title=%r)",
                        current_url,
                        await page.title(),
                    )
                    await self._diagnostics.log_context_cookies(
                        pw_ctx, "validation_cookie_snapshot",
                    )
                    await self._diagnostics.log_page_snapshot(
                        page, pw_ctx, "post_validation_snapshot", request=request,
                    )
                    info.last_validated_url = current_url
                    self._engine._set_stat(
                        "playwright/cloudflare/last_validated_url", current_url,
                    )
                    self._engine._emit_signal(
                        pw_signals.cloudflare_validation_completed,
                        **self._event_attrs(
                            request,
                            context_name=pw_ctx.name,
                            challenge_type=info.challenge_type,
                            attempt_count=info.attempt_count,
                            probe_url=probe_url,
                            duration=time() - validation_started_at,
                            validated_url=current_url,
                        ),
                    )
                    return True
            probe_resp = await page.goto(
                probe_url,
                wait_until="domcontentloaded",
                timeout=30_000,
            )
            if probe_resp is None:
                logger.warning("[Cloudflare] Validation probe returned None")
                self._engine._emit_signal(
                    pw_signals.cloudflare_validation_failed,
                    **self._event_attrs(
                        request,
                        context_name=pw_ctx.name,
                        challenge_type=info.challenge_type,
                        attempt_count=info.attempt_count,
                        probe_url=probe_url,
                        duration=time() - validation_started_at,
                        outcome="probe_none",
                    ),
                )
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
            await self._diagnostics.log_context_cookies(pw_ctx, "validation_cookie_snapshot")

            if status in (403, 429, 503) and cf_mitigated.lower() == "challenge":
                logger.warning(
                    "[Cloudflare] Validation probe still challenged (status=%d)",
                    status,
                )
                self._engine._emit_signal(
                    pw_signals.cloudflare_validation_failed,
                    **self._event_attrs(
                        request,
                        context_name=pw_ctx.name,
                        challenge_type=info.challenge_type,
                        attempt_count=info.attempt_count,
                        probe_url=probe_url,
                        duration=time() - validation_started_at,
                        outcome="challenge",
                    ),
                )
                return False

            body = await page.content()
            if body_has_challenge_markers(body):
                logger.warning(
                    "[Cloudflare] Validation probe body has challenge markers",
                )
                self._diagnostics.dump_challenge_html(body, pw_ctx.name, info)
                self._engine._emit_signal(
                    pw_signals.cloudflare_validation_failed,
                    **self._event_attrs(
                        request,
                        context_name=pw_ctx.name,
                        challenge_type=info.challenge_type,
                        attempt_count=info.attempt_count,
                        probe_url=probe_url,
                        duration=time() - validation_started_at,
                        outcome="body_markers",
                    ),
                )
                return False

            await self._diagnostics.log_page_snapshot(
                page, pw_ctx, "post_validation_snapshot", request=request,
            )
            info.last_validated_url = probe_url
            self._engine._set_stat(
                "playwright/cloudflare/last_validated_url", probe_url,
            )
            self._engine._emit_signal(
                pw_signals.cloudflare_validation_completed,
                **self._event_attrs(
                    request,
                    context_name=pw_ctx.name,
                    challenge_type=info.challenge_type,
                    attempt_count=info.attempt_count,
                    probe_url=probe_url,
                    duration=time() - validation_started_at,
                    validated_url=probe_url,
                ),
            )
            return True

        except Exception as exc:
            logger.error(
                "[Cloudflare] Validation probe failed: %s", exc, exc_info=True,
            )
            self._engine._emit_signal(
                pw_signals.cloudflare_validation_failed,
                **self._event_attrs(
                    request,
                    context_name=pw_ctx.name,
                    challenge_type=info.challenge_type,
                    attempt_count=info.attempt_count,
                    probe_url=probe_url,
                    duration=time() - validation_started_at,
                    outcome="error",
                    error_type=type(exc).__name__,
                ),
            )
            return False

    # ── Handoff wait ───────────────────────────────────────────────────

    async def _wait_for_handoff(
        self,
        page: Page,
        pw_ctx: "PlaywrightContext",
        request: Request,
        timeout_ms: int = 60_000,
        poll_interval: float = 1.0,
    ) -> dict:
        """Wait for Cloudflare's post-challenge handoff."""
        target_host = urlparse(
            resolve_first_party_url(request, self._seed_url, for_fetch=True)
        ).netloc
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
                clearance = has_cf_clearance(cookies)
                current_url = page.url
                current_host = urlparse(current_url).netloc if current_url else ""
                body = None

                if current_host == target_host:
                    body = await page.content()
                    if not body_has_challenge_markers(body):
                        handoff_completed = True

                if not handoff_completed:
                    status = last_document["status"]
                    if (
                        last_document["url"]
                        and status is not None
                        and status < 400
                        and current_host == target_host
                        and body is not None
                        and not body_has_challenge_markers(body)
                    ):
                        handoff_completed = True

                if poll_count == 1 or poll_count % 3 == 0 or clearance or handoff_completed:
                    logger.info(
                        "[Cloudflare] Handoff poll #%d: current_url=%s clearance=%s "
                        "handoff_completed=%s last_document=%s %s %s",
                        poll_count,
                        current_url,
                        clearance,
                        handoff_completed,
                        last_document["method"] or "-",
                        last_document["status"] if last_document["status"] is not None else "-",
                        last_document["url"] or "-",
                    )

                if clearance or handoff_completed:
                    return {
                        "has_clearance": clearance,
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

    # ── Turnstile interaction ──────────────────────────────────────────

    @staticmethod
    async def _poll_clearance(
        pw_ctx: "PlaywrightContext",
        timeout_ms: int = 60_000,
        poll_interval: float = 1.0,
    ) -> bool:
        """Poll the browser context's cookie jar for ``cf_clearance``."""
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
            if has_cf_clearance(cookies):
                return True
            await asyncio.sleep(poll_interval)
            elapsed += poll_interval
        return False

    async def _click_turnstile(self, page: Page) -> bool:
        """Locate the Turnstile iframe and click the verification checkbox."""
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

            cf_frame = None
            for _ in range(30):
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

            await asyncio.sleep(random.uniform(1.5, 3.0))

            async def _click_point(target_x: float, target_y: float, attempt: int) -> bool:
                logger.debug(
                    "[Cloudflare][TurnstileClick] mouse.click (%.1f, %.1f) attempt=%d backend=%s",
                    target_x, target_y, attempt, self._engine._browser_type_name,
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
                        self._engine._inc_stat("playwright/cloudflare/turnstile_click_count")
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
                            self._engine._inc_stat("playwright/cloudflare/turnstile_click_count")
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
