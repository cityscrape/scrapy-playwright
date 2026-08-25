"""Anti-bot remediation actions and telemetry."""

from __future__ import annotations

import asyncio
import logging
import os
import pathlib
import shutil
from dataclasses import dataclass
from datetime import datetime, timezone
from time import time
from typing import TYPE_CHECKING, Iterable, Optional

from scrapy_playwright import signals as pw_signals

if TYPE_CHECKING:
    from scrapy.http import Request, Response
    from scrapy_playwright.playwright import PlaywrightEngine

logger = logging.getLogger("scrapy-playwright")

PROFILE_ROTATION_ACTION = "profile_rotation"
EGRESS_DRAIN_ACTION = "egress_drain"

# Verdicts about *this browser* — a fingerprint or a warmed profile that has
# gone stale. Rotating the profile is a real answer to these.
#
# `private_token_challenge` is deliberately absent. A PAT is a verdict about the
# *address*: Cloudflare asks the device for an attestation token that no
# Camoufox build can mint, and it only asks when it already distrusts where the
# request came from. Rotating the profile relaunches on the same IP and gets
# challenged again on the next request, which is how a PAT used to burn a run in
# five seconds and report a "completed" remediation. It is handled by
# EgressDrainAction instead.
TERMINAL_ROTATION_REASONS = {
    "cloudflare_max_retries",
    "cloudflare_solve_failed",
    "cloudflare_solve_error",
    "cloudflare_plain_403",
}

PRIVATE_TOKEN_REASON = "private_token_challenge"

# Hours an address stays drained for the target that saw the PAT. A cooldown
# rather than a permanent mark: Cloudflare's reputation decays, so the IP comes
# back on its own and nothing has to un-drain it.
DEFAULT_DRAIN_COOLDOWN_HOURS = 6.0


@dataclass
class RemediationInput:
    """Normalized anti-bot terminal outcome used to select remediation."""

    request: Optional["Request"]
    context_name: str
    mode: str
    challenge_type: Optional[str] = None
    blocked_reason: Optional[str] = None
    status: Optional[int] = None
    resp_url: Optional[str] = None
    attempt_count: Optional[int] = None
    error_type: Optional[str] = None
    response: Optional["Response"] = None
    source_event: str = "playwright_blocked"


class ProfileRotationAction:
    """Archive a persistent Camoufox profile and force a clean next context."""

    name = PROFILE_ROTATION_ACTION

    async def applies_to(self, engine: "PlaywrightEngine", event: RemediationInput) -> tuple[bool, str]:
        if event.blocked_reason not in TERMINAL_ROTATION_REASONS:
            return False, "blocked_reason_not_configured"
        if not engine._uses_camoufox_backend:
            return False, "browser_not_camoufox"
        pw_ctx = engine.contexts.get(event.context_name)
        if pw_ctx is None:
            return False, "context_missing"
        if not pw_ctx.persistent:
            return False, "context_not_persistent"
        profile_dir = engine.get_context_user_data_dir(event.context_name)
        if not profile_dir:
            return False, "profile_dir_missing"
        return True, "selected"

    async def run(self, engine: "PlaywrightEngine", event: RemediationInput) -> dict:
        profile_dir = pathlib.Path(engine.get_context_user_data_dir(event.context_name))
        archive_root = pathlib.Path(engine._profile_rotation_archive_dir or profile_dir.parent / "profile_archives")
        timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
        reason = (event.blocked_reason or "blocked").replace("/", "_")
        archive_path = archive_root / f"{profile_dir.name}.{timestamp}.{reason}"

        pw_ctx = engine.contexts.get(event.context_name)
        if pw_ctx is None:
            return {"outcome": "skipped", "skip_reason": "context_missing"}

        await engine.close_context(event.context_name)
        await asyncio.sleep(0)

        archive_root.mkdir(parents=True, exist_ok=True)
        if profile_dir.exists():
            candidate = archive_path
            suffix = 1
            while candidate.exists():
                candidate = archive_root / f"{archive_path.name}.{suffix}"
                suffix += 1
            shutil.move(str(profile_dir), str(candidate))
            archive_path = candidate
        else:
            archive_path = None
        profile_dir.mkdir(parents=True, exist_ok=True)
        return {
            "outcome": "completed",
            "profile_dir": str(profile_dir),
            "archive_path": str(archive_path) if archive_path is not None else None,
        }


class EgressDrainAction:
    """Take the egress out of rotation for the target that drew a PAT.

    Scoped to one target on purpose. A portal's block is evidence about that
    portal — VivaReal cannot see the requests we send to imovelweb — so draining
    globally would throw away an address that is still good everywhere else.

    Nothing here tries to satisfy the PAT. There is no in-browser path to one,
    so the useful move is to stop spending runs on an address that has already
    been judged and to say plainly which address it was.
    """

    name = EGRESS_DRAIN_ACTION

    async def applies_to(self, engine: "PlaywrightEngine", event: RemediationInput) -> tuple[bool, str]:
        if event.blocked_reason != PRIVATE_TOKEN_REASON:
            return False, "blocked_reason_not_configured"
        if not os.getenv("SCRAPER_EGRESS_PROXY", "").strip():
            # A run pointed at a bare SCRAPER_PROXY_URL has no resource to
            # patch. Degrade quietly rather than failing the remediation.
            return False, "no_egress_resource"
        if not _drain_target():
            return False, "no_target"
        return True, "selected"

    async def run(self, engine: "PlaywrightEngine", event: RemediationInput) -> dict:
        from iwantafuckinghouse.egress import resources

        name = os.getenv("SCRAPER_EGRESS_PROXY", "").strip()
        target = _drain_target()
        cooldown = float(
            os.getenv("EGRESS_TARGET_COOLDOWN_H") or DEFAULT_DRAIN_COOLDOWN_HOURS
        )
        namespace = resources.namespace_from_env()

        # The patch is a network call to the API server; do not block the loop.
        def _drain() -> dict:
            resources.load_kube_config()
            return resources.mark_target_unhealthy(
                namespace, name, target, PRIVATE_TOKEN_REASON, cooldown
            )

        try:
            patched = await asyncio.get_running_loop().run_in_executor(None, _drain)
        except Exception as exc:
            # A drain that cannot be recorded must not look like it worked, or
            # the next run picks the same dead address straight back up.
            logger.error(
                "[Remediation] could not drain egress %s for %s: %s", name, target, exc
            )
            return {"outcome": "failed", "egress": name, "target": target, "error": str(exc)}

        observed = (patched.get("status") or {}) if isinstance(patched, dict) else {}
        spec = (patched.get("spec") or {}) if isinstance(patched, dict) else {}
        logger.error(
            "[Remediation] egress %s (%s, %s) drained for %s after a PrivateToken "
            "challenge; Cloudflare will not accept this address for that target. "
            "Back in rotation in %sh.",
            name,
            spec.get("host") or observed.get("observedIp") or "?",
            observed.get("observedAsn") or spec.get("asn") or "unknown ASN",
            target,
            cooldown,
        )
        return {
            "outcome": "completed",
            "egress": name,
            "target": target,
            "cooldown_hours": cooldown,
        }


def _drain_target() -> str:
    """The portal this run is scraping, named the way the Lease names it.

    `EGRESS_TARGET` is what claim.py leased under; PLAYWRIGHT_SPIDER_NAME is what
    a standalone run has. Preferring the former keeps the drain and the lease
    talking about the same thing.
    """
    for var in ("EGRESS_TARGET", "PLAYWRIGHT_SPIDER_NAME", "SPIDER"):
        value = os.getenv(var, "").strip()
        if value:
            return value
    return ""


class RemediationManager:
    """Select and run anti-bot remediation actions with consistent telemetry."""

    def __init__(
        self,
        *,
        engine: "PlaywrightEngine",
        enabled: bool,
        actions: Optional[Iterable[str]] = None,
    ) -> None:
        self._engine = engine
        self._enabled = enabled
        self._actions = []
        # Order is selection order: the manager runs the first action whose
        # applies_to passes, so egress_drain must precede profile_rotation or a
        # PAT would be swallowed by a rotation that cannot help it.
        for action in actions or (EGRESS_DRAIN_ACTION, PROFILE_ROTATION_ACTION):
            if action == EGRESS_DRAIN_ACTION:
                self._actions.append(EgressDrainAction())
            elif action == PROFILE_ROTATION_ACTION:
                self._actions.append(ProfileRotationAction())
            else:
                logger.warning("[Remediation] Unknown action %r ignored", action)

    def _base_attrs(self, event: RemediationInput, action: Optional[str] = None) -> dict:
        pw_ctx = self._engine.contexts.get(event.context_name)
        attrs = {
            "request": event.request,
            "url": event.request.url if event.request is not None else None,
            "method": event.request.method if event.request is not None else None,
            "mode": event.mode,
            "context_name": event.context_name,
            "browser_type": self._engine._browser_type_name,
            "persistent": getattr(pw_ctx, "persistent", None),
            "challenge_type": event.challenge_type,
            "blocked_reason": event.blocked_reason,
            "status": event.status,
            "resp_url": event.resp_url,
            "attempt_count": event.attempt_count,
            "error_type": event.error_type,
            "source_event": event.source_event,
            "action": action,
        }
        return {key: value for key, value in attrs.items() if value is not None}

    async def remediate(self, event: RemediationInput) -> None:
        if not self._enabled:
            self._emit_skipped(event, None, "remediation_disabled")
            return
        if not self._actions:
            self._emit_skipped(event, None, "no_actions_configured")
            return

        skipped = []
        for action in self._actions:
            applies, reason = await action.applies_to(self._engine, event)
            if not applies:
                skipped.append((action.name, reason))
                continue
            self._engine._inc_stat("playwright/antibot/remediation/selected_count")
            self._engine._inc_stat(f"playwright/antibot/remediation/{action.name}/selected_count")
            selected_attrs = self._base_attrs(event, action.name)
            self._engine._emit_signal(pw_signals.antibot_remediation_selected, **selected_attrs)
            started_at = time()
            self._engine._emit_signal(pw_signals.antibot_remediation_started, **selected_attrs)
            try:
                result = await action.run(self._engine, event)
                duration = time() - started_at
                if result.get("outcome") == "skipped":
                    self._emit_skipped(
                        event,
                        action.name,
                        result.get("skip_reason", "action_skipped"),
                        duration=duration,
                    )
                    return
                self._engine._inc_stat("playwright/antibot/remediation/completed_count")
                self._engine._inc_stat(f"playwright/antibot/remediation/{action.name}/completed_count")
                # The remediation tore down + rebuilt the context. Drop the stale
                # per-context challenge state and force the (latched) gate back
                # open so the fresh profile re-probes instead of inheriting a
                # poisoned attempt_count / staying serialized at ~0 throughput.
                self._reset_challenge_state(event.context_name)
                self._engine._emit_signal(
                    pw_signals.antibot_remediation_completed,
                    **selected_attrs,
                    duration=duration,
                    outcome=result.get("outcome", "completed"),
                    profile_dir=result.get("profile_dir"),
                    archive_path=result.get("archive_path"),
                )
                return
            except Exception as exc:
                duration = time() - started_at
                logger.error("[Remediation] Action %s failed: %s", action.name, exc, exc_info=True)
                self._engine._inc_stat("playwright/antibot/remediation/failed_count")
                self._engine._inc_stat(f"playwright/antibot/remediation/{action.name}/failed_count")
                self._engine._emit_signal(
                    pw_signals.antibot_remediation_failed,
                    **selected_attrs,
                    duration=duration,
                    outcome="failed",
                    failure_reason="action_error",
                    error_type=type(exc).__name__,
                )
                return

        action, reason = skipped[0] if skipped else (None, "no_applicable_action")
        self._emit_skipped(event, action, reason)

    def _reset_challenge_state(self, context_name: str) -> None:
        """Clear cached CF challenge state and re-open the gate after a
        completed remediation. Guards for the CF-retry-disabled case where the
        bypass / gate are not attached to the engine."""
        bypass = getattr(self._engine, "_cf_bypass", None)
        if bypass is not None:
            bypass.reset_context(context_name)
        gate = getattr(self._engine, "_cf_gate", None)
        if gate is not None:
            gate.force_reset(reason="remediation_completed")

    def _emit_skipped(
        self,
        event: RemediationInput,
        action: Optional[str],
        reason: str,
        *,
        duration: Optional[float] = None,
    ) -> None:
        self._engine._inc_stat("playwright/antibot/remediation/skipped_count")
        if action:
            self._engine._inc_stat(f"playwright/antibot/remediation/{action}/skipped_count")
        attrs = self._base_attrs(event, action)
        if duration is not None:
            attrs["duration"] = duration
        self._engine._emit_signal(
            pw_signals.antibot_remediation_skipped,
            **attrs,
            outcome="skipped",
            skip_reason=reason,
        )
