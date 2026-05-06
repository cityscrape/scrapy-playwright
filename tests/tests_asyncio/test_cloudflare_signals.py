from unittest import IsolatedAsyncioTestCase
from unittest.mock import AsyncMock

from scrapy import Request, Spider
from scrapy.http import HtmlResponse

from scrapy_playwright.cloudflare.bypass import CloudflareBypass
from scrapy_playwright.cloudflare.gate import CfGate
from scrapy_playwright import signals as pw_signals


class _FakeEngine:
    def __init__(self):
        self.events = []
        self.contexts = {}

    def _emit_signal(self, signal, **kwargs):
        self.events.append((signal, kwargs))

    def _inc_stat(self, name: str) -> None:
        return None

    def _set_stat(self, name: str, value) -> None:
        return None

    def _sanitize_for_log(self, value):
        return value


class TestCloudflareSignals(IsolatedAsyncioTestCase):
    async def test_plain_403_emits_terminal_block(self):
        engine = _FakeEngine()
        bypass = CloudflareBypass(engine=engine)
        request = Request("https://example.com/listings", meta={"playwright_context": "cf"})
        response = HtmlResponse(
            url=request.url,
            request=request,
            status=403,
            headers={b"Server": b"cloudflare"},
            body=b"Forbidden",
        )

        result = await bypass.handle_if_challenged(
            request,
            response,
            Spider("test"),
            "cf",
            dispatch_fn=AsyncMock(),
        )

        self.assertIs(result, response)
        assert engine.events[0][0] is pw_signals.playwright_blocked
        assert engine.events[0][1]["blocked_reason"] == "cloudflare_plain_403"

    async def test_turnstile_solve_emits_detected_and_solve_events(self):
        engine = _FakeEngine()
        bypass = CloudflareBypass(engine=engine)
        bypass._solve = AsyncMock(return_value=True)
        request = Request("https://example.com/listings", meta={"playwright_context": "cf"})
        response = HtmlResponse(
            url=request.url,
            request=request,
            status=403,
            headers={b"Server": b"cloudflare", b"Cf-Mitigated": b"challenge"},
            body=b'<html><div class="cf-turnstile" data-sitekey="abc123"></div></html>',
        )
        retried = HtmlResponse(url=request.url, request=request, status=200, body=b"ok")
        dispatch_fn = AsyncMock(return_value=retried)

        result = await bypass.handle_if_challenged(
            request,
            response,
            Spider("test"),
            "cf",
            dispatch_fn=dispatch_fn,
        )

        self.assertIs(result, retried)
        signals = [signal for signal, _ in engine.events]
        assert pw_signals.cloudflare_challenge_detected in signals
        assert pw_signals.cloudflare_solve_started in signals
        assert pw_signals.cloudflare_solve_completed in signals

    async def test_gate_emits_blocked_and_opened(self):
        events = []
        gate = CfGate(enabled=True, emit_signal=lambda signal, **kwargs: events.append((signal, kwargs)))
        request_attrs = {
            "url": "https://example.com/listings",
            "method": "GET",
            "mode": "page",
            "context_name": "cf",
        }
        challenged = HtmlResponse(
            url="https://example.com/listings",
            status=403,
            headers={b"Server": b"cloudflare", b"Cf-Mitigated": b"challenge"},
            body=b'<html><script>window._cf_chl_opt=1</script></html>',
        )
        clear = HtmlResponse(
            url="https://example.com/listings",
            status=200,
            headers={b"Server": b"cloudflare"},
            body=b"<html>ok</html>",
        )

        await gate.maybe_open(challenged, **request_attrs)
        await gate.maybe_open(clear, **request_attrs)

        assert events[0][0] is pw_signals.cloudflare_gate_blocked
        assert events[1][0] is pw_signals.cloudflare_gate_opened
