import tempfile
from pathlib import Path
from types import SimpleNamespace
from unittest import IsolatedAsyncioTestCase

from scrapy import Request

from scrapy_playwright import signals as pw_signals
from scrapy_playwright.cloudflare.remediation import (
    RemediationInput,
    RemediationManager,
)


class _FakeEngine:
    def __init__(self, profile_dir: Path, *, camoufox=True, persistent=True):
        self.events = []
        self.stats = []
        self.closed = []
        self.contexts = {"cf": SimpleNamespace(persistent=persistent)}
        self._profile_dir = profile_dir
        self._profile_rotation_archive_dir = str(profile_dir.parent / "archives")
        self._camoufox = camoufox
        self._browser_type_name = "camoufox" if camoufox else "chromium"

    @property
    def _uses_camoufox_backend(self):
        return self._camoufox

    def _emit_signal(self, signal, **kwargs):
        self.events.append((signal, kwargs))

    def _inc_stat(self, name):
        self.stats.append(name)

    def get_context_user_data_dir(self, context_name):
        return str(self._profile_dir)

    async def close_context(self, name):
        self.closed.append(name)
        self.contexts.pop(name, None)


class TestAntiBotRemediation(IsolatedAsyncioTestCase):
    async def test_profile_rotation_archives_profile_and_emits_completion(self):
        with tempfile.TemporaryDirectory() as tmp:
            profile_dir = Path(tmp) / "cloudflare"
            profile_dir.mkdir()
            (profile_dir / "prefs.js").write_text("user_pref();")
            engine = _FakeEngine(profile_dir)
            manager = RemediationManager(engine=engine, enabled=True)

            await manager.remediate(
                RemediationInput(
                    request=Request("https://example.com/listings"),
                    context_name="cf",
                    mode="page",
                    challenge_type="private_token",
                    blocked_reason="private_token_challenge",
                    status=401,
                )
            )

            signals = [signal for signal, _ in engine.events]
            assert pw_signals.antibot_remediation_selected in signals
            assert pw_signals.antibot_remediation_completed in signals
            completed = engine.events[-1][1]
            assert completed["outcome"] == "completed"
            assert completed["archive_path"]
            assert Path(completed["archive_path"]).is_dir()
            assert (Path(completed["archive_path"]) / "prefs.js").exists()
            assert profile_dir.is_dir()
            assert engine.closed == ["cf"]
            assert "playwright/antibot/remediation/completed_count" in engine.stats

    async def test_profile_rotation_skips_non_camoufox(self):
        with tempfile.TemporaryDirectory() as tmp:
            engine = _FakeEngine(Path(tmp) / "cloudflare", camoufox=False)
            manager = RemediationManager(engine=engine, enabled=True)

            await manager.remediate(
                RemediationInput(
                    request=None,
                    context_name="cf",
                    mode="page",
                    challenge_type="private_token",
                    blocked_reason="private_token_challenge",
                )
            )

            assert engine.events[-1][0] is pw_signals.antibot_remediation_skipped
            assert engine.events[-1][1]["skip_reason"] == "browser_not_camoufox"
