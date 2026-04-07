"""Cloudflare challenge handling subsystem.

Public API re-exported here for convenience::

    from scrapy_playwright.cloudflare import (
        CloudflareBypass,
        CfGate,
        ChallengeType,
        ChallengeState,
        ContextChallengeInfo,
    )
"""

from scrapy_playwright.cloudflare.types import (  # noqa: F401
    ChallengeType,
    ChallengeState,
    ContextChallengeInfo,
    has_cf_clearance,
    resolve_first_party_url,
)
from scrapy_playwright.cloudflare.gate import CfGate  # noqa: F401
from scrapy_playwright.cloudflare.bypass import CloudflareBypass  # noqa: F401


# ── Deprecated middleware stub (back-compat) ──────────────────────────

class CloudflareChallengeRetryMiddleware:
    """Deprecated stub — raises ``NotConfigured`` on startup."""

    @classmethod
    def from_crawler(cls, crawler):
        from scrapy.exceptions import NotConfigured

        raise NotConfigured(
            "CloudflareChallengeRetryMiddleware is deprecated. "
            "Remove it from DOWNLOADER_MIDDLEWARES and enable "
            "PLAYWRIGHT_CLOUDFLARE_CHALLENGE_RETRY in the download handler instead."
        )
