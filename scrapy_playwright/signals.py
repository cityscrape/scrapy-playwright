"""
Playwright downloader lifecycle signals.

Signals are ``object()`` sentinels dispatched through Scrapy's
``crawler.signals.send_catch_log()`` mechanism — the same pattern used
by built-in Scrapy signals such as ``spider_opened``.

Firing a signal (producer side, inside the download handler)::

    from scrapy_playwright.signals import page_created
    self._crawler.signals.send_catch_log(
        page_created,
        context_name="default",
        page_count=3,
    )

Connecting to a signal (consumer side)::

    from scrapy_playwright.signals import page_created
    crawler.signals.connect(self._on_page_created, page_created)
"""

# ── Handler lifecycle ───────────────────────────────────────────────

handler_ready = object()
"""Fired once after ``_launch()`` completes and the Playwright driver is ready.

Keyword arguments:
* ``browser_type`` — name of the browser (e.g. ``"chromium"``).
"""

handler_closing = object()
"""Fired at the start of ``_close()`` before any contexts are torn down.

Keyword arguments:
* ``context_count`` — number of open contexts at the time of closing.
* ``page_count``    — total number of open pages across all contexts.
"""

# ── Browser context lifecycle ───────────────────────────────────────

context_created = object()
"""Fired after a new browser context has been created.

Keyword arguments:
* ``context_name`` — logical name of the context.
* ``persistent``   — whether it is a persistent context.
* ``remote``       — whether it is a remote browser connection.
"""

# ── Page lifecycle ──────────────────────────────────────────────────

page_created = object()
"""Fired after a new page has been opened inside a context.

Keyword arguments:
* ``context_name``       — name of the owning context.
* ``context_page_count`` — page count within that context after creation.
* ``total_page_count``   — page count across all contexts after creation.
"""

# ── Download lifecycle ──────────────────────────────────────────────

download_started = object()
"""Fired when ``_download_request`` begins processing a Playwright request.

Keyword arguments:
* ``url``    — request URL.
* ``method`` — HTTP method.
* ``mode``   — ``"page"`` or ``"fetch"``.
"""

download_completed = object()
"""Fired when ``_download_request`` returns a successful response.

Keyword arguments:
* ``url``      — request URL.
* ``method``   — HTTP method.
* ``mode``     — ``"page"`` or ``"fetch"``.
* ``status``   — HTTP response status code.
* ``duration`` — elapsed time in seconds (float).
"""

download_failed = object()
"""Fired when ``_download_request`` raises an unrecoverable exception.

Keyword arguments:
* ``url``        — request URL.
* ``method``     — HTTP method.
* ``mode``       — ``"page"`` or ``"fetch"``.
* ``error_type`` — exception class name (str).
* ``duration``   — elapsed time in seconds (float).
"""

# ── Fetch-style download ───────────────────────────────────────────

fetch_executed = object()
"""Fired after a fetch-style ``page.evaluate(fetch(...))`` completes.

Keyword arguments:
* ``url``      — request URL.
* ``method``   — HTTP method.
* ``status``   — HTTP response status code.
* ``duration`` — elapsed time in seconds (float).
"""

# ── Failure / recovery ─────────────────────────────────────────────

browser_disconnected = object()
"""Fired when the browser's ``disconnected`` event fires.

Keyword arguments:
* ``restart_enabled`` — whether automatic restart is configured.
"""

driver_restarted = object()
"""Fired after the Playwright Node.js driver has been successfully restarted.

Keyword arguments:
* ``browser_type`` — name of the browser (e.g. ``"chromium"``).
"""
