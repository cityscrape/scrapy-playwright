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

page_tiled = object()
"""Fired after a page window has been positioned in the tiling grid.

Keyword arguments:
* ``context_name`` — name of the owning context.
* ``slot_index``   — grid slot assigned to this page.
* ``rect``         — (x, y, width, height) of the window.
* ``grid_size``    — (cols, rows) of the current grid.
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

fetch_seed_timeout = object()
"""Fired when a carrier-page seed navigation (``page.goto(seed_url)``) times out.

A timeout here almost always means the carrier page is stuck on a Cloudflare
interstitial rather than reaching the listing page — a strong signal that the
session is being actively challenged. Tracking its rate against request rate
helps locate the aggressiveness threshold.

Keyword arguments:
* ``context_name`` — logical name of the context.
* ``seed_url``     — URL being navigated to.
* ``attempt``      — 1-based attempt number.
* ``max_attempts`` — configured attempt budget.
* ``timeout_ms``   — per-attempt navigation timeout in milliseconds.
* ``final``        — ``True`` when this was the last attempt and it failed.
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

# ── Anti-bot / Cloudflare lifecycle ──────────────────────────────────

cloudflare_gate_blocked = object()
"""Fired when the pre-clearance gate remains closed after a challenged response.

Keyword arguments:
* ``url``            — request URL.
* ``method``         — HTTP method.
* ``mode``           — ``"page"`` or ``"fetch"``.
* ``context_name``   — logical name of the context.
* ``challenge_type`` — challenge classification.
* ``status``         — response status code.
* ``resp_url``       — final response URL.
* ``cf_mitigated``   — ``Cf-Mitigated`` response header if present.
"""

cloudflare_gate_opened = object()
"""Fired when the pre-clearance gate opens after an unchallenged response.

Keyword arguments:
* ``url``            — request URL.
* ``method``         — HTTP method.
* ``mode``           — ``"page"`` or ``"fetch"``.
* ``context_name``   — logical name of the context.
* ``challenge_type`` — classification of the final response (normally ``"none"``).
* ``status``         — response status code.
* ``resp_url``       — final response URL.
"""

cloudflare_gate_rearmed = object()
"""Fired when an already-open pre-clearance gate is re-closed.

This is the key "Cloudflare started flagging us again" marker: it means the
session previously had clearance (the gate was open and requests ran in
parallel) and then Cloudflare re-escalated — passive checks turned into an
interstitial/challenge — or the browser driver restarted onto a cold context.
The ``requests_since_clearance`` / ``seconds_since_clearance`` fields capture
how much traffic the session sustained before being flagged, which is the
primary measurement for request-rate aggressiveness experiments.

Keyword arguments:
* ``context_name``             — logical name of the context.
* ``mode``                     — ``"page"`` or ``"fetch"`` (when known).
* ``challenge_type``           — challenge classification that triggered it,
  or ``None`` for non-challenge causes (e.g. driver restart).
* ``reason``                   — coarse cause, e.g. ``"re_challenge"`` or
  ``"driver_restart"``.
* ``requests_since_clearance`` — downloads served since the gate last opened.
* ``seconds_since_clearance``  — wall-clock seconds the gate stayed open.
"""

cloudflare_challenge_detected = object()
"""Fired when a challenged response is classified as Cloudflare-managed.

Keyword arguments:
* ``request``        — originating Scrapy request.
* ``url``            — request URL.
* ``method``         — HTTP method.
* ``mode``           — ``"page"`` or ``"fetch"``.
* ``context_name``   — logical name of the context.
* ``challenge_type`` — challenge classification.
* ``status``         — response status code.
* ``resp_url``       — final response URL.
* ``cf_mitigated``   — ``Cf-Mitigated`` response header if present.
"""

cloudflare_request_parked = object()
"""Fired when a request waits behind an active solve cycle.

Keyword arguments:
* ``request``      — parked Scrapy request.
* ``url``          — request URL.
* ``method``       — HTTP method.
* ``mode``         — ``"page"`` or ``"fetch"``.
* ``context_name`` — logical name of the context.
* ``duration``     — parked time in seconds.
"""

cloudflare_solve_started = object()
"""Fired when a request becomes the solver for a context challenge cycle.

Keyword arguments:
* ``request``        — originating Scrapy request.
* ``url``            — request URL.
* ``method``         — HTTP method.
* ``mode``           — ``"page"`` or ``"fetch"``.
* ``context_name``   — logical name of the context.
* ``challenge_type`` — challenge classification.
* ``attempt_count``  — solve attempt number.
"""

cloudflare_solve_completed = object()
"""Fired when a solve cycle completes successfully.

Keyword arguments:
* ``request``           — originating Scrapy request.
* ``url``               — request URL.
* ``method``            — HTTP method.
* ``mode``              — ``"page"`` or ``"fetch"``.
* ``context_name``      — logical name of the context.
* ``challenge_type``    — challenge classification.
* ``attempt_count``     — solve attempt number.
* ``duration``          — solve duration in seconds.
* ``validated_url``     — URL used to validate clearance.
* ``outcome``           — terminal outcome string.
"""

cloudflare_solve_failed = object()
"""Fired when a solve cycle fails or errors.

Keyword arguments:
* ``request``         — originating Scrapy request.
* ``url``             — request URL.
* ``method``          — HTTP method.
* ``mode``            — ``"page"`` or ``"fetch"``.
* ``context_name``    — logical name of the context.
* ``challenge_type``  — challenge classification.
* ``attempt_count``   — solve attempt number.
* ``duration``        — solve duration in seconds.
* ``outcome``         — terminal outcome string.
* ``error_type``      — exception class name when available.
"""

cloudflare_validation_started = object()
"""Fired when clearance validation begins.

Keyword arguments:
* ``request``        — originating Scrapy request.
* ``url``            — request URL.
* ``method``         — HTTP method.
* ``mode``           — ``"page"`` or ``"fetch"``.
* ``context_name``   — logical name of the context.
* ``challenge_type`` — challenge classification.
* ``attempt_count``  — solve attempt number.
* ``probe_url``      — validation URL.
"""

cloudflare_validation_completed = object()
"""Fired when clearance validation succeeds.

Keyword arguments:
* ``request``        — originating Scrapy request.
* ``url``            — request URL.
* ``method``         — HTTP method.
* ``mode``           — ``"page"`` or ``"fetch"``.
* ``context_name``   — logical name of the context.
* ``challenge_type`` — challenge classification.
* ``attempt_count``  — solve attempt number.
* ``probe_url``      — validation URL.
* ``duration``       — validation duration in seconds.
* ``validated_url``  — URL that proved clearance.
"""

cloudflare_validation_failed = object()
"""Fired when clearance validation fails.

Keyword arguments:
* ``request``         — originating Scrapy request.
* ``url``             — request URL.
* ``method``          — HTTP method.
* ``mode``            — ``"page"`` or ``"fetch"``.
* ``context_name``    — logical name of the context.
* ``challenge_type``  — challenge classification.
* ``attempt_count``   — solve attempt number.
* ``probe_url``       — validation URL.
* ``duration``        — validation duration in seconds.
* ``outcome``         — failure outcome string.
* ``error_type``      — exception class name when available.
"""

playwright_blocked = object()
"""Fired for terminal blocked outcomes that matter operationally.

Keyword arguments:
* ``request``         — originating Scrapy request.
* ``url``             — request URL.
* ``method``          — HTTP method.
* ``mode``            — ``"page"`` or ``"fetch"``.
* ``context_name``    — logical name of the context.
* ``challenge_type``  — challenge classification when known.
* ``blocked_reason``  — normalized reason string.
* ``status``          — response status code when available.
* ``resp_url``        — final response URL when available.
* ``cf_mitigated``    — ``Cf-Mitigated`` response header if present.
* ``attempt_count``   — solve attempt number when applicable.
* ``error_type``      — exception class name when applicable.
"""

antibot_remediation_selected = object()
"""Fired after an anti-bot remediation action is selected.

Keyword arguments:
* ``request``         — originating Scrapy request when available.
* ``action``          — remediation action name.
* ``context_name``    — logical context name.
* ``challenge_type``  — challenge classification when known.
* ``blocked_reason``  — normalized blocked reason.
* ``source_event``    — source signal/event that requested remediation.
"""

antibot_remediation_started = object()
"""Fired when an anti-bot remediation action starts.

Keyword arguments are the same as ``antibot_remediation_selected``.
"""

antibot_remediation_completed = object()
"""Fired when an anti-bot remediation action completes.

Keyword arguments:
* ``action``       — remediation action name.
* ``outcome``      — terminal action outcome.
* ``duration``     — action duration in seconds.
* ``profile_dir``  — profile path affected by the action, for logs/traces.
* ``archive_path`` — archive path created by the action, for logs/traces.
"""

antibot_remediation_skipped = object()
"""Fired when remediation is not run.

Keyword arguments:
* ``action``       — candidate action when known.
* ``outcome``      — normally ``"skipped"``.
* ``skip_reason``  — low-cardinality reason string.
"""

antibot_remediation_failed = object()
"""Fired when remediation action execution fails.

Keyword arguments:
* ``action``         — remediation action name.
* ``outcome``        — normally ``"failed"``.
* ``failure_reason`` — low-cardinality failure reason.
* ``error_type``     — exception class name.
* ``duration``       — action duration in seconds.
"""
