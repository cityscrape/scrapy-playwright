"""
Network Recorder — captures Playwright network traffic into structured directories.

Hooks into Playwright page "response" events and writes per-request directories
containing request.http, response.http, response_body.*, metadata.json, and
cookies.json files. Output format is identical to scripts/har_processor.py so
the same analysis tools work on both browser-captured HARs and live recordings.
"""

import base64
import logging
import os
import pathlib
import re
from contextlib import suppress
from datetime import datetime, timezone
from typing import Optional
from urllib.parse import urlparse

from playwright.async_api import (
    Request as PlaywrightRequest,
    Response as PlaywrightResponse,
)
from iwantafuckinghouse.processors.har_artifacts import write_har_entry_artifacts

logger = logging.getLogger("scrapy-playwright")

def _get_playwright_http_version(obj) -> tuple[Optional[str], str]:
    """Best-effort protocol extraction from Playwright objects.

    Playwright's public Python Request/Response API does not expose the HTTP
    protocol version. Some browser backends may carry it in internal
    initializer fields, so use those when present and otherwise leave the value
    unknown instead of hardcoding HTTP/1.1.
    """
    for attr in ("http_version", "httpVersion", "protocol"):
        value = getattr(obj, attr, None)
        if callable(value):
            with suppress(Exception):
                value = value()
        if value:
            return str(value), f"playwright.{attr}"

    impl = getattr(obj, "_impl_obj", None)
    initializer = getattr(impl, "_initializer", None)
    if isinstance(initializer, dict):
        for key in ("httpVersion", "http_version", "protocol"):
            value = initializer.get(key)
            if value:
                return str(value), f"playwright._initializer.{key}"

    return None, "not_exposed_by_playwright"


# ── Cookie parsing helpers ─────────────────────────────────────────────────

def _parse_request_cookies(headers: list[dict]) -> list[dict]:
    """Extract cookies from request headers array [{name, value}]."""
    cookies = []
    for h in headers:
        if h['name'].lower() == 'cookie':
            for pair in h['value'].split(';'):
                pair = pair.strip()
                if '=' in pair:
                    name, _, value = pair.partition('=')
                    cookies.append({'name': name.strip(), 'value': value.strip()})
    return cookies


def _parse_response_cookies(headers: list[dict]) -> list[dict]:
    """Extract cookies from Set-Cookie response headers."""
    cookies = []
    for h in headers:
        if h['name'].lower() == 'set-cookie':
            parts = h['value'].split(';')
            if not parts:
                continue
            main = parts[0].strip()
            if '=' not in main:
                continue
            name, _, value = main.partition('=')
            cookie: dict = {'name': name.strip(), 'value': value.strip()}
            for attr in parts[1:]:
                attr = attr.strip()
                if not attr:
                    continue
                attr_lower = attr.lower()
                if attr_lower == 'httponly':
                    cookie['httpOnly'] = True
                elif attr_lower == 'secure':
                    cookie['secure'] = True
                elif '=' in attr:
                    aname, _, avalue = attr.partition('=')
                    aname_lower = aname.strip().lower()
                    avalue = avalue.strip()
                    if aname_lower == 'path':
                        cookie['path'] = avalue
                    elif aname_lower == 'domain':
                        cookie['domain'] = avalue
                    elif aname_lower == 'expires':
                        cookie['expires'] = avalue
                    elif aname_lower == 'samesite':
                        cookie['sameSite'] = avalue
                    elif aname_lower == 'max-age':
                        cookie['maxAge'] = avalue
            cookies.append(cookie)
    return cookies


# ── NetworkRecorder ────────────────────────────────────────────────────────

class NetworkRecorder:
    """Records Playwright network traffic into per-request directories.

    Attach ``on_response`` as a callback to ``page.on("response", ...)``.
    Each response writes a directory immediately, so data survives crashes.
    """

    def __init__(
            self,
            output_dir: pathlib.Path,
            context_name: str,
            url_filter: Optional[str] = None,
    ) -> None:
        safe_name = re.sub(r"[^\w\-]", "_", context_name)
        timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S")
        self.base_dir = output_dir / f"{safe_name}_{timestamp}"
        os.makedirs(self.base_dir, exist_ok=True)
        self.context_name = context_name
        self._url_filter = re.compile(url_filter) if url_filter else None
        self._counter = 0

    async def on_response(self, response: PlaywrightResponse) -> None:
        """Playwright page "response" event handler."""
        url = response.url
        if self._url_filter and not self._url_filter.search(url):
            return

        try:
            entry = await self._build_har_entry(response)
        except Exception:
            logger.debug(
                "Failed to build HAR entry for %s", url, exc_info=True,
            )
            return

        index = self._counter
        self._counter += 1

        try:
            self._write_entry(entry, index)
        except Exception:
            logger.debug(
                "Failed to write HAR entry %d for %s", index, url, exc_info=True,
            )

    async def on_request_failed(self, request: PlaywrightRequest) -> None:
        """Playwright page "requestfailed" event handler.

        Failed browser requests do not emit a response event, so recording only
        responses drops the exact requests we need when fetch() fails before an
        HTTP response is exposed to page JavaScript.
        """
        url = request.url
        if self._url_filter and not self._url_filter.search(url):
            return

        try:
            entry = await self._build_failed_har_entry(request)
        except Exception:
            logger.debug(
                "Failed to build failed HAR entry for %s", url, exc_info=True,
            )
            return

        index = self._counter
        self._counter += 1

        try:
            self._write_entry(entry, index)
        except Exception:
            logger.debug(
                "Failed to write failed HAR entry %d for %s", index, url, exc_info=True,
            )

    # ── Build HAR-format entry from Playwright objects ─────────────────

    async def _build_failed_har_entry(
            self, pw_request: PlaywrightRequest,
    ) -> dict:
        req_headers = await pw_request.headers_array()
        req_http_version, req_http_version_source = _get_playwright_http_version(pw_request)

        parsed = urlparse(pw_request.url)
        query_params = []
        if parsed.query:
            for part in parsed.query.split('&'):
                if '=' in part:
                    k, _, v = part.partition('=')
                    query_params.append({'name': k, 'value': v})
                else:
                    query_params.append({'name': part, 'value': ''})

        post_data_dict = None
        post_data_text = pw_request.post_data
        if post_data_text is not None:
            req_ct = ''
            for h in req_headers:
                if h['name'].lower() == 'content-type':
                    req_ct = h['value']
                    break
            post_data_dict = {
                'mimeType': req_ct,
                'text': post_data_text,
            }

        req_cookies = _parse_request_cookies(req_headers)

        sizes = {}
        with suppress(Exception):
            sizes = await pw_request.sizes()

        timing = None
        with suppress(Exception):
            timing = pw_request.timing

        failure = None
        with suppress(Exception):
            failure = pw_request.failure
            if callable(failure):
                failure = failure()

        entry = {
            'startedDateTime': datetime.now(timezone.utc).isoformat(),
            'time': timing.get('responseEnd', 0) if timing else 0,
            'serverIPAddress': None,
            '_resourceType': pw_request.resource_type,
            '_failureText': failure,
            'request': {
                'method': pw_request.method,
                'url': pw_request.url,
                'httpVersion': req_http_version,
                '_httpVersionSource': req_http_version_source,
                'headers': req_headers,
                'queryString': query_params,
                'cookies': req_cookies,
                'headersSize': sizes.get('requestHeadersSize', -1),
                'bodySize': sizes.get('requestBodySize', -1),
            },
            'response': {
                'status': 0,
                'statusText': 'REQUEST_FAILED',
                'httpVersion': None,
                '_httpVersionSource': 'no_response_for_failed_request',
                'headers': [],
                'cookies': [],
                'content': {
                    'size': 0,
                    'mimeType': 'x-playwright/request-failed',
                    'text': failure or '',
                },
                'headersSize': -1,
                'bodySize': 0,
            },
            'timings': timing,
        }

        if post_data_dict:
            entry['request']['postData'] = post_data_dict

        return entry

    async def _build_har_entry(
            self, response: PlaywrightResponse,
    ) -> dict:
        pw_request = response.request
        req_http_version, req_http_version_source = _get_playwright_http_version(pw_request)
        resp_http_version, resp_http_version_source = _get_playwright_http_version(response)

        # Headers — headers_array() returns [{name, value}] matching HAR format
        req_headers = await pw_request.headers_array()
        resp_headers = await response.headers_array()

        # Content-Type for response
        resp_mime = ''
        for h in resp_headers:
            if h['name'].lower() == 'content-type':
                resp_mime = h['value']
                break

        # Query string from URL
        parsed = urlparse(pw_request.url)
        query_params = []
        if parsed.query:
            for part in parsed.query.split('&'):
                if '=' in part:
                    k, _, v = part.partition('=')
                    query_params.append({'name': k, 'value': v})
                else:
                    query_params.append({'name': part, 'value': ''})

        # Request body
        post_data_dict = None
        post_data_text = pw_request.post_data
        if post_data_text is not None:
            # Detect content-type from request headers
            req_ct = ''
            for h in req_headers:
                if h['name'].lower() == 'content-type':
                    req_ct = h['value']
                    break
            post_data_dict = {
                'mimeType': req_ct,
                'text': post_data_text,
            }

        # Cookies
        req_cookies = _parse_request_cookies(req_headers)
        resp_cookies = _parse_response_cookies(resp_headers)

        # Response body
        body_text = None
        body_encoding = None
        body_size = 0
        try:
            body_bytes = await response.body()
            body_size = len(body_bytes)
            # Store as base64 for binary, text for text
            base_mime = resp_mime.split(';')[0].strip().lower()
            is_text = (
                    base_mime.startswith('text/')
                    or 'json' in base_mime
                    or 'xml' in base_mime
                    or 'javascript' in base_mime
                    or 'x-www-form-urlencoded' in base_mime
            )
            if is_text:
                try:
                    body_text = body_bytes.decode('utf-8')
                except UnicodeDecodeError:
                    body_text = base64.b64encode(body_bytes).decode('ascii')
                    body_encoding = 'base64'
            else:
                body_text = base64.b64encode(body_bytes).decode('ascii')
                body_encoding = 'base64'
        except Exception:
            pass

        # Sizes
        sizes = {}
        with suppress(Exception):
            sizes = await pw_request.sizes()

        # Server IP
        server_ip = None
        with suppress(Exception):
            addr = await response.server_addr()
            if addr:
                server_ip = addr.get('ipAddress')

        # Timing
        timing = None
        with suppress(Exception):
            timing = pw_request.timing

        # Build the entry dict in HAR format
        entry = {
            'startedDateTime': datetime.now(timezone.utc).isoformat(),
            'time': timing.get('responseEnd', 0) if timing else 0,
            'serverIPAddress': server_ip,
            '_resourceType': pw_request.resource_type,
            'request': {
                'method': pw_request.method,
                'url': pw_request.url,
                'httpVersion': req_http_version,
                '_httpVersionSource': req_http_version_source,
                'headers': req_headers,
                'queryString': query_params,
                'cookies': req_cookies,
                'headersSize': sizes.get('requestHeadersSize', -1),
                'bodySize': sizes.get('requestBodySize', -1),
            },
            'response': {
                'status': response.status,
                'statusText': response.status_text,
                'httpVersion': resp_http_version,
                '_httpVersionSource': resp_http_version_source,
                'headers': resp_headers,
                'cookies': resp_cookies,
                'content': {
                    'size': body_size,
                    'mimeType': resp_mime,
                    'text': body_text,
                },
                'headersSize': sizes.get('responseHeadersSize', -1),
                'bodySize': sizes.get('responseBodySize', -1),
            },
            'timings': timing,
        }

        if body_encoding:
            entry['response']['content']['encoding'] = body_encoding

        if post_data_dict:
            entry['request']['postData'] = post_data_dict

        return entry

    # ── Write files to disk ────────────────────────────────────────────

    def _write_entry(self, entry: dict, index: int) -> None:
        write_har_entry_artifacts(entry, index, self.base_dir)

    # ── Finalize ───────────────────────────────────────────────────────

    def finalize(self) -> None:
        """Log summary. Called when the browser context closes."""
        if self._counter > 0:
            logger.info(
                "Network recorder saved %d entries for context '%s': %s",
                self._counter, self.context_name, self.base_dir,
            )
        else:
            logger.info(
                "Network recorder captured 0 entries for context '%s'",
                self.context_name,
            )

    @property
    def entry_count(self) -> int:
        return self._counter
