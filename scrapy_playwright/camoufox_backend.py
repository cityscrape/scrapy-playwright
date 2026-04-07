"""Camoufox browser backend lifecycle helpers.

Keeps Camoufox-specific setup and teardown isolated from the general
Playwright lifecycle in the engine.
"""

import json as _json
import logging
from contextlib import suppress
from typing import Dict, Optional

logger = logging.getLogger("scrapy-playwright")

PERSISTENT_CONTEXT_PATH_KEY = "user_data_dir"


def load_camoufox() -> type:
    """Import and return the ``AsyncCamoufox`` class, or raise on failure."""
    try:
        from camoufox.async_api import AsyncCamoufox
    except ImportError as exc:
        raise RuntimeError(
            "Camoufox backend requested but the 'camoufox' package is not installed. "
            "Install it and run 'python -m camoufox fetch' before using this backend."
        ) from exc
    return AsyncCamoufox


def prepare_launch_options(
    launch_options: dict,
    camoufox_options: dict,
    context_kwargs: Optional[dict] = None,
) -> dict:
    """Merge options and strip keys unsupported by Camoufox."""
    launch_kwargs = dict(launch_options)
    launch_kwargs.update(camoufox_options)
    if context_kwargs:
        launch_kwargs.update(context_kwargs)
    for unsupported_key in (
        "channel",
        "executable_path",
        "ignore_default_args",
        "args",
    ):
        if unsupported_key in launch_kwargs:
            logger.warning(
                "[Lifecycle] Ignoring Camoufox-incompatible launch option %r",
                unsupported_key,
            )
            launch_kwargs.pop(unsupported_key, None)
    if launch_kwargs.get(PERSISTENT_CONTEXT_PATH_KEY):
        launch_kwargs.setdefault("persistent_context", True)
    return launch_kwargs


async def close_all_launchers(launchers: Dict[str, object]) -> None:
    """Shut down all Camoufox launcher context managers."""
    for name, launcher in list(launchers.items()):
        with suppress(Exception):
            await launcher.__aexit__(None, None, None)
        launchers.pop(name, None)


async def close_launcher(name: str, launcher: object, launchers: Dict[str, object]) -> None:
    """Shut down a single Camoufox launcher and remove it from the registry."""
    with suppress(Exception):
        await launcher.__aexit__(None, None, None)
    launchers.pop(name, None)
