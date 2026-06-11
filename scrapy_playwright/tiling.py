import asyncio
import logging
import math
import os
import re
import shlex
import subprocess
from dataclasses import dataclass
from typing import Optional, List, Dict, Tuple

from playwright.async_api import Page

logger = logging.getLogger("scrapy-playwright.tiling")


@dataclass
class Rect:
    x: int
    y: int
    width: int
    height: int


class TilingGrid:
    """Computes a grid layout for N windows on a screen."""

    def __init__(self, screen_width: int, screen_height: int, margin: int = 0):
        self.screen_width = screen_width
        self.screen_height = screen_height
        self.margin = margin
        self.cols = 1
        self.rows = 1

    def compute_layout(self, total_slots: int) -> List[Rect]:
        if total_slots <= 0:
            return []

        self.cols = math.ceil(math.sqrt(total_slots))
        self.rows = math.ceil(total_slots / self.cols)

        cell_w = self.screen_width // self.cols
        cell_h = self.screen_height // self.rows
        
        logger.info(f"[TILING] Computed grid: {self.cols}x{self.rows} for {total_slots} slots on {self.screen_width}x{self.screen_height} screen")

        layout = []
        for i in range(total_slots):
            row = i // self.cols
            col = i % self.cols
            
            x = (col * cell_w) + self.margin
            y = (row * cell_h) + self.margin
            w = cell_w - (2 * self.margin)
            h = cell_h - (2 * self.margin)

            layout.append(Rect(x, y, w, h))
            logger.info(f"[TILING] Slot {i} assigned Rect(x={x}, y={y}, w={w}, h={h})")

        return layout


def detect_screen_resolution() -> Optional[Tuple[int, int]]:
    """Parse xrandr output to get primary monitor resolution."""
    try:
        output = subprocess.check_output(["xrandr", "--query"], stderr=subprocess.DEVNULL).decode("utf-8")
        # Look for the primary display first
        primary_match = re.search(r"(\d+)x(\d+)\+\d+\+\d+\s+primary", output)
        if primary_match:
            logger.info(f"[TILING] Detected primary screen resolution: {primary_match.group(1)}x{primary_match.group(2)}")
            return int(primary_match.group(1)), int(primary_match.group(2))
        
        # Fallback to the first connected display with a resolution
        connected_match = re.search(r"\bconnected\b.*?(\d+)x(\d+)\+\d+\+\d+", output, re.DOTALL)
        if connected_match:
            logger.info(f"[TILING] Detected connected screen resolution: {connected_match.group(1)}x{connected_match.group(2)}")
            return int(connected_match.group(1)), int(connected_match.group(2))
            
    except Exception as e:
        logger.debug(f"[TILING] Failed to detect screen resolution via xrandr: {e}")
    return None


class WindowTilingManager:
    """Assigns grid slots to pages and moves OS windows."""

    def __init__(
        self,
        pool_size: int,
        screen_width: Optional[int] = None,
        screen_height: Optional[int] = None,
        margin: int = 0,
        adjust_viewport: bool = True,
    ):
        if not screen_width or not screen_height:
            detected = detect_screen_resolution()
            if detected:
                w, h = detected
                screen_width = screen_width or w
                screen_height = screen_height or h
            else:
                # Safe defaults if detection fails
                screen_width = screen_width or 1920
                screen_height = screen_height or 1080
                logger.warning(f"[TILING] Screen detection failed, defaulting to {screen_width}x{screen_height}")

        self.adjust_viewport = adjust_viewport
        self._grid = TilingGrid(screen_width, screen_height, margin)
        self._layout = self._grid.compute_layout(pool_size)
        self._slots: Dict[Page, int] = {}
        # Keep track of available slots, initially all of them.
        self._free_slots: List[int] = list(range(pool_size))
        
        # For keeping track of the browser process
        self._browser_pid: Optional[int] = None

    async def _get_browser_pid(self, page: Page) -> Optional[int]:
        if self._browser_pid is not None:
            return self._browser_pid
            
        try:
            # We can try to extract PID from the Playwright browser object
            if hasattr(page.context.browser, "_browser_process"):
                self._browser_pid = page.context.browser._browser_process.pid
                logger.info(f"[TILING] Detected Playwright browser PID: {self._browser_pid}")
                return self._browser_pid
        except Exception as e:
            logger.debug(f"[TILING] Failed to get browser PID directly: {e}")
            
        return None

    async def assign_page(self, page: Page) -> Optional[Rect]:
        """Assign the next free slot and position the window."""
        if "DISPLAY" not in os.environ and "WAYLAND_DISPLAY" not in os.environ:
            logger.debug("[TILING] No display detected, skipping window tiling.")
            return None

        if not self._free_slots:
            logger.warning("[TILING] No free slots available for window tiling.")
            return None

        # Take the next available slot
        slot_index = self._free_slots.pop(0)
        self._slots[page] = slot_index
        rect = self._layout[slot_index]
        
        logger.info(f"[TILING] Assigning slot {slot_index} {rect} to page {page}")

        # In a real environment, the browser window might take a moment to appear
        await asyncio.sleep(0.5)

        success = await self._move_window(page, rect)
        
        if success:
            logger.info(f"[TILING] Successfully moved window for page {page} to {rect}")
            if self.adjust_viewport:
                try:
                    await page.set_viewport_size({"width": rect.width, "height": rect.height})
                    logger.info(f"[TILING] Adjusted viewport for page {page} to {rect.width}x{rect.height}")
                except Exception as e:
                    logger.warning(f"[TILING] Failed to adjust viewport for tiled window: {e}")
        else:
            logger.warning(f"[TILING] Failed to move window for page {page}")
                
        return rect if success else None

    def release_page(self, page: Page) -> None:
        """Free a slot when a page closes."""
        if page in self._slots:
            slot_index = self._slots.pop(page)
            self._free_slots.append(slot_index)
            # Keep sorted so we reuse lower indices first
            self._free_slots.sort()
            logger.info(f"[TILING] Released slot {slot_index} from page {page}")

    async def _detect_window_id(self, page: Page) -> Optional[int]:
        """Find the X11 window ID for a Playwright page."""
        try:
            # 1. Try to find by injecting a unique title
            # This is the most reliable way to find the EXACT OS window 
            # associated with this specific page, especially for popups.
            unique_title = f"playwright-window-{id(page)}"
            # We save the original title to restore it later if needed,
            # though it usually gets overwritten by navigation anyway.
            original_title = await page.title()
            await page.evaluate(f"document.title = '{unique_title}'")
            
            # Wait a moment for X11/WM to reflect the title change
            await asyncio.sleep(0.2)
            
            cmd = f"xdotool search --name {shlex.quote(unique_title)}"
            logger.info(f"[TILING] Trying to detect window by unique title: {cmd}")
            proc = await asyncio.create_subprocess_shell(
                cmd, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.DEVNULL
            )
            stdout, _ = await proc.communicate()
            if proc.returncode == 0 and stdout:
                windows = stdout.decode().strip().split('\n')
                if windows:
                    wid = int(windows[-1]) # Return the last (usually newest) window
                    logger.info(f"[TILING] Found window ID {wid} by title")
                    return wid
        except Exception as e:
            logger.debug(f"[TILING] Exception detecting by title: {e}")
            
        try:
            # 2. Try to find by browser PID
            pid = await self._get_browser_pid(page)
            if pid:
                cmd = f"xdotool search --pid {pid}"
                logger.info(f"[TILING] Trying to detect window by PID: {cmd}")
                proc = await asyncio.create_subprocess_shell(
                    cmd, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.DEVNULL
                )
                stdout, _ = await proc.communicate()
                if proc.returncode == 0 and stdout:
                    windows = stdout.decode().strip().split('\n')
                    if windows:
                        wid = int(windows[-1])
                        logger.info(f"[TILING] Found window ID {wid} by PID {pid}")
                        return wid
        except Exception as e:
            logger.debug(f"[TILING] Exception detecting by PID: {e}")

        try:
            # 3. Fallback: Find the most recent window owned by Firefox/Camoufox
            cmd = "xdotool search --class firefox"
            logger.info(f"[TILING] Trying to detect window by class: {cmd}")
            proc = await asyncio.create_subprocess_shell(
                cmd, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.DEVNULL
            )
            stdout, _ = await proc.communicate()
            if proc.returncode == 0 and stdout:
                windows = stdout.decode().strip().split('\n')
                if windows:
                    wid = int(windows[-1])
                    logger.info(f"[TILING] Found window ID {wid} by class")
                    return wid
        except Exception as e:
            logger.debug(f"[TILING] Exception detecting by class: {e}")

        logger.warning("[TILING] Could not detect any window ID for the page")
        return None

    async def _move_window(self, page: Page, rect: Rect) -> bool:
        """Move + resize the OS window using xdotool."""
        wid = await self._detect_window_id(page)
        if not wid:
            logger.warning(f"[TILING] Could not detect window ID for page to move to {rect}")
            return False

        try:
            # Use wmctrl if available, it's generally more reliable
            proc = await asyncio.create_subprocess_shell(
                "which wmctrl", stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.DEVNULL
            )
            await proc.communicate()
            
            if proc.returncode == 0:
                # wmctrl is available: wmctrl -i -r <wid> -e 0,x,y,w,h
                cmd = f"wmctrl -i -r {wid} -e 0,{rect.x},{rect.y},{rect.width},{rect.height}"
                logger.info(f"[TILING] Executing wmctrl: {cmd}")
            else:
                # fallback to xdotool
                cmd = f"xdotool windowsize {wid} {rect.width} {rect.height} windowmove {wid} {rect.x} {rect.y}"
                logger.info(f"[TILING] Executing xdotool: {cmd}")

            proc = await asyncio.create_subprocess_shell(
                cmd, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE
            )
            stdout, stderr = await proc.communicate()
            
            if proc.returncode == 0:
                logger.info(f"[TILING] Successfully executed window movement command.")
                return True
            else:
                logger.warning(f"[TILING] Window movement command failed with code {proc.returncode}. stderr: {stderr.decode()}")
                return False

        except Exception as e:
            logger.error(f"[TILING] Failed to move window: {e}")
            return False
