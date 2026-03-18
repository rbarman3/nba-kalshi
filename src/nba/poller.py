"""
Background polling infrastructure.

The Poller class runs a caller-supplied fetch function on a daemon thread
at a fixed interval and forwards each result to a callback. Used by the
``watch`` CLI command to continuously refresh the live lineup display.
"""
import threading
from typing import Any, Callable


class Poller:
    """Repeatedly calls fetch_fn on a background daemon thread and passes results to callback."""

    def __init__(
        self,
        fetch_fn: Callable[[], Any],
        callback: Callable[[Any], None],
        interval: int = 60,
    ) -> None:
        self._fetch_fn = fetch_fn
        self._callback = callback
        self._interval = interval
        self._stop_event = threading.Event()
        self._thread: threading.Thread | None = None

    def start(self) -> None:
        """Start the background polling thread. Raises RuntimeError if already running."""
        if self._thread is not None and self._thread.is_alive():
            raise RuntimeError("Poller is already running")
        self._stop_event.clear()
        self._thread = threading.Thread(target=self._run, daemon=True)
        self._thread.start()

    def stop(self) -> None:
        """Signal the polling thread to stop and block until it exits."""
        self._stop_event.set()
        if self._thread is not None:
            self._thread.join()
            self._thread = None

    def is_running(self) -> bool:
        """Return True if the background thread is alive, False otherwise."""
        return self._thread is not None and self._thread.is_alive()

    def _run(self) -> None:
        while True:
            try:
                result = self._fetch_fn()
                self._callback(result)
            except Exception:
                pass  # keep polling; errors are swallowed to prevent thread death
            if self._stop_event.wait(timeout=self._interval):
                break
