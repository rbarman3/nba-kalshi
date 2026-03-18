import threading
import time
from unittest.mock import MagicMock

import pytest

from nba.poller import Poller


class TestPoller:
    def test_start_calls_fetch_fn_at_least_once(self):
        fetch_fn = MagicMock(return_value="data")
        callback = MagicMock()
        poller = Poller(fetch_fn, callback, interval=60)
        poller.start()
        # give the thread a moment to execute
        time.sleep(0.1)
        poller.stop()
        fetch_fn.assert_called()

    def test_callback_receives_fetch_fn_result(self):
        fetch_fn = MagicMock(return_value=["game1", "game2"])
        callback = MagicMock()
        poller = Poller(fetch_fn, callback, interval=60)
        poller.start()
        time.sleep(0.1)
        poller.stop()
        callback.assert_called_with(["game1", "game2"])

    def test_stop_halts_polling(self):
        call_count = []
        event = threading.Event()

        def fetch_fn():
            call_count.append(1)
            event.set()
            return "data"

        poller = Poller(fetch_fn, MagicMock(), interval=0.05)
        poller.start()
        event.wait(timeout=1)
        initial = len(call_count)
        poller.stop()
        time.sleep(0.15)  # wait longer than one interval
        assert len(call_count) <= initial + 1  # at most one more after stop

    def test_is_running_reflects_state(self):
        poller = Poller(MagicMock(return_value=None), MagicMock(), interval=60)
        assert not poller.is_running()
        poller.start()
        time.sleep(0.05)
        assert poller.is_running()
        poller.stop()
        assert not poller.is_running()

    def test_start_twice_raises_runtime_error(self):
        poller = Poller(MagicMock(return_value=None), MagicMock(), interval=60)
        poller.start()
        try:
            with pytest.raises(RuntimeError):
                poller.start()
        finally:
            poller.stop()

    def test_exception_in_fetch_fn_does_not_crash_thread(self):
        call_count = []
        succeeded = threading.Event()

        def fetch_fn():
            call_count.append(1)
            if len(call_count) == 1:
                raise ValueError("boom")
            succeeded.set()
            return "ok"

        callback = MagicMock()
        poller = Poller(fetch_fn, callback, interval=0.05)
        poller.start()
        succeeded.wait(timeout=2)
        poller.stop()
        # thread survived the exception and continued polling
        assert len(call_count) >= 2
