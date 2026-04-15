"""Unit tests for the NBA transport layer."""
import asyncio
import time
from unittest.mock import AsyncMock, MagicMock, patch

import httpx
import pytest

from pipeline.api_stats import ApiStatsCollector
from pipeline.models import PollResult, RawSnapshot
from pipeline.signal import FeedQualitySignal
from pipeline.transport import GameStatus, NBATransport, NOT_STARTED_POLL_INTERVAL, _hash_payload


@pytest.fixture
def queue():
    """Provide a fresh asyncio.Queue for each test."""
    return asyncio.Queue()


@pytest.fixture
def game_ids():
    """Provide a list of game IDs."""
    return ["0022400001", "0022400002"]


class TestNBATransport:
    def test_init_stores_parameters(self, game_ids, queue):
        transport = NBATransport(game_ids=game_ids, queue=queue)
        assert transport.game_ids == game_ids
        assert transport.queue is queue
        assert transport.poll_interval_range == (0.6, 1.2)

    def test_custom_poll_interval_range(self, game_ids, queue):
        custom_range = (1.0, 2.0)
        transport = NBATransport(game_ids=game_ids, queue=queue, poll_interval_range=custom_range)
        assert transport.poll_interval_range == custom_range

    @pytest.mark.asyncio
    async def test_fetch_one_puts_raw_snapshot_on_queue(self, queue, game_ids):
        """Test that _fetch_one wraps a valid response in RawSnapshot and queues it."""
        game_id = game_ids[0]
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = {"game": {"gameId": game_id, "gameStatus": 2, "homeTeam": {}, "awayTeam": {}}}

        transport = NBATransport(game_ids=[game_id], queue=queue)

        with patch("pipeline.transport.httpx.AsyncClient.get", new_callable=AsyncMock) as mock_get:
            mock_get.return_value = mock_response
            mock_client = MagicMock()
            mock_client.get = mock_get
            await transport._fetch_one(game_id, mock_client)

        snapshot = await queue.get()
        assert isinstance(snapshot, RawSnapshot)
        assert snapshot.game_id == game_id
        assert snapshot.payload == {"game": {"gameId": game_id, "gameStatus": 2, "homeTeam": {}, "awayTeam": {}}}
        assert isinstance(snapshot.fetched_at, float)

    @pytest.mark.asyncio
    async def test_fetch_one_timestamp_approximately_now(self, queue, game_ids):
        """Test that fetched_at is set to approximately the current time."""
        game_id = game_ids[0]
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = {"game": {"gameId": game_id, "gameStatus": 2}}

        transport = NBATransport(game_ids=[game_id], queue=queue)

        before = time.time()
        with patch("pipeline.transport.httpx.AsyncClient.get", new_callable=AsyncMock) as mock_get:
            mock_get.return_value = mock_response
            mock_client = MagicMock()
            mock_client.get = mock_get
            await transport._fetch_one(game_id, mock_client)
        after = time.time()

        snapshot = await queue.get()
        assert before <= snapshot.fetched_at <= after

    @pytest.mark.asyncio
    async def test_fetch_one_handles_http_error(self, queue, game_ids):
        """Test that _fetch_one logs errors but doesn't crash on HTTP errors."""
        game_id = game_ids[0]
        transport = NBATransport(game_ids=[game_id], queue=queue)

        with patch("pipeline.transport.httpx.AsyncClient.get", new_callable=AsyncMock) as mock_get:
            mock_get.side_effect = httpx.HTTPError("Connection failed")
            mock_client = MagicMock()
            mock_client.get = mock_get
            # Should not raise, just log
            await transport._fetch_one(game_id, mock_client)

        # Queue should be empty (no snapshot added)
        assert queue.empty()

    @pytest.mark.asyncio
    async def test_fetch_one_logs_on_error(self, queue, game_ids):
        """Test that _fetch_one logs the error."""
        game_id = game_ids[0]
        transport = NBATransport(game_ids=[game_id], queue=queue)

        with patch("pipeline.transport.httpx.AsyncClient.get", new_callable=AsyncMock) as mock_get:
            mock_get.side_effect = httpx.HTTPError("Connection failed")
            mock_client = MagicMock()
            mock_client.get = mock_get
            with patch("pipeline.transport.logger") as mock_logger:
                await transport._fetch_one(game_id, mock_client)
                mock_logger.error.assert_called_once()

    @pytest.mark.asyncio
    async def test_headers_class_constant_has_required_fields(self):
        """Test that HEADERS has required fields for the transport."""
        assert "Host" in NBATransport.HEADERS
        assert "User-Agent" in NBATransport.HEADERS
        assert "Sec-Ch-Ua" in NBATransport.HEADERS
        assert NBATransport.HEADERS["Host"] == "cdn.nba.com"

    @pytest.mark.asyncio
    async def test_fetch_constructs_correct_url(self, queue, game_ids):
        """Test that _fetch_one constructs the correct CDN URL."""
        game_id = game_ids[0]
        transport = NBATransport(game_ids=[game_id], queue=queue)

        with patch("pipeline.transport.httpx.AsyncClient.get", new_callable=AsyncMock) as mock_get:
            mock_get.return_value = MagicMock()
            mock_get.return_value.status_code = 200
            mock_get.return_value.json.return_value = {"game": {"gameStatus": 2}}
            mock_client = MagicMock()
            mock_client.get = mock_get
            await transport._fetch_one(game_id, mock_client)

        expected_url = f"https://cdn.nba.com/static/json/liveData/boxscore/boxscore_{game_id}.json"
        mock_get.assert_called_once_with(expected_url)


class TestTransportCacheAndPolling:
    @pytest.fixture
    def game_id(self):
        return "0022400001"

    def _make_mock_client(self, status_code: int, payload: dict) -> MagicMock:
        resp = MagicMock()
        resp.status_code = status_code
        resp.json.return_value = payload
        client = MagicMock()
        client.get = AsyncMock(return_value=resp)
        return client

    def test_cache_empty_initially(self):
        transport = NBATransport(game_ids=["0022400001"], queue=asyncio.Queue())
        assert transport.cache == {}

    @pytest.mark.asyncio
    async def test_live_game_updates_cache(self, game_id):
        queue = asyncio.Queue()
        transport = NBATransport(game_ids=[game_id], queue=queue)
        client = self._make_mock_client(200, {"game": {"gameStatus": 2}})

        await transport._fetch_one(game_id, client)

        assert game_id in transport.cache
        assert transport.cache[game_id].game_id == game_id
        assert transport._poll_states[game_id].status == GameStatus.LIVE

    @pytest.mark.asyncio
    async def test_403_marks_not_started(self, game_id):
        queue = asyncio.Queue()
        transport = NBATransport(game_ids=[game_id], queue=queue)
        client = self._make_mock_client(403, {})

        await transport._fetch_one(game_id, client)

        assert transport._poll_states[game_id].status == GameStatus.NOT_STARTED
        assert queue.empty()
        assert game_id not in transport.cache

    @pytest.mark.asyncio
    async def test_not_started_throttled_within_interval(self, game_id):
        queue = asyncio.Queue()
        transport = NBATransport(game_ids=[game_id], queue=queue)
        transport._poll_states[game_id].status = GameStatus.NOT_STARTED
        transport._poll_states[game_id].last_polled = time.time()  # just polled

        client = MagicMock()
        client.get = AsyncMock()

        await transport._fetch_one(game_id, client)

        client.get.assert_not_called()
        assert queue.empty()

    @pytest.mark.asyncio
    async def test_not_started_polls_after_interval(self, game_id):
        queue = asyncio.Queue()
        transport = NBATransport(game_ids=[game_id], queue=queue)
        transport._poll_states[game_id].status = GameStatus.NOT_STARTED
        transport._poll_states[game_id].last_polled = time.time() - NOT_STARTED_POLL_INTERVAL - 1

        client = self._make_mock_client(200, {"game": {"gameStatus": 2}})

        await transport._fetch_one(game_id, client)

        client.get.assert_called_once()

    @pytest.mark.asyncio
    async def test_final_game_never_fetches(self, game_id):
        queue = asyncio.Queue()
        transport = NBATransport(game_ids=[game_id], queue=queue)
        transport._poll_states[game_id].status = GameStatus.FINAL

        client = MagicMock()
        client.get = AsyncMock()

        await transport._fetch_one(game_id, client)

        client.get.assert_not_called()
        assert queue.empty()

    @pytest.mark.asyncio
    async def test_200_final_status_marks_final(self, game_id):
        queue = asyncio.Queue()
        transport = NBATransport(game_ids=[game_id], queue=queue)
        client = self._make_mock_client(200, {"game": {"gameStatus": 3}})

        await transport._fetch_one(game_id, client)

        assert transport._poll_states[game_id].status == GameStatus.FINAL
        assert queue.empty()
        assert game_id not in transport.cache


class TestHashPayload:
    def test_same_payload_produces_same_hash(self):
        payload = {"game": {"gameStatus": 2, "score": 10}}
        assert _hash_payload(payload) == _hash_payload(payload)

    def test_different_payloads_produce_different_hashes(self):
        p1 = {"game": {"gameStatus": 2, "score": 10}}
        p2 = {"game": {"gameStatus": 2, "score": 11}}
        assert _hash_payload(p1) != _hash_payload(p2)

    def test_key_order_does_not_affect_hash(self):
        p1 = {"b": 2, "a": 1}
        p2 = {"a": 1, "b": 2}
        assert _hash_payload(p1) == _hash_payload(p2)

    def test_returns_string(self):
        assert isinstance(_hash_payload({}), str)


class TestDeduplication:
    PAYLOAD = {"game": {"gameStatus": 2, "score": 10}}
    CHANGED_PAYLOAD = {"game": {"gameStatus": 2, "score": 11}}

    def _make_mock_client(self, payload: dict) -> MagicMock:
        resp = MagicMock()
        resp.status_code = 200
        resp.json.return_value = payload
        client = MagicMock()
        client.get = AsyncMock(return_value=resp)
        return client

    @pytest.mark.asyncio
    async def test_first_fetch_queues_snapshot(self):
        game_id = "0022400001"
        queue = asyncio.Queue()
        transport = NBATransport(game_ids=[game_id], queue=queue)

        await transport._fetch_one(game_id, self._make_mock_client(self.PAYLOAD))

        assert queue.qsize() == 1

    @pytest.mark.asyncio
    async def test_identical_payload_not_requeued(self):
        game_id = "0022400001"
        queue = asyncio.Queue()
        transport = NBATransport(game_ids=[game_id], queue=queue)
        client = self._make_mock_client(self.PAYLOAD)

        await transport._fetch_one(game_id, client)
        await transport._fetch_one(game_id, client)

        assert queue.qsize() == 1

    @pytest.mark.asyncio
    async def test_changed_payload_is_requeued(self):
        game_id = "0022400001"
        queue = asyncio.Queue()
        transport = NBATransport(game_ids=[game_id], queue=queue)

        await transport._fetch_one(game_id, self._make_mock_client(self.PAYLOAD))
        await transport._fetch_one(game_id, self._make_mock_client(self.CHANGED_PAYLOAD))

        assert queue.qsize() == 2

    @pytest.mark.asyncio
    async def test_cache_not_updated_on_duplicate(self):
        game_id = "0022400001"
        queue = asyncio.Queue()
        transport = NBATransport(game_ids=[game_id], queue=queue)

        await transport._fetch_one(game_id, self._make_mock_client(self.PAYLOAD))
        first_snapshot = transport.cache[game_id]

        await transport._fetch_one(game_id, self._make_mock_client(self.PAYLOAD))

        assert transport.cache[game_id] is first_snapshot

    @pytest.mark.asyncio
    async def test_hash_stored_after_first_fetch(self):
        game_id = "0022400001"
        transport = NBATransport(game_ids=[game_id], queue=asyncio.Queue())

        await transport._fetch_one(game_id, self._make_mock_client(self.PAYLOAD))

        assert transport._poll_states[game_id].last_payload_hash == _hash_payload(self.PAYLOAD)


class TestStatsEmission:
    """Test that transport emits PollResult to the stats collector."""

    GAME_ID = "0022400001"
    LIVE_PAYLOAD = {"game": {"gameStatus": 2, "score": 10}}

    def _make_mock_client(self, status_code: int, payload: dict | None = None) -> MagicMock:
        resp = MagicMock()
        resp.status_code = status_code
        resp.json.return_value = payload or {}
        client = MagicMock()
        client.get = AsyncMock(return_value=resp)
        return client

    def _make_error_client(self, error: Exception) -> MagicMock:
        client = MagicMock()
        client.get = AsyncMock(side_effect=error)
        return client

    @pytest.mark.asyncio
    async def test_emits_poll_result_on_200(self):
        stats = ApiStatsCollector(base_dir="/tmp/unused")
        transport = NBATransport(
            game_ids=[self.GAME_ID], queue=asyncio.Queue(), stats=stats,
        )
        client = self._make_mock_client(200, self.LIVE_PAYLOAD)

        await transport._fetch_one(self.GAME_ID, client)

        assert len(stats._buffer) == 1
        assert stats._buffer[0].status_code == 200
        assert stats._buffer[0].game_id == self.GAME_ID
        assert stats._buffer[0].error_type is None

    @pytest.mark.asyncio
    async def test_emits_poll_result_on_403(self):
        stats = ApiStatsCollector(base_dir="/tmp/unused")
        transport = NBATransport(
            game_ids=[self.GAME_ID], queue=asyncio.Queue(), stats=stats,
        )
        client = self._make_mock_client(403)

        await transport._fetch_one(self.GAME_ID, client)

        assert len(stats._buffer) == 1
        assert stats._buffer[0].status_code == 403

    @pytest.mark.asyncio
    async def test_emits_poll_result_on_exception(self):
        stats = ApiStatsCollector(base_dir="/tmp/unused")
        transport = NBATransport(
            game_ids=[self.GAME_ID], queue=asyncio.Queue(), stats=stats,
        )
        client = self._make_error_client(httpx.TimeoutException("timed out"))

        await transport._fetch_one(self.GAME_ID, client)

        assert len(stats._buffer) == 1
        assert stats._buffer[0].status_code == -1
        assert stats._buffer[0].error_type == "TimeoutException"

    @pytest.mark.asyncio
    async def test_no_emit_when_stats_is_none(self):
        transport = NBATransport(
            game_ids=[self.GAME_ID], queue=asyncio.Queue(), stats=None,
        )
        client = self._make_mock_client(200, self.LIVE_PAYLOAD)

        # Should not raise
        await transport._fetch_one(self.GAME_ID, client)

    @pytest.mark.asyncio
    async def test_no_emit_for_final_game(self):
        stats = ApiStatsCollector(base_dir="/tmp/unused")
        transport = NBATransport(
            game_ids=[self.GAME_ID], queue=asyncio.Queue(), stats=stats,
        )
        transport._poll_states[self.GAME_ID].status = GameStatus.FINAL

        client = MagicMock()
        client.get = AsyncMock()

        await transport._fetch_one(self.GAME_ID, client)

        assert len(stats._buffer) == 0
        client.get.assert_not_called()

    @pytest.mark.asyncio
    async def test_no_emit_for_throttled_not_started(self):
        stats = ApiStatsCollector(base_dir="/tmp/unused")
        transport = NBATransport(
            game_ids=[self.GAME_ID], queue=asyncio.Queue(), stats=stats,
        )
        transport._poll_states[self.GAME_ID].status = GameStatus.NOT_STARTED
        transport._poll_states[self.GAME_ID].last_polled = time.time()

        client = MagicMock()
        client.get = AsyncMock()

        await transport._fetch_one(self.GAME_ID, client)

        assert len(stats._buffer) == 0
        client.get.assert_not_called()

    @pytest.mark.asyncio
    async def test_response_time_positive(self):
        stats = ApiStatsCollector(base_dir="/tmp/unused")
        transport = NBATransport(
            game_ids=[self.GAME_ID], queue=asyncio.Queue(), stats=stats,
        )
        client = self._make_mock_client(200, self.LIVE_PAYLOAD)

        await transport._fetch_one(self.GAME_ID, client)

        assert stats._buffer[0].response_time_ms > 0


class TestSignalIntegration:
    """Test that transport calls FeedQualitySignal hooks."""

    GAME_ID = "0022400001"
    LIVE_PAYLOAD = {"game": {"gameStatus": 2, "score": 10}}
    CHANGED_PAYLOAD = {"game": {"gameStatus": 2, "score": 11}}

    def _make_mock_client(self, status_code: int, payload: dict | None = None) -> MagicMock:
        resp = MagicMock()
        resp.status_code = status_code
        resp.json.return_value = payload or {}
        client = MagicMock()
        client.get = AsyncMock(return_value=resp)
        return client

    @pytest.mark.asyncio
    async def test_signal_on_poll_called_on_200(self):
        signal = FeedQualitySignal()
        transport = NBATransport(
            game_ids=[self.GAME_ID], queue=asyncio.Queue(), signal=signal,
        )
        client = self._make_mock_client(200, self.LIVE_PAYLOAD)

        await transport._fetch_one(self.GAME_ID, client)

        assert len(signal._states[self.GAME_ID].polls) == 1
        assert signal._states[self.GAME_ID].polls[0][1] == 200

    @pytest.mark.asyncio
    async def test_signal_on_snapshot_called_on_new(self):
        signal = FeedQualitySignal()
        transport = NBATransport(
            game_ids=[self.GAME_ID], queue=asyncio.Queue(), signal=signal,
        )
        client = self._make_mock_client(200, self.LIVE_PAYLOAD)

        await transport._fetch_one(self.GAME_ID, client)

        assert signal._states[self.GAME_ID].last_snapshot_at > 0

    @pytest.mark.asyncio
    async def test_signal_on_snapshot_not_called_on_duplicate(self):
        signal = FeedQualitySignal()
        transport = NBATransport(
            game_ids=[self.GAME_ID], queue=asyncio.Queue(), signal=signal,
        )
        client = self._make_mock_client(200, self.LIVE_PAYLOAD)

        await transport._fetch_one(self.GAME_ID, client)
        first_snapshot_at = signal._states[self.GAME_ID].last_snapshot_at

        # Same payload — duplicate, should NOT call on_snapshot again
        await transport._fetch_one(self.GAME_ID, client)
        assert signal._states[self.GAME_ID].last_snapshot_at == first_snapshot_at

    @pytest.mark.asyncio
    async def test_no_signal_when_none(self):
        transport = NBATransport(
            game_ids=[self.GAME_ID], queue=asyncio.Queue(), signal=None,
        )
        client = self._make_mock_client(200, self.LIVE_PAYLOAD)

        # Should not raise
        await transport._fetch_one(self.GAME_ID, client)
