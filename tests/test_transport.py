"""Unit tests for the NBA transport layer."""
import asyncio
import time
from unittest.mock import AsyncMock, MagicMock, patch

import httpx
import pytest

from pipeline.models import RawSnapshot
from pipeline.transport import NBATransport


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
        mock_response.json.return_value = {"gameId": game_id, "homeTeam": {}, "awayTeam": {}}

        transport = NBATransport(game_ids=[game_id], queue=queue)

        with patch("pipeline.transport.httpx.AsyncClient.get", new_callable=AsyncMock) as mock_get:
            mock_get.return_value = mock_response
            mock_client = MagicMock()
            mock_client.get = mock_get
            await transport._fetch_one(game_id, mock_client)

        snapshot = await queue.get()
        assert isinstance(snapshot, RawSnapshot)
        assert snapshot.game_id == game_id
        assert snapshot.payload == {"gameId": game_id, "homeTeam": {}, "awayTeam": {}}
        assert isinstance(snapshot.fetched_at, float)

    @pytest.mark.asyncio
    async def test_fetch_one_timestamp_approximately_now(self, queue, game_ids):
        """Test that fetched_at is set to approximately the current time."""
        game_id = game_ids[0]
        mock_response = MagicMock()
        mock_response.json.return_value = {"gameId": game_id}

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
            mock_get.return_value.json.return_value = {}
            mock_client = MagicMock()
            mock_client.get = mock_get
            await transport._fetch_one(game_id, mock_client)

        expected_url = f"https://cdn.nba.com/static/json/liveData/boxscore/boxscore_{game_id}.json"
        mock_get.assert_called_once_with(expected_url)
