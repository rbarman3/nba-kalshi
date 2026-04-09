"""Tests for pipeline.espn_transport — ESPN play-by-play fetching for backtesting."""
import json
from pathlib import Path
from unittest.mock import AsyncMock, patch, MagicMock

import pytest

from pipeline.espn_transport import ESPNScraper
from pipeline.models import ESPNPlayByPlaySnapshot

FIXTURES = Path(__file__).parent / "fixtures"


@pytest.fixture
def pbp_payload() -> dict:
    return json.loads((FIXTURES / "espn_playbyplay_sample.json").read_text())


def _mock_client(payload: dict, status_code: int = 200, raise_on_status: bool = False) -> AsyncMock:
    resp = MagicMock()
    resp.status_code = status_code
    resp.json.return_value = payload
    if raise_on_status:
        resp.raise_for_status.side_effect = Exception(f"{status_code} error")
    else:
        resp.raise_for_status = MagicMock()

    client = AsyncMock()
    client.get = AsyncMock(return_value=resp)
    client.__aenter__ = AsyncMock(return_value=client)
    client.__aexit__ = AsyncMock(return_value=False)
    return client


class TestFetchGame:
    @pytest.mark.asyncio(loop_scope="function")
    async def test_returns_snapshot(self, pbp_payload):
        client = _mock_client(pbp_payload)
        with patch("pipeline.espn_transport.httpx.AsyncClient", return_value=client):
            scraper = ESPNScraper()
            snapshot = await scraper.fetch_game("401584793")

        assert isinstance(snapshot, ESPNPlayByPlaySnapshot)
        assert snapshot.espn_game_id == "401584793"
        assert snapshot.payload == pbp_payload

    @pytest.mark.asyncio(loop_scope="function")
    async def test_fetched_at_is_set(self, pbp_payload):
        client = _mock_client(pbp_payload)
        with patch("pipeline.espn_transport.httpx.AsyncClient", return_value=client):
            scraper = ESPNScraper()
            snapshot = await scraper.fetch_game("401584793")

        assert snapshot.fetched_at > 0

    @pytest.mark.asyncio(loop_scope="function")
    async def test_http_error_raises(self):
        client = _mock_client({}, status_code=404, raise_on_status=True)
        with patch("pipeline.espn_transport.httpx.AsyncClient", return_value=client):
            scraper = ESPNScraper()
            with pytest.raises(Exception, match="404"):
                await scraper.fetch_game("bad_id")


class TestFetchDate:
    @pytest.mark.asyncio(loop_scope="function")
    async def test_fetches_multiple_games(self, pbp_payload):
        client = _mock_client(pbp_payload)
        with patch("pipeline.espn_transport.httpx.AsyncClient", return_value=client):
            scraper = ESPNScraper()
            snapshots = await scraper.fetch_games(["401584793", "401584794"])

        assert len(snapshots) == 2
        assert snapshots[0].espn_game_id == "401584793"
        assert snapshots[1].espn_game_id == "401584794"

    @pytest.mark.asyncio(loop_scope="function")
    async def test_empty_game_ids_returns_empty(self):
        scraper = ESPNScraper()
        snapshots = await scraper.fetch_games([])
        assert snapshots == []
