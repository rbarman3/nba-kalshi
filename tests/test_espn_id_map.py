"""Tests for pipeline.espn_id_map — ESPN game discovery."""
import json
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from pipeline.espn_id_map import discover_espn_game_ids, ESPNGame

FIXTURES = Path(__file__).parent / "fixtures"


@pytest.fixture
def scoreboard_payload() -> dict:
    return json.loads((FIXTURES / "espn_scoreboard_sample.json").read_text())


def _mock_client(payload: dict) -> MagicMock:
    mock_resp = MagicMock()
    mock_resp.json.return_value = payload
    mock_resp.raise_for_status = MagicMock()

    client = AsyncMock()
    client.get = AsyncMock(return_value=mock_resp)
    client.__aenter__ = AsyncMock(return_value=client)
    client.__aexit__ = AsyncMock(return_value=False)
    return client


class TestDiscoverESPNGameIds:
    @pytest.mark.asyncio(loop_scope="function")
    async def test_returns_all_games(self, scoreboard_payload):
        client = _mock_client(scoreboard_payload)
        with patch("pipeline.espn_id_map.httpx.AsyncClient", return_value=client):
            games = await discover_espn_game_ids()
        assert len(games) == 3

    @pytest.mark.asyncio(loop_scope="function")
    async def test_parses_game_fields(self, scoreboard_payload):
        client = _mock_client(scoreboard_payload)
        with patch("pipeline.espn_id_map.httpx.AsyncClient", return_value=client):
            games = await discover_espn_game_ids()

        first = games[0]
        assert isinstance(first, ESPNGame)
        assert first.espn_game_id == "401584793"
        assert first.home_team == "LAL"
        assert first.away_team == "BOS"
        assert first.status == "in"

    @pytest.mark.asyncio(loop_scope="function")
    async def test_classifies_statuses(self, scoreboard_payload):
        client = _mock_client(scoreboard_payload)
        with patch("pipeline.espn_id_map.httpx.AsyncClient", return_value=client):
            games = await discover_espn_game_ids()

        statuses = {g.espn_game_id: g.status for g in games}
        assert statuses["401584793"] == "in"
        assert statuses["401584794"] == "pre"
        assert statuses["401584795"] == "post"

    @pytest.mark.asyncio(loop_scope="function")
    async def test_passes_date_param(self, scoreboard_payload):
        client = _mock_client(scoreboard_payload)
        with patch("pipeline.espn_id_map.httpx.AsyncClient", return_value=client):
            await discover_espn_game_ids(date="2026-03-29")

        call_args = client.get.call_args
        assert "20260329" in call_args[1].get("params", {}).get("dates", "")

    @pytest.mark.asyncio(loop_scope="function")
    async def test_empty_scoreboard(self):
        client = _mock_client({"events": []})
        with patch("pipeline.espn_id_map.httpx.AsyncClient", return_value=client):
            games = await discover_espn_game_ids()
        assert games == []
