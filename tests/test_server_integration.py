"""Integration tests for the FastAPI server — real NBA API, no mocking."""
import pytest
from fastapi.testclient import TestClient

from nba.server import app

client = TestClient(app)


class TestScoreboardEndpoint:
    def test_returns_200(self):
        response = client.get("/scoreboard")
        assert response.status_code == 200

    def test_response_has_games_key(self):
        response = client.get("/scoreboard")
        assert "games" in response.json()

    def test_games_is_a_list(self):
        data = client.get("/scoreboard").json()
        assert isinstance(data["games"], list)

    def test_game_fields_present_when_games_exist(self):
        data = client.get("/scoreboard").json()
        games = data["games"]
        if not games:
            pytest.skip("No games scheduled today")
        first = games[0]
        for field in ("game_id", "home_team", "away_team", "home_score", "away_score", "status"):
            assert field in first, f"Missing field: {field}"


class TestPlayersEndpoint:
    def test_search_lebron_returns_players(self):
        response = client.get("/players", params={"name": "LeBron"})
        assert response.status_code == 200
        data = response.json()
        assert len(data["players"]) >= 1

    def test_player_has_expected_fields(self):
        response = client.get("/players", params={"name": "LeBron"})
        first = response.json()["players"][0]
        for field in ("id", "full_name", "first_name", "last_name", "is_active"):
            assert field in first, f"Missing field: {field}"

    def test_empty_name_returns_422(self):
        response = client.get("/players", params={"name": ""})
        assert response.status_code == 422

    def test_missing_name_param_returns_422(self):
        response = client.get("/players")
        assert response.status_code == 422

    def test_search_partial_name_james_returns_multiple(self):
        response = client.get("/players", params={"name": "James"})
        assert response.status_code == 200
        assert len(response.json()["players"]) > 1


class TestPlayersByTeamEndpoint:
    def test_lakers_returns_players(self):
        response = client.get("/players/team", params={"name": "Los Angeles Lakers"})
        assert response.status_code == 200
        data = response.json()
        assert len(data["players"]) >= 1

    def test_unknown_team_returns_empty_list(self):
        response = client.get("/players/team", params={"name": "Nonexistent XYZ"})
        assert response.status_code == 200
        assert response.json()["players"] == []

    def test_empty_name_returns_422(self):
        response = client.get("/players/team", params={"name": ""})
        assert response.status_code == 422
