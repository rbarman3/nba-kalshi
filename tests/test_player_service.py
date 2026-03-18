"""
Tests for player_service.py — written before implementation (TDD RED phase).
Static data from nba_api.stats.static is used; no network calls are made.
"""
import pytest
from unittest.mock import patch
from nba.models import Player
from nba.player_service import find_players_by_name, find_players_by_team


# ---------------------------------------------------------------------------
# Fixtures / shared data
# ---------------------------------------------------------------------------

MOCK_PLAYERS = [
    {"id": 2544, "full_name": "LeBron James", "first_name": "LeBron",
     "last_name": "James", "is_active": True},
    {"id": 201939, "full_name": "Stephen Curry", "first_name": "Stephen",
     "last_name": "Curry", "is_active": True},
    {"id": 203999, "full_name": "Nikola Jokic", "first_name": "Nikola",
     "last_name": "Jokic", "is_active": True},
    {"id": 1629029, "full_name": "Luka Doncic", "first_name": "Luka",
     "last_name": "Doncic", "is_active": True},
    {"id": 977, "full_name": "James Harden", "first_name": "James",
     "last_name": "Harden", "is_active": False},
]

MOCK_TEAMS = [
    {"id": 1610612747, "full_name": "Los Angeles Lakers", "abbreviation": "LAL",
     "nickname": "Lakers", "city": "Los Angeles", "state": "California",
     "year_founded": 1948},
    {"id": 1610612744, "full_name": "Golden State Warriors", "abbreviation": "GSW",
     "nickname": "Warriors", "city": "San Francisco", "state": "California",
     "year_founded": 1946},
]

# Minimal CommonPlayerInfo-style roster stub: maps team_id → list of player dicts
MOCK_ROSTER = {
    1610612747: [  # Lakers
        {"id": 2544, "full_name": "LeBron James", "first_name": "LeBron",
         "last_name": "James", "is_active": True},
    ],
    1610612744: [  # Warriors
        {"id": 201939, "full_name": "Stephen Curry", "first_name": "Stephen",
         "last_name": "Curry", "is_active": True},
    ],
}


# ---------------------------------------------------------------------------
# find_players_by_name
# ---------------------------------------------------------------------------

class TestFindPlayersByName:
    def test_exact_full_name_returns_player(self):
        with patch("nba.player_service.players_static.find_players_by_full_name",
                   return_value=[MOCK_PLAYERS[0]]):
            results = find_players_by_name("LeBron James")

        assert len(results) == 1
        assert results[0].full_name == "LeBron James"
        assert results[0].id == 2544

    def test_returns_player_dataclass(self):
        with patch("nba.player_service.players_static.find_players_by_full_name",
                   return_value=[MOCK_PLAYERS[0]]):
            results = find_players_by_name("LeBron James")

        assert isinstance(results[0], Player)

    def test_partial_name_returns_multiple(self):
        james_players = [MOCK_PLAYERS[0], MOCK_PLAYERS[4]]  # LeBron James + James Harden
        with patch("nba.player_service.players_static.find_players_by_full_name",
                   return_value=james_players):
            results = find_players_by_name("James")

        assert len(results) == 2

    def test_no_match_returns_empty_list(self):
        with patch("nba.player_service.players_static.find_players_by_full_name",
                   return_value=[]):
            results = find_players_by_name("Nonexistent Player")

        assert results == []

    def test_empty_string_raises_value_error(self):
        with pytest.raises(ValueError, match="name cannot be empty"):
            find_players_by_name("")

    def test_whitespace_only_raises_value_error(self):
        with pytest.raises(ValueError, match="name cannot be empty"):
            find_players_by_name("   ")

    def test_player_active_flag_preserved(self):
        with patch("nba.player_service.players_static.find_players_by_full_name",
                   return_value=[MOCK_PLAYERS[4]]):  # James Harden, inactive
            results = find_players_by_name("James Harden")

        assert results[0].is_active is False


# ---------------------------------------------------------------------------
# find_players_by_team
# ---------------------------------------------------------------------------

class TestFindPlayersByTeam:
    def _make_patch(self, team_results, roster):
        """Helper: patches teams static + roster lookup."""
        return (
            patch("nba.player_service.teams_static.find_teams_by_full_name",
                  return_value=team_results),
            patch("nba.player_service._get_roster_for_team",
                  side_effect=lambda team_id: roster.get(team_id, [])),
        )

    def test_returns_players_for_valid_team(self):
        p1, p2 = patch("nba.player_service.teams_static.find_teams_by_full_name",
                       return_value=[MOCK_TEAMS[0]]), \
                 patch("nba.player_service._get_roster_for_team",
                       return_value=[MOCK_ROSTER[1610612747][0]])
        with p1, p2:
            results = find_players_by_team("Los Angeles Lakers")

        assert len(results) == 1
        assert results[0].full_name == "LeBron James"

    def test_returns_player_dataclass_instances(self):
        p1, p2 = patch("nba.player_service.teams_static.find_teams_by_full_name",
                       return_value=[MOCK_TEAMS[0]]), \
                 patch("nba.player_service._get_roster_for_team",
                       return_value=[MOCK_ROSTER[1610612747][0]])
        with p1, p2:
            results = find_players_by_team("Los Angeles Lakers")

        assert all(isinstance(r, Player) for r in results)

    def test_unknown_team_returns_empty_list(self):
        with patch("nba.player_service.teams_static.find_teams_by_full_name",
                   return_value=[]):
            results = find_players_by_team("Nonexistent Team")

        assert results == []

    def test_empty_team_name_raises_value_error(self):
        with pytest.raises(ValueError, match="team_name cannot be empty"):
            find_players_by_team("")

    def test_whitespace_team_name_raises_value_error(self):
        with pytest.raises(ValueError, match="team_name cannot be empty"):
            find_players_by_team("   ")

    def test_partial_team_name_match(self):
        p1, p2 = patch("nba.player_service.teams_static.find_teams_by_full_name",
                       return_value=[MOCK_TEAMS[1]]), \
                 patch("nba.player_service._get_roster_for_team",
                       return_value=[MOCK_ROSTER[1610612744][0]])
        with p1, p2:
            results = find_players_by_team("Warriors")

        assert results[0].full_name == "Stephen Curry"
