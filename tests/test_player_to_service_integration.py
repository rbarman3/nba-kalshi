"""Integration tests for player_service using real static data (no network).

find_players_by_name uses the local nba_api static dictionary — safe to call
without mocking. find_players_by_team tests mock only _get_roster_for_team to
avoid the real HTTP call to commonteamroster.
"""
from dataclasses import FrozenInstanceError
from unittest.mock import patch

import pytest

from nba.models import Player
from nba.player_service import find_players_by_name, find_players_by_team


class TestFindPlayersByNameIntegration:
    def test_search_lebron_returns_at_least_one_player(self):
        results = find_players_by_name("LeBron")
        assert len(results) >= 1
        assert all(isinstance(p, Player) for p in results)

    def test_search_lebron_james_full_name_found(self):
        results = find_players_by_name("LeBron James")
        full_names = [p.full_name for p in results]
        assert "LeBron James" in full_names

    def test_returned_players_are_frozen(self):
        results = find_players_by_name("LeBron James")
        assert len(results) >= 1
        with pytest.raises(FrozenInstanceError):
            results[0].full_name = "mutated"  # type: ignore[misc]

    def test_empty_string_raises_value_error(self):
        with pytest.raises(ValueError):
            find_players_by_name("")

    def test_partial_name_james_returns_multiple_players(self):
        results = find_players_by_name("James")
        assert len(results) > 1

    def test_player_fields_are_correct_types(self):
        results = find_players_by_name("LeBron James")
        assert len(results) >= 1
        p = results[0]
        assert isinstance(p.id, int)
        assert isinstance(p.full_name, str)
        assert isinstance(p.first_name, str)
        assert isinstance(p.last_name, str)
        assert isinstance(p.is_active, bool)


FAKE_ROSTER = [
    {
        "id": 2544,
        "full_name": "LeBron James",
        "first_name": "LeBron",
        "last_name": "James",
        "is_active": True,
    },
    {
        "id": 1629029,
        "full_name": "Anthony Davis",
        "first_name": "Anthony",
        "last_name": "Davis",
        "is_active": True,
    },
]

FAKE_WARRIORS_ROSTER = [
    {
        "id": 201939,
        "full_name": "Stephen Curry",
        "first_name": "Stephen",
        "last_name": "Curry",
        "is_active": True,
    },
]


class TestFindPlayersByTeamIntegration:
    def test_lakers_lookup_uses_real_static_then_mocked_roster(self):
        with patch("nba.player_service._get_roster_for_team", return_value=FAKE_ROSTER):
            results = find_players_by_team("Los Angeles Lakers")
        assert len(results) == 2
        assert all(isinstance(p, Player) for p in results)
        assert results[0].full_name == "LeBron James"

    def test_unknown_team_returns_empty_list(self):
        results = find_players_by_team("Nonexistent Team XYZ")
        assert results == []

    def test_empty_team_name_raises_value_error(self):
        with pytest.raises(ValueError):
            find_players_by_team("")

    def test_returned_players_are_frozen(self):
        with patch("nba.player_service._get_roster_for_team", return_value=FAKE_ROSTER):
            results = find_players_by_team("Los Angeles Lakers")
        assert len(results) >= 1
        with pytest.raises(FrozenInstanceError):
            results[0].full_name = "mutated"  # type: ignore[misc]

    def test_partial_team_name_warriors_resolves(self):
        with patch("nba.player_service._get_roster_for_team", return_value=FAKE_WARRIORS_ROSTER):
            results = find_players_by_team("Golden State Warriors")
        assert len(results) == 1
        assert results[0].full_name == "Stephen Curry"
