"""Tests for the _get_roster_for_team helper (no network calls)."""
import pandas as pd
from unittest.mock import patch, MagicMock
from nba.player_service import _get_roster_for_team


def _make_roster_endpoint(rows: list[dict]) -> MagicMock:
    df = pd.DataFrame(rows)
    endpoint = MagicMock()
    endpoint.common_team_roster.get_data_frame.return_value = df
    return endpoint


class TestGetRosterForTeam:
    def test_returns_list_of_dicts(self):
        endpoint = _make_roster_endpoint([
            {"PLAYER_ID": 2544, "PLAYER": "LeBron James"},
        ])
        with patch("nba.player_service.commonteamroster.CommonTeamRoster",
                   return_value=endpoint):
            result = _get_roster_for_team(1610612747)

        assert isinstance(result, list)
        assert len(result) == 1

    def test_player_dict_fields_populated(self):
        endpoint = _make_roster_endpoint([
            {"PLAYER_ID": 2544, "PLAYER": "LeBron James"},
        ])
        with patch("nba.player_service.commonteamroster.CommonTeamRoster",
                   return_value=endpoint):
            result = _get_roster_for_team(1610612747)

        p = result[0]
        assert p["id"] == 2544
        assert p["full_name"] == "LeBron James"
        assert p["first_name"] == "LeBron"
        assert p["last_name"] == "James"
        assert p["is_active"] is True

    def test_multiple_players_returned(self):
        endpoint = _make_roster_endpoint([
            {"PLAYER_ID": 2544, "PLAYER": "LeBron James"},
            {"PLAYER_ID": 1629029, "PLAYER": "Anthony Davis"},
        ])
        with patch("nba.player_service.commonteamroster.CommonTeamRoster",
                   return_value=endpoint):
            result = _get_roster_for_team(1610612747)

        assert len(result) == 2

    def test_empty_roster_returns_empty_list(self):
        endpoint = _make_roster_endpoint([])
        with patch("nba.player_service.commonteamroster.CommonTeamRoster",
                   return_value=endpoint):
            result = _get_roster_for_team(1610612747)

        assert result == []
