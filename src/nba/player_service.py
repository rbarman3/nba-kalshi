"""
Player lookup services backed by the nba_api library.

Public API
----------
find_players_by_name(name)      -- search static player dictionary (no network)
find_players_by_team(team_name) -- fetch live team roster (network via CommonTeamRoster)

Mock target for tests: ``nba.player_service._get_roster_for_team``
"""
from typing import List

from nba_api.stats.static import players as players_static
from nba_api.stats.static import teams as teams_static
from nba_api.stats.endpoints import commonteamroster

from nba.models import Player


def _player_from_dict(data: dict) -> Player:
    return Player(
        id=data["id"],
        full_name=data["full_name"],
        first_name=data["first_name"],
        last_name=data["last_name"],
        is_active=data["is_active"],
    )


def _get_roster_for_team(team_id: int) -> List[dict]:
    """Fetch current roster for a team from the NBA API."""
    roster = commonteamroster.CommonTeamRoster(team_id=team_id)
    players_df = roster.common_team_roster.get_data_frame()
    result = []
    for _, row in players_df.iterrows():
        result.append({
            "id": int(row["PLAYER_ID"]),
            "full_name": row["PLAYER"],
            "first_name": row["PLAYER"].split()[0] if row["PLAYER"] else "",
            "last_name": " ".join(row["PLAYER"].split()[1:]) if row["PLAYER"] else "",
            "is_active": True,
        })
    return result


def find_players_by_name(name: str) -> List[Player]:
    """Search for players whose full name matches (partial, case-insensitive)."""
    if not name or not name.strip():
        raise ValueError("name cannot be empty")

    raw = players_static.find_players_by_full_name(name.strip())
    return [_player_from_dict(p) for p in raw]


def find_players_by_team(team_name: str) -> List[Player]:
    """Return players whose current team matches the given team name."""
    if not team_name or not team_name.strip():
        raise ValueError("team_name cannot be empty")

    matched_teams = teams_static.find_teams_by_full_name(team_name.strip())
    if not matched_teams:
        return []

    team_id = matched_teams[0]["id"]
    roster = _get_roster_for_team(team_id)
    return [_player_from_dict(p) for p in roster]
