"""ESPN play-by-play processor — classifies raw plays into typed events.

Parses the plays array from an ESPN play-by-play snapshot and emits
typed event objects for downstream consumption by the backtesting pipeline.

Classification rules:
  - scoringPlay == true  → ScoringPlayEvent
  - type.id in FOUL_IDS  → FoulEvent
  - type.id in TIMEOUT_IDS → TimeoutEvent
  - type.id in TURNOVER_IDS → TurnoverEvent
  - type.id == "412"     → PeriodEvent (start or end based on text)
  - type.id == "584"     → SubstitutionEvent
  - Everything else      → Skipped with debug log
"""
import logging
from typing import Optional

from .models import (
    ESPNPlayByPlaySnapshot,
    FoulEvent,
    PeriodEvent,
    ScoringPlayEvent,
    SubstitutionEvent,
    TimeoutEvent,
    TurnoverEvent,
)

logger = logging.getLogger(__name__)

# ESPN play type IDs by category
FOUL_IDS = frozenset({"44", "45", "46", "37", "38", "39", "6"})
TIMEOUT_IDS = frozenset({"16", "17", "18"})
TURNOVER_IDS = frozenset({"62", "64", "87", "56", "52"})
PERIOD_ID = "412"
SUBSTITUTION_ID = "584"


def _extract_player_id(play: dict, index: int = 0) -> str:
    """Extract a participant's athlete ID by index, or empty string."""
    participants = play.get("participants", [])
    if len(participants) > index and "athlete" in participants[index]:
        return participants[index]["athlete"]["id"]
    return ""


def _classify_play(
    game_id: str, play: dict, observed_at: float
) -> Optional[object]:
    """Classify a single ESPN play into a typed event.

    Returns None for plays that should be skipped (substitutions, unknown types).
    """
    type_id = play.get("type", {}).get("id", "")
    type_text = play.get("type", {}).get("text", "")
    team_id = play.get("team", {}).get("id", "")

    common = dict(
        game_id=game_id,
        espn_play_id=play.get("id", ""),
        sequence=play.get("sequenceNumber", 0),
        period=play.get("period", {}).get("number", 0),
        clock=play.get("clock", {}).get("displayValue", ""),
        home_score=int(play.get("homeScore", "0")),
        away_score=int(play.get("awayScore", "0")),
        text=play.get("text", ""),
        wallclock=play.get("wallclock", ""),
        observed_at=observed_at,
    )

    # Scoring plays — use the boolean flag as primary discriminator
    if play.get("scoringPlay"):
        return ScoringPlayEvent(
            **common,
            score_value=int(play.get("scoreValue", 0)),
            team_id=team_id,
            player_id=_extract_player_id(play),
            play_type=type_text,
        )

    # Period start/end
    if type_id == PERIOD_ID:
        text_lower = play.get("text", "").lower()
        event_type = "end" if "end" in text_lower else "start"
        return PeriodEvent(**common, event_type=event_type)

    # Fouls
    if type_id in FOUL_IDS:
        return FoulEvent(
            **common,
            team_id=team_id,
            player_id=_extract_player_id(play),
            foul_type=type_text,
        )

    # Timeouts
    if type_id in TIMEOUT_IDS:
        return TimeoutEvent(
            **common,
            team_id=team_id,
            timeout_type=type_text,
        )

    # Turnovers
    if type_id in TURNOVER_IDS:
        return TurnoverEvent(
            **common,
            team_id=team_id,
            player_id=_extract_player_id(play),
            turnover_type=type_text,
        )

    # Substitutions
    if type_id == SUBSTITUTION_ID:
        return SubstitutionEvent(
            **common,
            team_id=team_id,
            player_in_id=_extract_player_id(play, 0),
            player_out_id=_extract_player_id(play, 1),
        )

    # Unknown type — log and skip
    logger.debug(
        "Skipping unknown play type %s (%s) in game %s",
        type_id, type_text, game_id,
    )
    return None


class ESPNPlayByPlayProcessor:
    """Process ESPN play-by-play snapshots into typed events.

    Stateless processor for batch/historical use. Each call to
    process_snapshot or process_plays is independent.
    """

    def process_plays(
        self,
        game_id: str,
        plays: list[dict],
        observed_at: float,
    ) -> list:
        """Classify a list of raw ESPN plays into typed events.

        Args:
            game_id: ESPN game ID.
            plays: List of raw play dicts from ESPN API.
            observed_at: Unix timestamp for event attribution.

        Returns:
            List of typed events, ordered by sequence number.
        """
        events = []
        for play in plays:
            event = _classify_play(game_id, play, observed_at)
            if event is not None:
                events.append(event)
        return events

    def process_snapshot(
        self, snapshot: ESPNPlayByPlaySnapshot
    ) -> list:
        """Extract and classify all plays from an ESPN snapshot.

        Args:
            snapshot: ESPNPlayByPlaySnapshot from ESPNScraper.

        Returns:
            List of typed events, ordered by sequence number.
        """
        plays = (
            snapshot.payload
            .get("gamepackageJSON", {})
            .get("plays", [])
        )
        return self.process_plays(
            snapshot.espn_game_id, plays, snapshot.fetched_at
        )
