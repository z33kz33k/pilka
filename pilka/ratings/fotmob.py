"""

    pilka.ratings.fotmob
    ~~~~~~~~~~~~~~~~~~~~
    Scrape Ekstraklasa players' ratings data from Fotmob.

    @author: z33k

"""
from datetime import datetime

from pilka.ratings import Country, Match, Player, Team
from pilka.utils.scrape.dynamic import fetch_selenium_json

URL_TEMPLATE = "https://www.fotmob.com/api/data/matchDetails?matchId={match_id}"


def _process_team(data: list) -> Team:
    starters, subs = [], []
    # for player_data in data:
    #     player = Player(
    #         name = player_data["player"]["name"],
    #         country = Country(**player_data["player"]["country"]),
    #         jersey_number = player_data["player"]["jerseyNumber"],
    #         position = player_data["player"]["position"],
    #         height = player_data["player"]["height"],
    #         date_of_birth = datetime.fromtimestamp(
    #             player_data["player"]["dateOfBirthTimestamp"]).date(),
    #         rating = player_data.setdefault("statistics", {}).get("rating"),
    #         minutes_played = player_data.setdefault("statistics", {}).get("minutesPlayed"),
    #         goals = player_data.setdefault("statistics", {}).get("goals"),
    #         assists = player_data.setdefault("statistics", {}).get("goalsAssist"),
    #         fouls = player_data.setdefault("statistics", {}).get("fouls"),
    #     )
    #     if player_data["substitute"]:
    #         subs.append(player)
    #     else:
    #         starters.append(player)
    # return Team(team_name, tuple(starters), tuple(subs))


def fetch_match(match_id: int) -> Match:
    data = fetch_selenium_json(URL_TEMPLATE.format(match_id=match_id))
    home_team = _process_team(data["home"]["players"])
    away_team = _process_team(data["away"]["players"])
    return Match(home_team, away_team)


# TODO: event IDs harvesting
