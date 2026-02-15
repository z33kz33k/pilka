"""

    pilka.ratings.sofascore
    ~~~~~~~~~~~~~~~~~~~~~~~
    Scrape Ekstraklasa players' ratings data from Sofascore.

    @author: z33k

"""
from datetime import datetime

from pilka.ratings import Country, Match, Player, Team
from pilka.utils.scrape.dynamic import fetch_selenium_json

URL_TEMPLATE = "https://www.sofascore.com/api/v1/event/{event_id}/lineups"
TEAM_IDS = {
    3106: "Legia Warszawa",
    3110: "Górnik Zabrze",
    3112: "GKS Katowice",
    3113: "Zagłębie Lubin",
    3115: "Widzew Łódź",
    3117: "Pogoń Szczecin",
    3121: "Lech Poznań",
    3122: "Wisła Płock",
    4901: "Cracovia Kraków",
    5064: "Arka Gdynia",
    5065: "Korona Kielce",
    7295: "Radomiak Radom",
    7691: "Jagiellonia Białystok",
    7913: "Lechia Gdańsk",
    7915: "Motor Lublin",
    7918: "Piast Gliwice",
    35268: "Raków Częstochowa",
    36851: "Bruk-Bet Termalica Nieciecza",
}


def _process_team(data: list) -> Team:
    starters, subs = [], []
    team_name = TEAM_IDS[data[0]["teamId"]]
    for player_data in data:
        player = Player(
            name = player_data["player"]["name"],
            country = Country(**player_data["player"]["country"]),
            jersey_number = player_data["player"]["jerseyNumber"],
            position = player_data["player"]["position"],
            height = player_data["player"]["height"],
            date_of_birth = datetime.fromtimestamp(
                player_data["player"]["dateOfBirthTimestamp"]).date(),
            rating = player_data.setdefault("statistics", {}).get("rating"),
            minutes_played = player_data.setdefault("statistics", {}).get("minutesPlayed"),
            goals = player_data.setdefault("statistics", {}).get("goals"),
            assists = player_data.setdefault("statistics", {}).get("goalsAssist"),
            fouls = player_data.setdefault("statistics", {}).get("fouls"),
        )
        if player_data["substitute"]:
            subs.append(player)
        else:
            starters.append(player)
    return Team(team_name, tuple(starters), tuple(subs))


def fetch_match(event_id: int) -> Match:
    data = fetch_selenium_json(URL_TEMPLATE.format(event_id=event_id))
    home_team = _process_team(data["home"]["players"])
    away_team = _process_team(data["away"]["players"])
    return Match(home_team, away_team)


# TODO: event IDs harvesting
