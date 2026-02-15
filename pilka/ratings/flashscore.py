"""

    pilka.ratings.flashscore
    ~~~~~~~~~~~~~~~~~~~~~~~~
    Scrape Ekstraklasa players' ratings data from Flashscore.

    @author: z33k

"""
from pilka.ratings import Manager, Match, Player, Team
from pilka.utils import ParsingError
from pilka.utils.scrape.dynamic import fetch_selenium_json

URL_TEMPLATE = "https://2.ds.lsapp.eu/pq_graphql?_hash=dlie2&eventId={event_id}&projectId=2"
TEAM_IDS = {
    'veCn0USa': "Arka Gdynia",
    'QDZZYiU0': "Bruk-Bet Termalica Nieciecza",
    'nLFfb8cC': "Cracovia Kraków",
    '8UhCGkDt': "GKS Katowice",
    'YwwrXVbD': "Górnik Zabrze",
    'hII2dnSO': "Jagiellonia Białystok",
    'zaN87CzQ': "Korona Kielce",
    'OKaSC7C5': "Lech Poznań",
    'tbVbkDEs': "Lechia Gdańsk",
    'GfdKETsg': "Legia Warszawa",
    'pSqTfZjK': "Motor Lublin",
    'dYczBosI': "Piast Gliwice",
    '6sEv2jbm': "Pogoń Szczecin",
    'U1zNq9rJ': "Radomiak Radom",
    'dlxFoVD6': "Raków Częstochowa",
    'h0ySzEal': "Widzew Łódź",
    'n19fgMFF': "Wisła Płock",
    'lOY6miqf': "Zagłębie Lubin",
}


def _process_grouping(lineup_data: dict) -> tuple[set[str], set[str]]:
    starter_ids, subs_ids = set(), set()
    for group_data in lineup_data["groups"]:
        if "start" in group_data["name"].lower():
            starter_ids.update(group_data["playerIds"])
        elif "substitute" in group_data["name"].lower():
            subs_ids.update(group_data["playerIds"])
        else:
            raise ParsingError(f"invalid team data grouping: {group_data['name']!r}")
    return starter_ids, subs_ids


def _process_manager(lineup_data: dict) -> Manager | None:
    manager = None
    match lineup_data:
        case {"coaches": {"players": [{'listName': manager_name}, *_]}}:
            match lineup_data:
                case {"coaches": {"players": [{'teamName': manager_country}, *_]}}:
                    manager = Manager(manager_name, manager_country)
        case _:
            pass
    return manager


def _process_position(player_data: dict) -> str | None:
    match player_data:
        case {"playerRoles": [{"suffix": position}, *_]}:
            return position.strip("()")
        case _:
            return None


def _process_rating(player_data: dict) -> float | None:
    match player_data:
        case {"rating": {"value": rating}}:
            return float(rating)
        case _:
            return None


def _process_team(data: dict) -> Team:
    starters, subs = [], []
    team_name = TEAM_IDS[data["id"]]
    lineup_data = data["lineup"]
    starting_ids, subs_ids = _process_grouping(lineup_data)
    manager = _process_manager(lineup_data)

    for player_data in lineup_data["players"]:
        name = player_data["listName"]
        player = Player(
            name = name,
            country = player_data["teamName"],
            jersey_number = int(player_data["number"]),
            position = _process_position(player_data),
            rating = _process_rating(player_data),
        )
        if player_data["id"] in starting_ids:
            starters.append(player)
        elif player_data["id"] in subs_ids:
            subs.append(player)
        else:
            raise ParsingError(f"player {name!r} ID not in starters or substitutions")

    return Team(team_name, tuple(starters), tuple(subs), manager)


def fetch_match(event_id: str) -> Match:
    """Fetch match data from Flashscore.

    Comparing to Sofascore, only this player data is available:
        * name (in a worse shape, e.g. "Straczek R." instead of a full name)
        * country (in a worse shape, e.g. "Poland" instead of full country codes info)
        * jersey_number
        * position (not in each case)
        * rating (with no other stats, including "minutes_played")

    This renders Team's properties "rating" and "goals_scored" not calculable (and Match's
    "score" too).
    """
    data = fetch_selenium_json(URL_TEMPLATE.format(event_id=event_id))
    is_home = None
    for event_data in data["data"]["findEventById"]["eventParticipants"]:
        match event_data["type"]:
            case {"side": side} if side in {"HOME", "AWAY"}:
                is_home = side == "HOME"
            case _:
                raise ParsingError(f"invalid event type data: {event_data['type']!r}")
        team = _process_team(event_data)
        if is_home:
            home_team = team
        else:
            away_team = team

    return Match(home_team, away_team)


# TODO: event IDs harvesting
