"""

    stadiums.__init__.py
    ~~~~~~~~~~~~~~~~~~~~
    Scrape data on Polish stadiums.

    @author: z33k

"""
import random
import re
from collections import defaultdict
from dataclasses import dataclass, asdict
from datetime import datetime
from operator import itemgetter

from stadiums.utils import extract_int, from_iterable, init_log
from stadiums.utils.scrape import getsoup, throttled

init_log()
URL = "http://stadiony.net/stadiony/pol"
TOWN_URL = "https://pl.wikipedia.org/wiki/Dane_statystyczne_o_miastach_w_Polsce"


@dataclass(frozen=True)
class Town:
    name: str
    county: str
    voivodeship: str
    area: int  # in ha
    population: int


def _scrape_towns() -> list[Town]:
    soup = getsoup(TOWN_URL)
    table = soup.find("table", class_="wikitable")
    towns = []
    for tr_tag in table.select("tbody tr"):
        try:
            name, county, voivod, area, pop, *_ = [
                tag.text.strip() for tag in tr_tag.find_all("td")]
            towns.append(Town(name, county.replace("[a]", ""), voivod, int(area), int(pop)))
        except ValueError:
            pass
    return towns


TOWNS = _scrape_towns()
TOWNS.extend([
    Town(name="Nieciecza", county="tarnowski", voivodeship="małopolskie", population=682, area=490),
    Town(name="Stężyca", county="kartuski", voivodeship="pomorskie", population=2165, area=0),
])


def get_tier(capacity: int) -> str:
    """Return stadium's tier based on its capacity.

    Ranges are loosely based on a following function:
        def step(n, factor=1.48):
        number = 1_000
        if n <= 0:
            return number
        for _ in range(n):
            number *= factor
        return int(round(number))

        >>> step(1)
        1480
        >>> step(2)
        2190
        >>> step(3)
        3242
        >>> step(4)
        4798
        >>> step(5)
        7101
        >>> step(6)
        10509
        >>> step(7)
        15554
        >>> step(8)
        23019
        >>> step(9)
        34069
        >>> step(10)
        50422
        >>> step(11)
        74624
    """
    tiers2steps = {
        0: 75_000,
        1: 50_000,
        2: 34_000,
        3: 23_000,
        4: 15_550,
        5: 10_500,
        6: 7_100,
        7: 4_800,
        8: 3_250,
        9: 2_200,
        10: 1_500,
    }
    for i in range(11):
        if capacity >= tiers2steps[i]:
            return "S" if i == 0 else str(i)
    return "11"


@dataclass(frozen=True)
class _BasicStadium:
    name: str
    url: str
    town: str
    clubs: tuple[str, ...]
    capacity: int

    @property
    def tier(self) -> str:
        return get_tier(self.capacity)


def _scrape_basic_data() -> list[_BasicStadium]:
    soup = getsoup(URL)
    tables = soup.find_all("table", limit=4)
    stadiums = []
    for table in tables:
        for row in table.find_all("tr")[1:]:
            name_tag, town_tag, clubs_tag, cap_tag = row.find_all("td")
            name, url = name_tag.text.strip(), name_tag.find("a").attrs["href"]
            town = town_tag.text.strip()
            clubs = [club.strip() for club in clubs_tag.text.split(", ") if club.strip() != "-"]
            # trim redundant town info in clubs
            clubs = [club.replace(f" {town}", "") for club in clubs]
            cap = extract_int(cap_tag.text)
            stadiums.append(_BasicStadium(name, url, town, tuple(clubs), cap))
    return stadiums


BASIC_STADIUMS = _scrape_basic_data()
_KORONA_INAUGURATION = datetime(2006, 4, 1, 0, 0)


@dataclass(frozen=True)
class Stadium(_BasicStadium):
    country: str
    inauguration: datetime
    renovation: datetime | None
    cost: int | None  # in PLN
    illumination: int | None  # in lux

    @property
    def is_modern(self) -> bool:
        dates = [d for d in (self.inauguration, self.renovation) if d is not None]
        if not dates:
            return False
        date = max(dates)
        return date >= _KORONA_INAUGURATION


_DATE_REGEX = re.compile(r"\b(?:\d{2}\.\d{2}\.\d{4}|\d{4})(?:\s*\w+)*\b")
_DT_FMT = "%d.%m.%Y"


def throttling_delay() -> float:
    return round(random.uniform(0.4, 0.6), 3)


@throttled(throttling_delay)
def _scrape_details(basic_data: _BasicStadium) -> Stadium:
    soup = getsoup(basic_data.url)
    table = soup.find("table", class_="stadium-info")
    country, inauguration, renovation, cost, illumination = None, None, None, None, None
    for row in table.find_all("tr"):
        match row.find("th").text:
            case "Kraj":
                country = row.find("td").find("a").text.strip().strip('"')
            case "Inauguracja":
                text = row.find("td").text.strip()
                date = _DATE_REGEX.search(text)
                if date:
                    date = date.group()
                    inauguration = datetime.strptime(date, _DT_FMT) if len(
                        date) == 10 else datetime(int(date), 1, 1)
            case "Renowacje":
                years = []
                for token in row.find("td").text.strip().split(", "):
                    if "-" in token:
                        years.extend(token.split("-"))
                    elif "–" in token:
                        years.extend(token.split("–"))
                    else:
                        years.append(token)
                renovation = max(int(year) for year in years)
                renovation = datetime(renovation, 1, 1)
            case "Koszt":
                text = row.find("td").text.strip()
                cost_int = extract_int(text)
                cost = cost_int * 1_000_000_000 if "mld" in text else cost_int * 1_000_000
            case "Oświetlenie":
                illumination = extract_int(row.find("td").text.strip())
            case _:
                pass

    return Stadium(
        **asdict(basic_data),
        country=country,
        inauguration=inauguration,
        renovation=renovation,
        cost=cost,
        illumination=illumination
    )


STADIUMS = [_scrape_details(bs) for bs in BASIC_STADIUMS]
CLUB_NAMES = sorted({club for stadium in STADIUMS for club in stadium.clubs})
MODERN_STADIUMS = [s for s in STADIUMS if s.is_modern]


def _stadiums_per_town() -> list:
    towns = {t.name: t for t in TOWNS}
    aggregated = defaultdict(list)
    for stadium in MODERN_STADIUMS:
        aggregated[stadium.town].append(stadium)

    result = []
    for town, stadiums in aggregated.items():
        pop = towns[town].population
        cap = sum(s.capacity for s in stadiums)
        result.append(
            {
                "town": town,
                "stadiums": [asdict(s) for s in stadiums],
                "population": pop,
                "total_capacity": cap,
                "cap2pop": f"{cap / pop * 100:.2f} %",
            }
        )
    return result


STADIUMS_PER_TOWN = sorted(_stadiums_per_town(), key=itemgetter("total_capacity"), reverse=True)

