"""

    stadiums.__init__.py
    ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    Scrape data Polish stadiums.

    @author: z33k

"""
from dataclasses import dataclass
from datetime import datetime

from stadiums.utils import extract_int, init_log
from stadiums.utils.scrape import getsoup

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
            towns.append(Town(name, county, voivod, int(area), int(pop)))
        except ValueError:
            pass
    return towns


TOWNS = _scrape_towns()


@dataclass(frozen=True)
class _BasicStadium:
    name: str
    url: str
    town: str
    clubs: tuple[str]
    capacity: int


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


@dataclass(frozen=True)
class Stadium:
    name: str
    town: str
    country: str
    clubs: tuple[str]
    capacity: int
    inauguration: datetime
    renovations: list[datetime]
    cost: int | None  # in PLN
    illumination: int | None  # in lux




