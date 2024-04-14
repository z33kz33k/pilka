"""

    stadiums.__init__.py
    ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    Scrape data Polish stadiums.

    @author: z33k

"""
from dataclasses import dataclass
from datetime import datetime

from stadiums.utils import init_log
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
            name, county, voivod, area, pop, *_ = [tag.text for tag in tr_tag.find_all("td")]
            towns.append(Town(name, county, voivod, int(area), int(pop)))
        except ValueError:
            pass
    return towns


TOWNS = _scrape_towns()


@dataclass(frozen=True)
class Stadium:
    name: str
    town: str
    country: str
    clubs: tuple[str]
    capacity: int
    inauguration: datetime
    cost: int | None
    illumination: int | None  # in lux


