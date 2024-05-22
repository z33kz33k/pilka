"""

    stadiums.__init__.py
    ~~~~~~~~~~~~~~~~~~~~
    Scrape stadiums data from stadiony.net page.

    @author: z33k

"""
import json
import logging
import random
import re
from collections import defaultdict
from dataclasses import dataclass, asdict
from datetime import datetime
from operator import itemgetter
from typing import Any, Iterable, Iterator

from bs4 import Tag

from stadiums.constants import FILENAME_TIMESTAMP_FORMAT, Json, OUTPUT_DIR, \
    READABLE_TIMESTAMP_FORMAT
from stadiums.utils import extract_int, from_iterable, getdir, init_log
from stadiums.utils.scrape import getsoup, throttled

init_log()
URL = "http://stadiumdb.com/stadiums/{}"
URL_PL = "http://stadiony.net/stadiony/{}"
_log = logging.getLogger(__name__)


@dataclass(frozen=True)
class Town:
    name: str
    county: str
    province: str
    population: int
    area_ha: int | None = None


def scrape_polish_towns() -> list[Town]:
    url = "https://pl.wikipedia.org/wiki/Dane_statystyczne_o_miastach_w_Polsce"
    soup = getsoup(url)
    table = soup.find("table", class_="wikitable")
    towns = []
    for tr_tag in table.select("tbody tr"):
        try:
            name, county, voivod, area, pop, *_ = [
                tag.text.strip() for tag in tr_tag.find_all("td")]
            towns.append(Town(name, county.replace("[a]", ""), voivod, int(pop), int(area)))
        except ValueError:
            pass
    towns.extend([
        Town(
            name="Nieciecza", county="tarnowski", province="małopolskie", population=682,
            area_ha=490),
        Town(name="Stężyca", county="kartuski", province="pomorskie", population=2165),
    ])
    return towns


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
    romans = ["I", "II", "III", "IV", "V", "VI", "VII", "VIII", "IX", "X", "XI"]
    for i in range(11):
        if capacity >= tiers2steps[i]:
            return romans[i]
    return "XII"


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


# TODO: include Town data
def scrape_basic_data(country_suffix="pol", tables_count=0) -> Iterator[_BasicStadium]:
    url = URL_PL if country_suffix == "pol" else URL
    soup = getsoup(url.format(country_suffix))
    if tables_count:
        tables = soup.find_all("table", limit=tables_count)
    else:
        tables = soup.find_all("table")
    for table in tables:
        for row in table.find_all("tr")[1:]:
            name_tag, town_tag, clubs_tag, cap_tag = row.find_all("td")
            name, url = name_tag.text.strip(), name_tag.find("a").attrs["href"]
            town = town_tag.text.strip()
            clubs = [club.strip() for club in clubs_tag.text.split(", ") if club.strip() != "-"]
            # trim redundant town info in clubs
            clubs = [club.replace(f" {town}", "") for club in clubs]
            cap = extract_int(cap_tag.text)
            yield _BasicStadium(name, url, town, tuple(clubs), cap)


_KORONA_INAUGURATION = datetime(2006, 4, 1, 0, 0)


@dataclass(frozen=True)
class Cost:
    amount: int
    currency: str


@dataclass(frozen=True)
class Stadium(_BasicStadium):
    country: str
    address: str | None
    inauguration: datetime | None
    renovation: datetime | None
    cost: Cost | None
    illumination_lux: int | None
    description: str | None

    @property
    def is_modern(self) -> bool:
        dates = [d for d in (self.inauguration, self.renovation) if d is not None]
        if not dates:
            return False
        date = max(dates)
        return date >= _KORONA_INAUGURATION

    @property
    def json(self) -> Json:
        data = asdict(self)
        data["inauguration"] = self.inauguration.strftime(
            READABLE_TIMESTAMP_FORMAT) if self.inauguration else None
        data["renovation"] = self.renovation.strftime(
            READABLE_TIMESTAMP_FORMAT) if self.renovation else None
        return {
            **data,
            "is_modern": self.is_modern,
            "tier": self.tier
        }

    @classmethod
    def from_json(cls, data: Json) -> "Stadium":
        if data["inauguration"]:
            data["inauguration"] = datetime.strptime(data["inauguration"], READABLE_TIMESTAMP_FORMAT)
        if data["renovation"]:
            data["renovation"] = datetime.strptime(data["renovation"], READABLE_TIMESTAMP_FORMAT)
        return cls(
            **{k: v for k, v in data.items() if k not in ("is_modern", "tier")}
        )


def throttling_delay() -> float:
    return round(random.uniform(0.4, 0.6), 3)


class _DetailsScraper:
    DOT_DATE_REGEX = re.compile(r"\b(?:\d{2}\.\d{2}\.\d{4}|\d{2}\.\d{4}|\d{4})(?:\s*\w+)*\b")
    SLASH_DATE_REGEX = re.compile(r"\b(?:\d{2}\/\d{2}\/\d{4}|\d{2}\/\d{4}|\d{4})(?:\s*\w+)*\b")
    DMY_DT_FMT = "%d.%m.%Y"
    MY_DT_FMT = "%m.%Y"
    ROWS = {
        "country": "Country",
        "address": "Address",
        "inauguration": "Inauguration",
        "renovation": "Renovations",
        "cost": "Cost",
        "illumination": "Floodlights",
    }

    def __init__(self, basic_data: _BasicStadium) -> None:
        self._basic_data = basic_data
        self._soup = getsoup(self._basic_data.url)

    @classmethod
    def _parse_inauguration(cls, row: Tag) -> datetime | None:
        text = row.find("td").text.strip()
        if "/" in text:
            date = cls.SLASH_DATE_REGEX.search(text)
        else:
            date = cls.DOT_DATE_REGEX.search(text)
        if date:
            date = date.group()
            if len(date) == 10:
                return datetime.strptime(date, cls.DMY_DT_FMT)
            elif len(date) == 7:
                return datetime.strptime(date, cls.MY_DT_FMT)
            else:
                return datetime(int(date), 1, 1)
        return None

    @staticmethod
    def _parse_renovation(row: Tag) -> datetime | None:
        years = []
        for token in row.find("td").text.strip().split(", "):
            if "-" in token:
                years.extend(token.split("-"))
            elif "–" in token:
                years.extend(token.split("–"))
            else:
                years.append(token)
        if years:
            renovation = max(int(year) for year in years)
            return datetime(renovation, 1, 1)
        return None

    def _parse_cost(self, row: Tag) -> Cost | None:
        text = row.find("td").text.strip()
        if "(" in text:
            text, *_ = text.split("(")
            text = text.strip()
        if text.startswith("ok. "):
            text = text[4:]
        if text.count(" ") < 2:
            return None

        currency, amount, qualifier, *_ = text.split()
        if qualifier not in ("million", "billion"):
            _log.warning(
                f"Unexpected cost qualifier: {qualifier!r} for {self._basic_data.url!r}")
        else:
            amount = extract_int(text)
            amount *= 1_000_000_000 if qualifier == "billion" else 1_000_000
            return Cost(amount, currency)

        return None

    def _parse_description(self) -> str | None:
        article = self._soup.find("article", class_="stadium-description")
        if article is None:
            return None
        lines = []
        h2 = article.find("h2")
        if h2 is not None:
            lines.append(h2.text)
        lines += [p.text for p in article.find_all("p")]
        return "\n".join(lines) if lines else None

    @throttled(throttling_delay)
    def scrape(self) -> Stadium:
        table = self._soup.find("table", class_="stadium-info")
        country, address = None, None
        inauguration, renovation, cost, illumination = None, None, None, None
        for row in table.find_all("tr"):
            header = row.find("th").text
            if header == self.ROWS["country"]:
                country = row.find("td").find("a").text.strip().strip('"')
            elif header == self.ROWS["address"]:
                address = row.find("td").text.strip()
            elif header == self.ROWS["inauguration"]:
                inauguration = self._parse_inauguration(row)
            elif header == self.ROWS["renovation"]:
                renovation = self._parse_renovation(row)
            elif header == self.ROWS["cost"]:
                cost = self._parse_cost(row)
            elif header == self.ROWS["illumination"]:
                illumination = extract_int(row.find("td").text.strip())

        return Stadium(
            **asdict(self._basic_data),
            country=country,
            address=address,
            inauguration=inauguration,
            renovation=renovation,
            cost=cost,
            illumination_lux=illumination,
            description=self._parse_description()
        )


class _DetailsScraperPl(_DetailsScraper):
    ROWS = {
        "country": "Kraj",
        "address": "Adres",
        "inauguration": "Inauguracja",
        "renovation": "Renowacje",
        "cost": "Koszt",
        "illumination": "Oświetlenie",
    }

    def _parse_cost(self, row: Tag) -> Cost | None:  # override
        text = row.find("td").text.strip()
        if "(" in text:
            text, *_ = text.split("(")
            text = text.strip()
        if text.startswith("ok. "):
            text = text[4:]
        if text.count(" ") < 2:
            return None

        amount, qualifier, currency, *_ = text.split()
        if qualifier not in ("mln", "mld"):
            _log.warning(
                f"Unexpected cost qualifier: {qualifier!r} for {self._basic_data.url!r}")
        else:
            amount = extract_int(text)
            amount *= 1_000_000_000 if qualifier == "mld" else 1_000_000
            return Cost(amount, currency)

        return None


def scrape_stadiums(country="pol") -> Iterator[Stadium]:
    scraper = _DetailsScraperPl if country == "pol" else _DetailsScraper
    for stadium in scrape_basic_data(country_suffix=country):
        yield scraper(stadium).scrape()


@dataclass(frozen=True)
class StadiumsDump:
    country: str
    url: str
    stadiums: tuple[Stadium, ...]

    @property
    def json(self) -> Json:
        data = asdict(self)
        data["stadiums"] = [s.json for s in self.stadiums]
        return {**data}


def scrape_stadiums_per_country(*countries: str) -> Iterator[StadiumsDump]:
    for country in countries:
        data = [*scrape_stadiums(country)]
        url = URL.format(country)
        if not data:
            _log.warning(f"Nothing has been scraped for {url!r}")
            return
        yield StadiumsDump(data[0].country, url, tuple(data))


def dump_stadiums(*countries: str, **kwargs: Any) -> None:
    """Scrape stadiums data and dump it to a JSON file.

    Recognized optional arguments:
        use_timestamp: whether to append a timestamp to the dumpfile's name (default: True)
        prefix: a prefix for a dumpfile's name
        filename: a complete filename for the dumpfile (renders moot other filename-concerned arguments)
        output_dir: an output directory (if not provided, defaults to OUTPUT_DIR)

    Args:
        countries: variable number of country specifiers
        kwargs: optional arguments
    """
    countries = countries or ["pol"]
    now = datetime.now()
    data = {
        "timestamp": now.strftime(READABLE_TIMESTAMP_FORMAT),
        "countries": [data.json for data in scrape_stadiums_per_country(*countries)]
    }
    prefix = kwargs.get("prefix") or "stadiums"
    prefix = f"{prefix}_" if not prefix.endswith("_") else prefix
    use_timestamp = kwargs.get("use_timestamp") if kwargs.get("use_timestamp") is not None else \
        True
    timestamp = f"_{now.strftime(FILENAME_TIMESTAMP_FORMAT)}" if use_timestamp else ""
    output_dir = kwargs.get("output_dir") or kwargs.get("outputdir") or OUTPUT_DIR
    output_dir = getdir(output_dir, create_missing=False)
    filename = kwargs.get("filename")
    if filename:
        filename = filename
    else:
        filename = f"{prefix}dump{timestamp}.json"

    dest = output_dir / filename
    with dest.open("w", encoding="utf8") as f:
        json.dump(data, f, indent=4, ensure_ascii=False)

    if dest.exists():
        _log.info(f"Successfully dumped '{dest}'")


# CLUB_NAMES = sorted({club for stadium in STADIUMS for club in stadium.clubs})


def stadiums_per_town(stadiums: Iterable[Stadium], towns: Iterable[Town]) -> list:
    towns = {t.name: t for t in towns}
    modern_stadiums = [s for s in stadiums if s.is_modern]
    aggregated = defaultdict(list)
    for stadium in modern_stadiums:
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
    return sorted(result, key=itemgetter("total_capacity"), reverse=True)
