"""

    pilka.stadiums.__init__.py
    ~~~~~~~~~~~~~~~~~~~~~~~~~~
    Scrape stadiums data from stadiony.net/stadiumdb.com page.

    @author: z33k

"""
import json
import logging
import random
import re
import traceback
from collections import defaultdict
from dataclasses import asdict
from datetime import datetime
from operator import itemgetter
from typing import Any, Iterable, Iterator

from bs4 import Tag

from pilka.constants import FILENAME_TIMESTAMP_FORMAT, OUTPUT_DIR, \
    READABLE_TIMESTAMP_FORMAT
from pilka.stadiums.data import Cost, Country, CountryStadiumsData, League, Stadium, Town, \
    BasicStadium
from pilka.utils import extract_float, extract_int, getdir, timed
from pilka.utils.scrape import ScrapingError, getsoup, http_requests_counted, throttled

_log = logging.getLogger(__name__)


def scrape_polish_towns() -> list[Town]:
    url = "https://pl.wikipedia.org/wiki/Dane_statystyczne_o_miastach_w_Polsce"
    soup = getsoup(url)
    table = soup.find("table", class_="wikitable")
    if table is None:
        raise ScrapingError(f"Page at {url} contains no 'table' tag of class 'wikitable'")
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


URL = "http://stadiumdb.com/stadiums/{}"
URL_PL = "http://stadiony.net/stadiony/{}"


def scrape_basic_data(country_id="pol") -> list[BasicStadium]:
    basic_stadiums = []
    is_pl = country_id == "pol"
    url = URL_PL if is_pl else URL
    towns = {t.name: t for t in scrape_polish_towns()} if is_pl else None
    soup = getsoup(url.format(country_id))
    leagues = [h2.text.strip() for h2 in soup.find_all("h2")]
    has_national = leagues[0] in ("National Stadium", "Stadion Narodowy")
    for idx, table in enumerate(soup.find_all("table")):
        for row in table.find_all("tr")[1:]:
            name_tag, town_tag, clubs_tag, cap_tag = row.find_all("td")
            name, url = name_tag.text.strip(), name_tag.find("a").attrs["href"]
            town = town_tag.text.strip()
            if is_pl:
                found = towns.get(town)
                town = found or town
            clubs = [club.strip() for club in clubs_tag.text.split(", ") if club.strip() != "-"]
            cap = extract_int(cap_tag.text)
            league = League(leagues[idx], idx if has_national else idx + 1)
            basic_stadiums.append(BasicStadium(name, url, town, tuple(clubs), cap, league))

    return basic_stadiums


def throttling_delay() -> float:
    return round(random.uniform(0.4, 1.2), 3)


# TODO: parse more fields
class DetailsScraper:
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
    # DEBUG
    TEMP_FIELDS: dict[str, str] = {}

    def __init__(self, basic_data: BasicStadium) -> None:
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
                fmt = cls.DMY_DT_FMT.replace(".", "/") if "/" in date else cls.DMY_DT_FMT
                return datetime.strptime(date, fmt)
            elif len(date) == 7:
                fmt = cls.MY_DT_FMT.replace(".", "/") if "/" in date else cls.MY_DT_FMT
                return datetime.strptime(date, fmt)
            else:
                return datetime(int(date), 1, 1)
        return None

    @staticmethod
    def _parse_renovation(row: Tag) -> datetime | None:
        years = []
        text = row.find("td").text.strip()
        pattern = r"\(.*?\)"
        cleaned_text = re.sub(pattern, "", text)  # get rid of anything within parentheses
        for token in cleaned_text.split(","):
            token = token.strip()
            if "-" in token:
                years.extend(token.split("-"))
            elif "–" in token:
                years.extend(token.split("–"))
            else:
                years.append(token)
        if years:
            try:
                renovation = max(int(year) for year in years)
            except ValueError:  # very rare cases where a year is actually a month and a year
                return None
            return datetime(renovation, 1, 1)
        return None

    @staticmethod
    def _parse_cost(row: Tag) -> Cost | None:
        return _CostSubParser(row).parse()

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
        if table is None:
            raise ScrapingError(
                f"Page at {self._basic_data.url} contains no 'table' tag of class 'stadium-info'")
        country, address = None, None
        inauguration, renovation, cost, illumination = None, None, None, None
        for row in table.find_all("tr"):
            header = row.find("th").text.strip()
            # DEBUG
            if header:
                self.TEMP_FIELDS[header] = self._basic_data.url

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
                text = row.find("td").text.strip()
                illumination = extract_int(text) if not all(ch.isalpha() for ch in text) else None

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


class _CostSubParser:
    QUALIFIERS = "million", "mln", "billion", "bln"

    def __init__(self, row: Tag) -> None:
        self._text = self._prepare_text(row)
        self._tokens = self._text.split()

    @staticmethod
    def _prepare_text(row: Tag) -> str:
        text = row.find("td").text.strip()
        if "(" in text:
            text, *_ = text.split("(")
            text = text.strip()
        if text.startswith("ok. "):
            text = text[4:]
        return text

    @classmethod
    def _identify_qualifier(cls, *tokens: str) -> tuple[int, str]:
        for i, token in enumerate(tokens):
            for qualifier in cls.QUALIFIERS:
                if token == qualifier:
                    return i, qualifier
        return -1, ""

    @classmethod
    def _get_amount(cls, amount_text: str, qualifier: str) -> int:
        base_amount = extract_float(amount_text)
        if qualifier in cls.QUALIFIERS[:2]:
            return int(base_amount * 1_000_000)
        return int(base_amount * 1_000_000_000)

    def _handle_merged_qualifier(self) -> Cost | None:
        idx, found = self._identify_qualifier(*self._tokens)
        if idx == -1:
            return None
        if idx == 0:
            qualifier, currency = self._tokens
        else:
            currency, qualifier = self._tokens
        try:
            return Cost(self._get_amount(qualifier, found), currency)
        except ValueError:
            return None

    def parse(self) -> Cost | None:
        # text = row.find("td").text.strip()
        if len(self._tokens) == 2:
            return self._handle_merged_qualifier()
        elif len(self._tokens) == 3:
            idx, found = self._identify_qualifier(*self._tokens)
            if idx == 1:
                amount, _, currency = self._tokens
            elif idx == 2:
                currency, amount, _ = self._tokens
            else:
                return None
            return Cost(self._get_amount(amount, found), currency)

        _log.warning(f"Unexpected cost string: {self._text!r}")
        return None


class _CostSubParserPl(_CostSubParser):
    QUALIFIERS = "milion", "mln", "miliard", "mld"


class DetailsScraperPl(DetailsScraper):
    ROWS = {
        "country": "Kraj",
        "address": "Adres",
        "inauguration": "Inauguracja",
        "renovation": "Renowacje",
        "cost": "Koszt",
        "illumination": "Oświetlenie",
    }

    def __init__(self, basic_data: BasicStadium) -> None:
        if "stadiony.net" not in basic_data.url:
            raise ValueError(f"Invalid URL for a Polish scraper: {basic_data.url!r}")
        super().__init__(basic_data)

    @staticmethod
    def _parse_cost(row: Tag) -> Cost | None:  # override
        return _CostSubParserPl(row).parse()


def scrape_stadiums(country_id="pol") -> Iterator[Stadium]:
    scraper = DetailsScraperPl if country_id == "pol" else DetailsScraper
    basic_stadiums = scrape_basic_data(country_id=country_id)
    _log.info(f"Only {len(basic_stadiums)} stadium(s) to go...")
    for stadium in basic_stadiums:
        try:
            yield scraper(stadium).scrape()
        except ScrapingError as e:
            _log.error(f"Scraping of {stadium.name} failed with: {e}")


def scrape_countries() -> Iterator[Country]:
    url = "http://stadiumdb.com/stadiums"
    soup = getsoup(url)
    confederations = [h2.text.strip() for h2 in soup.find_all("h2")]
    uls = soup.find_all("ul", class_="country-list")
    for idx, ul in enumerate(uls):
        for li in ul.find_all("li"):
            a: Tag = li.find("a")
            if a is not None:
                suburl = a.attrs["href"]
                *_, country_id = suburl.split("/")
                name, *_ = a.text.split("(")
                yield Country(name.strip(), country_id, confederations[idx])


@http_requests_counted("country scraping")
@timed("country scraping", precision=2)
def scrape_country_stadiums(country: Country) -> CountryStadiumsData | None:
    _log.info(f"Scraping {country.name!r} started...")
    stadiums = [*scrape_stadiums(country.id)]
    url = URL.format(country.id)
    if not stadiums:
        _log.warning(f"Nothing has been scraped for {url!r}")
        return
    return CountryStadiumsData(country, url, tuple(stadiums))


@http_requests_counted("dump")
@timed("dump", precision=0)
def dump_stadiums(*countries: Country, **kwargs: Any) -> None:
    """Scrape stadiums data and dump it to a JSON file.

    Recognized optional arguments:
        excluded: iterable of countries to be excluded from dump
        use_timestamp: whether to append a timestamp to the dumpfile's name (default: True)
        prefix: a prefix for a dumpfile's name
        filename: a complete filename for the dumpfile (renders moot other filename-concerned arguments)
        output_dir: an output directory (if not provided, defaults to OUTPUT_DIR)

    Args:
        countries: variable number of country specifiers
        kwargs: optional arguments
    """
    now = datetime.now()
    countries = list(countries or scrape_countries())
    excluded = kwargs.get("excluded")
    excluded = set(excluded) if excluded else set()
    countries = [c for c in countries if c not in excluded]
    _log.info(f"Scraping {len(countries)} country(ies) started...")
    data = {
        "timestamp": now.strftime(READABLE_TIMESTAMP_FORMAT),
        "countries": []
    }
    for country in countries:
        try:
            country_stadiums_data = scrape_country_stadiums(country)
            if country_stadiums_data:
                data["countries"].append(country_stadiums_data.json)
                # DEBUG
                from pprint import pprint
                pprint(DetailsScraper.TEMP_FIELDS)
        except Exception as e:
            _log.error(f"{type(e).__qualname__}: {e}:\n{traceback.format_exc()}")

    try:
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
    except Exception as e:
        _log.critical(f"{type(e).__qualname__}: {e}:\n{traceback.format_exc()}")


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
