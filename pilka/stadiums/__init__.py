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
from datetime import date, datetime
from operator import itemgetter
from typing import Any, Callable, Iterable, Iterator, TypeVar

from bs4 import BeautifulSoup, Tag

from pilka.constants import FILENAME_TIMESTAMP_FORMAT, OUTPUT_DIR, \
    READABLE_TIMESTAMP_FORMAT
from pilka.stadiums.data import Cost, Country, CountryStadiumsData, Duration, League, Nickname, \
    Stadium, Town, BasicStadium, POLAND
from pilka.utils import ParsingError, extract_date, extract_float, extract_int, from_iterable, \
    getdir, timed, clean_parenthesized, trim_suffix
from pilka.utils.scrape import ScrapingError, getsoup, http_requests_counted, throttled
from pilka.constants import T

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


def scrape_basic_data(country=POLAND) -> list[BasicStadium]:
    basic_stadiums = []
    is_pl = country == POLAND
    url = URL_PL if is_pl else URL
    towns = {t.name: t for t in scrape_polish_towns()} if is_pl else None
    soup = getsoup(url.format(country.id))
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
            league = League(league.name) if league.name == "Other" else league
            basic_stadiums.append(
                BasicStadium(name, url, country.name, town, tuple(clubs), cap, league))

    return basic_stadiums


def throttling_delay() -> float:
    return round(random.uniform(0.6, 1.5), 3)


T2 = TypeVar("T2")


class DetailsScraper:
    ROWS = {
        "address": {"Address", "Addres", "Adfress"},
        "other_names": {"Nicknames", "Former name", "Other name", "Other names"},
        "illumination": {"Floodlights"},
        "record_attendance": {
            "Record attendance", "Record Attendance", "Recod attendance",
            "Record attendance (MLS)", "Record attendance (football)", "Record attendence",
            "Record attnedance", "Record audience", "Rekord frekwencji", "Rercord attendance",
            "Attendance record"
        },
        "cost": {"Cost", "cost", "Koszt", "Kost", "Renovation Cost", "Renovation cost"},
        "design": {"Design time", "Date of project", "Project date"},
        "construction": {
            "Construction", "Concstruction", "Construction time", "Costruction", "Czas budowy"
        },
        "inauguration": {
            "Inauguration", "Ianuguration", "Iauguration", "Inauguaration", "Inauguartion",
            "Inauguation", "Inauguracja", "Inauguration (club establishment)", "Inaugurtion",
            "Inuguration", "First match", "First event", "First game", "Opening game"
        },
        "renovations": {"Renovations", "Renovation", "Renovatons"},
        "designer": {
            "Design", "Deisgn", "Architect", "Designer", "Designs", "Project", "Projekt", "project"
        },
        "structural_engineer": {
            "Structural Engineer", "Structural engineer", "Engineer", "Roof structure"
        },
        "contractor": {"Contractor", "Contracor", "Constractor"},
        "investor": {"Client", "Investors", "Operator", "Owner", "Ownership", "ownership"},
        "note": {
            "Hints", "Note", "Notes", "Notice", "Notices", "Other", "Others", "Within the project",
            "Dentro del proyecto"
        },
    }
    DURATION_SEPARATORS = "-", "–", "/"

    def __init__(self, basic_data: BasicStadium) -> None:
        self._basic_data = basic_data
        self._soup: BeautifulSoup | None = None
        self._text: str | None = None

    @staticmethod
    def _split_parenthesized(text: str) -> tuple[str, str]:
        first, second = text.split("(")
        second = second[:-1] if second.endswith(")") else second
        return first.strip(), second.strip()

    @classmethod
    def _parse_text_with_details(
            cls, text: str,
            text_func: Callable[[str], T] | None = None,
            details_func: Callable[[str], T2] | None = None
    ) -> tuple[T | str, T2 | str | None] | None:
        details = None
        if "(" in text:
            try:
                text, details = cls._split_parenthesized(text)
            except ValueError:
                text, *_ = text.split("(")
        try:
            text = text_func(text) if text_func else text
            details = details_func(details) if details_func and details else details
            return text, details
        except ParsingError:
            return None

    @classmethod
    def _parse_duration(cls, text: str) -> date | Duration | None:
        sep = from_iterable(cls.DURATION_SEPARATORS, lambda s: s in text)
        if not sep:
            try:
                return extract_date(text)
            except ParsingError:
                return None
        try:
            first, second = text.split(sep)
        except ValueError:
            return None
        first, second = first.strip(), second.strip()
        if len(first) == 4 and len(second) in (2, 4):
            if len(second) == 2:
                second = first[:2] + second
            try:
                start, end = int(first), int(second)
            except ValueError:
                return None
            return Duration(date(start, 1, 1), date(end, 1, 1))
        try:
            return Duration(extract_date(first), extract_date(second))
        except ParsingError:
            return None

    def _parse_renovations(self) -> tuple[datetime | Duration, ...] | None:
        cleaned_text = clean_parenthesized(self._text)
        renovations = []
        for token in cleaned_text.split(","):
            token = token.strip()
            duration = self._parse_duration(token)
            if duration:
                renovations.append(duration)
        return tuple(renovations) or None

    def _parse_cost(self) -> Cost | None:
        return _CostSubParser(self._text).parse()

    def _parse_other_names(self) -> tuple[Nickname, ...] | None:
        other_names = []
        for token in self._text.split(","):
            token = token.strip()
            result = self._parse_text_with_details(token, details_func=self._parse_duration)
            if result:
                other_names.append(Nickname(*result))
        return tuple(other_names) or None

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
        self._soup = getsoup(self._basic_data.url)
        table = self._soup.find("table", class_="stadium-info")
        if table is None:
            raise ScrapingError(
                f"Page at {self._basic_data.url} contains no 'table' tag of class 'stadium-info'")

        # fields initialization
        # main
        address, other_names, illumination, cost = None, None, None, None
        record_attendance, record_attendance_details = None, None
        # temporal
        design, construction = None, None
        inauguration, inauguration_details, renovations = None, None, None
        # personal/corporate
        designer, structural_engineer, contractor, investor = None, None, None, None
        # other
        note = None

        for row in table.find_all("tr"):
            header = row.find("th").text.strip()
            self._text = row.find("td").text.strip()
            if header in self.ROWS["address"]:
                address = trim_suffix(self._text, ".")
            elif header in self.ROWS["other_names"]:
                other_names = self._parse_other_names()
                if not other_names:
                    _log.warning(f"Unable to parse other names from: {self._basic_data.url!r}")
            elif header in self.ROWS["illumination"]:
                try:
                    illumination = extract_int(self._text)
                except ParsingError:
                    _log.warning(f"Unable to parse illumination from: {self._basic_data.url!r}")
            elif header in self.ROWS["record_attendance"]:
                record_attendance = self._parse_text_with_details(self._text, text_func=extract_int)
                if record_attendance:
                    record_attendance, record_attendance_details = record_attendance
                    record_attendance_details = trim_suffix(
                        record_attendance_details, ".") if record_attendance_details else None
                else:
                    _log.warning(
                        f"Unable to parse record attendance from: {self._basic_data.url!r}")
            elif header in self.ROWS["cost"]:
                cost = self._parse_cost()
                if not cost:
                    _log.warning(f"Unable to parse cost from: {self._basic_data.url!r}")
            elif header in self.ROWS["design"]:
                design = self._parse_duration(self._text)
                if not design:
                    _log.warning(f"Unable to parse design from: {self._basic_data.url!r}")
            elif header in self.ROWS["construction"]:
                construction = self._parse_duration(self._text)
                if not construction:
                    _log.warning(f"Unable to parse construction from: {self._basic_data.url!r}")
            elif header in self.ROWS["inauguration"]:
                if not inauguration:
                    inauguration = self._parse_text_with_details(
                        self._text, text_func=extract_date)
                    if inauguration:
                        inauguration, inauguration_details = inauguration
                        inauguration_details = trim_suffix(
                            inauguration_details, ".") if inauguration_details else None
                    else:
                        _log.warning(f"Unable to parse inauguration from: {self._basic_data.url!r}")
            elif header in self.ROWS["renovations"]:
                renovations = self._parse_renovations()
                if not renovations:
                    _log.warning(f"Unable to parse renovations from: {self._basic_data.url!r}")
            elif header in self.ROWS["designer"]:
                text = self._text
                if ", " in text:
                    designer = clean_parenthesized(text)
                else:
                    designer = self._parse_text_with_details(text, details_func=self._parse_duration)
                    if designer:
                        designer, design = designer
                    else:
                        _log.warning(f"Unable to parse designer from: {self._basic_data.url!r}")
                designer = trim_suffix(designer, ".") if designer else None
            elif header in self.ROWS["structural_engineer"]:
                structural_engineer = trim_suffix(self._text, ".")
            elif header in self.ROWS["contractor"]:
                contractor = trim_suffix(clean_parenthesized(self._text), ".")
            elif header in self.ROWS["investor"]:
                investor = trim_suffix(self._text, ".")
            elif header in self.ROWS["note"]:
                if note and self._text:
                    note += ", " + self._text[0].lower() + self._text[1:]
                else:
                    note = self._text
                note = trim_suffix(note, ".") if note else None

        return Stadium(
            **asdict(self._basic_data),
            address=address,
            other_names=other_names,
            illumination_lux=illumination,
            record_attendance=record_attendance,
            record_attendance_details=record_attendance_details,
            cost=cost,
            design=design,
            construction=construction,
            inauguration=inauguration,
            inauguration_details=inauguration_details,
            renovations=renovations,
            designer=designer,
            structural_engineer=structural_engineer,
            contractor=contractor,
            investor=investor,
            note=note,
            description=self._parse_description()
        )


class _CostSubParser:
    MILLION_QUALIFIERS = "million", "mln", "M", "m"
    BILLION_QUALIFIERS = "billion", "bln", "B", "b"
    APPROXIMATORS = "approx. ", "app. "
    COMPOUND_SEPARATORS = " + ", ", "

    def __init__(self, text: str) -> None:
        self._text = self._prepare_text(text)
        self._tokens = [t.strip() for t in self._text.split()]

    @classmethod
    def _prepare_text(cls, text: str) -> str:
        text = clean_parenthesized(text)
        if " / " in text:
            *_, text = text.split(" / ")
            text = text.strip()
        for approx in cls.APPROXIMATORS:
            if text.startswith(approx):
                text = text[len(approx):]
                break
        return text

    @classmethod
    def _identify_qualifier(cls, *tokens: str, strict=False) -> tuple[int, str]:
        qualifiers = (*cls.MILLION_QUALIFIERS, *cls.BILLION_QUALIFIERS)
        for i, token in enumerate(tokens):
            for qualifier in qualifiers:
                if strict:
                    if token == qualifier:
                        return i, qualifier
                else:
                    if token.endswith(qualifier):
                        return i, qualifier
        return -1, ""

    @classmethod
    def _get_qualified_amount(cls, amount: str, qualifier: str) -> int:
        base_amount = extract_float(amount)
        if qualifier in cls.MILLION_QUALIFIERS:
            return int(base_amount * 1_000_000)
        return int(base_amount * 1_000_000_000)

    @staticmethod
    def _split_merged(text: str) -> tuple:
        match = re.search(r"\d", text)
        if not match:
            return ()
        return text[:match.start()], text[match.start():]

    def _handle_single_token(self) -> Cost | None:
        _, qualifier = self._identify_qualifier(self._text)
        text = self._text[:-len(qualifier)] if qualifier else self._text
        result = self._split_merged(text)
        if not result:
            return None
        currency, amount_str = result
        if qualifier:
            return Cost(self._get_qualified_amount(amount_str, qualifier), currency)
        try:
            return Cost(extract_int(amount_str), currency)
        except ValueError:
            return None

    def _handle_two_tokens_no_qualifier(self) -> Cost | None:
        first, second = self._tokens
        if all(ch.isalpha() for ch in first):
            currency, amount_str = first, second
        elif all(ch.isalpha() for ch in second):
            amount_str, currency = first, second
        else:
            return None
        try:
            return Cost(extract_int(amount_str), currency)
        except ValueError:
            return None

    def _handle_two_tokens_one_merged(self, qualifier_idx: int, qualifier: str) -> Cost | None:
        merged = self._tokens[0] if qualifier_idx == 1 else self._tokens[1]
        result = self._split_merged(merged)
        if not result:
            return None
        currency, amount_str = result
        return Cost(self._get_qualified_amount(amount_str, qualifier), currency)

    def _handle_two_tokens(self) -> Cost | None:
        idx, found = self._identify_qualifier(*self._tokens)
        if idx == -1:
            return self._handle_two_tokens_no_qualifier()

        # qualifier is a token so the other is a merged currency-amount
        if found in self._tokens:
            return self._handle_two_tokens_one_merged(idx, found)

        if idx == 0:
            amount_str, currency = self._tokens
        else:
            currency, amount_str = self._tokens
        try:
            return Cost(self._get_qualified_amount(amount_str, found), currency)
        except ValueError:
            return None

    def _handle_space_delimited_amount(self) -> Cost | None:
        *amount_tokens, currency = self._tokens
        amount_str = "".join(amount_tokens)
        try:
            amount = extract_float(amount_str)
            return Cost(int(amount), currency)
        except ValueError:
            return None

    def _handle_compound_cost(self) -> Cost | None:
        add, comma = self.COMPOUND_SEPARATORS
        sep = add if add in self._text else comma
        tokens = self._text.split(sep)

        compound = None
        for t in tokens:
            cost = _CostSubParser(t).parse()
            if cost is not None:
                if compound is None:
                    compound = cost
                else:
                    try:
                        compound += cost
                    except ValueError:
                        pass
        return compound

    def _handle_three_tokens(self):
        idx, found = self._identify_qualifier(*self._tokens, strict=True)
        if idx == 1:
            amount_str, _, currency = self._tokens
        elif idx == 2:
            currency, amount_str, _ = self._tokens
        else:
            return None
        return Cost(self._get_qualified_amount(amount_str, found), currency)

    def parse(self) -> Cost | None:
        if any(sep in self._text for sep in self.COMPOUND_SEPARATORS):
            return self._handle_compound_cost()
        if len(self._tokens) == 1:
            return self._handle_single_token()
        elif len(self._tokens) == 2:
            return self._handle_two_tokens()
        elif len(self._tokens) == 3:
            return self._handle_three_tokens()
        elif len(self._tokens) > 3 and all(ch.isalpha() for ch in self._tokens[-1]):
            return self._handle_space_delimited_amount()

        _log.warning(f"Unexpected cost string: {self._text!r}")
        return None


class _CostSubParserPl(_CostSubParser):
    MILLION_QUALIFIERS = "milion", "mln"
    BILLION_QUALIFIERS = "miliard", "mld"
    APPROXIMATORS = "ok. ",


class DetailsScraperPl(DetailsScraper):
    ROWS = {
        "country": {"Kraj"},
        "address": {"Adres"},
        "inauguration": {"Inauguracja"},
        "renovation": {"Renowacje"},
        "cost": {"Koszt"},
        "illumination": {"Oświetlenie"},
    }

    def __init__(self, basic_data: BasicStadium) -> None:
        if "stadiony.net" not in basic_data.url:
            raise ValueError(f"Invalid URL for a Polish scraper: {basic_data.url!r}")
        super().__init__(basic_data)

    @staticmethod
    def _parse_cost(text: str) -> Cost | None:  # override
        return _CostSubParserPl(text).parse()


def scrape_stadiums(country=POLAND) -> Iterator[Stadium]:
    scraper = DetailsScraperPl if country == POLAND else DetailsScraper
    basic_stadiums = scrape_basic_data(country)
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
    stadiums = [*scrape_stadiums(country)]
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
