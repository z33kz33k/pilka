"""

    pilka.stadiums
    ~~~~~~~~~~~~~~
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
from operator import attrgetter, itemgetter
from pathlib import Path
from typing import Any, Callable, Iterable, Iterator, TypeVar

from bs4 import BeautifulSoup, Tag

from pilka.constants import FILENAME_TIMESTAMP_FORMAT, OUTPUT_DIR, \
    PathLike, READABLE_TIMESTAMP_FORMAT
from pilka.constants import T
from pilka.stadiums.data import BasicStadium, Cost, Country, CountryStadiumsData, Duration, League, \
    Nickname, POLAND, Stadium, SubCapacity, Town
from pilka.utils import ParsingError, clean_parenthesized, extract_date, extract_float, extract_int, \
    from_iterable, getdir, timed
from pilka.utils.scrape import ScrapingError, fetch_soup, http_requests_counted, throttled

_log = logging.getLogger(__name__)


def normalize(text: str) -> str:
    return text.replace("–", "-").replace("−", "-").replace("’", "'")


def scrape_polish_towns() -> list[Town]:
    url = "https://pl.wikipedia.org/wiki/Dane_statystyczne_o_miastach_w_Polsce"
    soup = fetch_soup(url)
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
    soup = fetch_soup(url.format(country.id))
    leagues = [normalize(h2.text.strip()) for h2 in soup.find_all("h2")]
    has_national = leagues[0] in ("National Stadium", "Stadion Narodowy")
    for idx, table in enumerate(soup.find_all("table")):
        for row in table.find_all("tr")[1:]:
            name_tag, town_tag, clubs_tag, cap_tag = row.find_all("td")
            name, url = normalize(name_tag.text.strip()), name_tag.find("a").attrs["href"]
            town = normalize(town_tag.text.strip())
            if is_pl:
                found = towns.get(town)
                town = found or town
            clubs = [normalize(club.strip()) for club in clubs_tag.text.split(", ")
                     if club.strip() != "-"]
            league = League(leagues[idx], idx if has_national else idx + 1)
            league = League(league.name) if league.name in ("Other", "Inne") else league
            cap = extract_int(cap_tag.text)
            basic_stadiums.append(
                BasicStadium(name, url, country.name, town, tuple(clubs), league, cap))

    return basic_stadiums


def throttling_delay() -> float:
    return round(random.uniform(0.8, 1.5), 3)


AGGREGATED_FIELDS: defaultdict[str, list[str]] = defaultdict(list)
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
        "track_length": {},
    }
    DURATION_SEPARATORS = "-", "/"  # those are different glyphs

    def __init__(self, basic_data: BasicStadium) -> None:
        self._basic_data = basic_data
        self._soup: BeautifulSoup | None = None
        self._text: str | None = None

    @staticmethod
    def _trim_multiples(text: str) -> str:
        text, _, _ = text.partition(", ")
        return text

    @staticmethod
    def _split_parenthesized(text: str) -> tuple[str, str]:
        first, _, second = text.partition("(")
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
                text, _, _ = text.partition("(")
        try:
            text = text_func(text) if text_func else text
            details = details_func(details) if details_func and details else details
            return text, details
        except ParsingError:
            return None

    @classmethod
    def _parse_duration(cls, text: str) -> date | Duration | None:
        sep = from_iterable(cls.DURATION_SEPARATORS, lambda s: s in text)
        if not sep or (sep == "/" and len(text) in (7, 10)):
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
        text = self._text.removesuffix(".")
        cleaned_text = clean_parenthesized(text)
        renovations = []
        for token in cleaned_text.split(","):
            token = token.strip()
            duration = self._parse_duration(token)
            if duration:
                renovations.append(duration)
        return tuple(renovations) or None

    def _parse_cost(self) -> Cost | None:
        return _CostSubParser(self._text).parse()

    def _parse_other_names(self) -> tuple[str | Nickname, ...] | None:
        other_names = []
        for token in self._text.split(","):
            token = token.strip()
            result = self._parse_text_with_details(token, details_func=self._parse_duration)
            if result:
                name, duration = result
                if duration:
                    other_names.append(Nickname(*result))
                else:
                    other_names.append(name)
        return tuple(other_names) or None

    def _parse_illumination(self) -> int | None:
        if self._text == "none":
            return 0
        try:
            return extract_int(self._text)
        except ParsingError:
            return None

    def _parse_record_attendance(self) -> tuple[int, str | None] | None:
        record_attendance = self._parse_text_with_details(self._text, text_func=extract_int)
        if not record_attendance:
            return None
        record_attendance, record_attendance_details = record_attendance
        record_attendance_details = record_attendance_details.removesuffix(
            ".") if record_attendance_details else None
        return record_attendance, record_attendance_details

    def _parse_inauguration(self) -> tuple[date, str | None] | None:
        text = self._text.strip()
        if ")" in text:  # trim multiples #1
            text, _, _ = text.partition(")")
            text += ")"

            # trim multiples #2
            if sep := from_iterable((", ", " / "), lambda s: s in text):
                if text.index(sep) < text.index("("):
                    text, _, _ = text.partition(sep)
                    text = text.strip()
                    try:
                        return extract_date(text), None
                    except ParsingError:
                        return None

            # usual case of "text1 (text2)"
            date_is_first = text[0].isdigit()
            if date_is_first:
                inauguration = self._parse_text_with_details(text, text_func=extract_date)
            else:
                inauguration = self._parse_text_with_details(text, details_func=extract_date)
            if not inauguration:
                return None
            if date_is_first:
                inauguration, inauguration_details = inauguration
            else:
                inauguration_details, inauguration = inauguration
            inauguration_details = inauguration_details.removesuffix(
                ".") if inauguration_details else None
            return inauguration, inauguration_details

        else:  # no parentheses
            if sep := from_iterable((", ", " / "), lambda s: s in text):  # trim multiples
                text, _, _ = text.partition(sep)
                text = text.strip()
            try:
                return extract_date(text), None
            except ValueError:
                return None

    def _parse_designer(self) -> tuple[str, date | Duration | None] | None:
        design = None
        if ", " in self._text or " / " in self._text:
            designer = clean_parenthesized(self._text)
        else:
            designer = self._parse_text_with_details(self._text, details_func=self._parse_duration)
            if not designer:
                return None
            designer, design = designer
        designer = designer.removesuffix(".")
        return designer, design

    def _parse_note(self, old_note: str | None) -> str | None:
        if old_note and self._text:
            old_note += ", " + self._text[0].lower() + self._text[1:]
        else:
            old_note = self._text
        return old_note.removesuffix(".") if old_note else None

    @staticmethod
    def _parse_sub_capacity_amount(text: str) -> int:
        try:
            if "+" in text:
                return sum(extract_int(token.strip()) for token in text.split("+") if token.strip())
            return extract_int(text)
        except ValueError:
            raise ParsingError

    def _parse_sub_capacity(self, row: Tag) -> SubCapacity | None:
        span = row.find("span")
        designation = span.text.strip() if span is not None else None
        designation = designation[1:-1] if designation else designation
        if self._text.count("(") == 2:
            first, second, _ = self._text.split("(")
            text = first.strip() + f" ({second.strip()}"
        else:
            text = self._text
        sub_capacity = self._parse_text_with_details(
            text, text_func=self._parse_sub_capacity_amount)
        if sub_capacity:
            sub_capacity, note = sub_capacity
            note = note if note not in designation else None
            return SubCapacity(sub_capacity, designation, note)
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
        return normalize("\n".join(lines)) if lines else None

    # FIXME: handle "404 Not Found" cases with backoff (on rare occasions a link that leads to a
    #  working page results in this hiccup) ==> this means using the builtin capabilities of
    #  `requests` of raising a proper HTTPError in `getsoup()` and catching it here and than
    #  handling with backoff
    # FIXME: also, sometimes there's even no "404 Not Found" error but the main tag is missing
    #  but the page is OK when checked in the browser
    @throttled(throttling_delay)
    def scrape(self) -> Stadium:
        self._soup = fetch_soup(self._basic_data.url)
        table = self._soup.find("table", class_="stadium-info")
        if table is None:
            raise ScrapingError(
                f"Page at {self._basic_data.url} contains no 'table' tag of class 'stadium-info'")

        # fields initialization
        # main
        sub_capacities = []
        address, other_names, illumination, cost = None, None, None, None
        record_attendance, record_attendance_details = None, None
        # temporal
        design, construction = None, None
        inauguration, inauguration_details, renovations = None, None, None
        # personal/corporate
        designer, structural_engineer, contractor, investor = None, None, None, None
        # other
        note, track_length = None, None

        for row in table.find_all("tr"):
            header = row.find("th").text.strip()
            self._text = normalize(row.find("td").text.strip())
            if header in self.ROWS["address"]:
                address = self._text.removesuffix(".")
            elif header in self.ROWS["other_names"]:
                other_names = self._parse_other_names()
                if not other_names:
                    _log.warning(f"Unable to parse other names from: {self._basic_data.url!r}")
            elif header in self.ROWS["illumination"]:
                illumination = self._parse_illumination()
                if illumination is None:
                    _log.warning(f"Unable to parse illumination from: {self._basic_data.url!r}")
            elif header in self.ROWS["record_attendance"]:
                record_attendance = self._parse_record_attendance()
                if not record_attendance:
                    _log.warning(
                        f"Unable to parse record attendance from: {self._basic_data.url!r}")
                else:
                    record_attendance, record_attendance_details = record_attendance
            elif header in self.ROWS["cost"]:
                if not cost:  # ignore duplicated fields
                    cost = self._parse_cost()
                    if not cost:
                        _log.warning(f"Unable to parse cost from: {self._basic_data.url!r}")
            elif header in self.ROWS["design"]:
                design = self._parse_duration(self._trim_multiples(self._text))
                if not design:
                    _log.warning(f"Unable to parse design from: {self._basic_data.url!r}")
            elif header in self.ROWS["construction"]:
                if not construction:  # ignore duplicated fields
                    try:
                        construction = self._parse_duration(self._trim_multiples(self._text))
                    except ValueError:
                        construction = None
                    if not construction:
                        _log.warning(f"Unable to parse construction from: {self._basic_data.url!r}")
            elif header in self.ROWS["inauguration"]:
                if not inauguration:  # ignore duplicated fields
                    inauguration = self._parse_inauguration()
                    if not inauguration:
                        _log.warning(f"Unable to parse inauguration from: {self._basic_data.url!r}")
                    else:
                        inauguration, inauguration_details = inauguration
            elif header in self.ROWS["renovations"]:
                renovations = self._parse_renovations()
                if not renovations:
                    _log.warning(f"Unable to parse renovations from: {self._basic_data.url!r}")
            elif header in self.ROWS["designer"]:
                designer = self._parse_designer()
                if not designer:
                    _log.warning(f"Unable to parse designer from: {self._basic_data.url!r}")
                else:
                    designer, new_design = designer
                    design = new_design if new_design and not design else design
            elif header in self.ROWS["structural_engineer"]:
                structural_engineer = self._text.removesuffix(".")
            elif header in self.ROWS["contractor"]:
                contractor = clean_parenthesized(self._text).removesuffix(".")
            elif header in self.ROWS["investor"]:
                investor = self._text.removesuffix(".")
            elif header in self.ROWS["note"]:
                note = self._parse_note(note)
            elif header in self.ROWS["track_length"]:
                track_length = extract_int(self._text)
            elif not header:
                sub_capacity = self._parse_sub_capacity(row)
                if sub_capacity:
                    sub_capacities.append(sub_capacity)
                elif self._text:
                    _log.warning(
                        f"Unable to parse sub-capacity from text: {self._text!r} in"
                        f" {self._basic_data.url!r}")
            else:
                AGGREGATED_FIELDS[header].append(self._basic_data.url)

        return Stadium(
            **asdict(self._basic_data),
            capacity_details=tuple(sub_capacities) or None,
            address=address,
            other_names=other_names,
            floodlights_lux=illumination,
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
            track_length_metres=track_length,
            description=self._parse_description()
        )


class DetailsScraperPl(DetailsScraper):
    ROWS = {
        "address": {"Adres"},
        "other_names": {"Inne nazwy", "Nazwy potoczne"},
        "illumination": {"Moc oświetlenia", "Oświetlenie"},
        "record_attendance": {"Rekord frekwencji"},
        "cost": {"Koszt"},
        "design": {"Data projektu"},
        "construction": {"Budowa", "Czas budowy", "Czas budowa", "Rok budowy"},
        "inauguration": {"Inauguracja", "Inauguration", "Pierwszy mecz"},
        "renovations": {"Renowacja", "Renowacje"},
        "designer": {"Projekt"},
        "structural_engineer": {},
        "contractor": {"Wykonawca"},
        "investor": {"Właściciel"},
        "note": {"Inne", "Uwagi", "W ramach projektu"},
        "track_length": {"Długość toru"},
    }

    def __init__(self, basic_data: BasicStadium) -> None:
        if "stadiony.net" not in basic_data.url:
            raise ValueError(f"Invalid URL for a Polish scraper: {basic_data.url!r}")
        super().__init__(basic_data)


class _CostSubParser:
    MILLION_QUALIFIERS = "million", "mln", "M", "m", "milion", "Million", "millones"
    BILLION_QUALIFIERS = "billion", "bln", "B", "b", "N", "miliard", "mld"
    TRILLION_QUALIFIERS = "trillion",
    APPROXIMATORS = "approx. ", "app. ", "ok. "
    COMPOUND_SEPARATORS = " + ", ", "

    def __init__(self, text: str) -> None:
        self._text = self._prepare_text(text)
        self._tokens = [t.strip() for t in self._text.split()]

    @classmethod
    def _prepare_text(cls, text: str) -> str:
        text, _, _ = text.partition("(")
        text, _, _ = text.strip().partition(" / ")
        text = text.strip()
        approx = from_iterable(cls.APPROXIMATORS, lambda a: text.startswith(a))
        if approx:
            text = text.removeprefix(approx)
        return text

    @classmethod
    def _identify_qualifier(cls, *tokens: str, strict=False) -> tuple[int, str]:
        qualifiers = (*cls.MILLION_QUALIFIERS, *cls.BILLION_QUALIFIERS, *cls.TRILLION_QUALIFIERS)
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
        if qualifier in cls.BILLION_QUALIFIERS:
            return int(base_amount * 1_000_000_000)
        return int(base_amount * 1_000_000_000_000)

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
        currency = currency or None
        if qualifier:
            return Cost(self._get_qualified_amount(amount_str, qualifier), currency)
        try:
            return Cost(extract_int(amount_str), currency)
        except ValueError:
            return None

    def _handle_two_tokens_no_qualifier(self) -> Cost | None:
        first, second = self._tokens
        if all(not ch.isdigit() for ch in first):
            currency, amount_str = first, second
        elif all(not ch.isdigit() for ch in second):
            amount_str, currency = first, second
        else:
            return None
        try:
            return Cost(extract_int(amount_str), currency or None)
        except ValueError:
            return None

    def _handle_two_tokens_one_merged(self, qualifier_idx: int, qualifier: str) -> Cost | None:
        merged = self._tokens[0] if qualifier_idx == 1 else self._tokens[1]
        result = self._split_merged(merged)
        if not result:
            return None
        currency, amount_str = result
        return Cost(self._get_qualified_amount(amount_str, qualifier), currency or None)

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
            return Cost(self._get_qualified_amount(amount_str, found), currency or None)
        except ValueError:
            return None

    def _handle_space_delimited_amount(self) -> Cost | None:
        *amount_tokens, currency = self._tokens
        amount_str = "".join(amount_tokens)
        try:
            amount = extract_float(amount_str)
            return Cost(int(amount), currency or None)
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
        return Cost(self._get_qualified_amount(amount_str, found), currency or None)

    def parse(self) -> Cost | None:
        if any(sep in self._text for sep in self.COMPOUND_SEPARATORS):
            return self._handle_compound_cost()
        if len(self._tokens) == 1:
            return self._handle_single_token()
        elif len(self._tokens) == 2:
            return self._handle_two_tokens()
        elif len(self._tokens) == 3:
            return self._handle_three_tokens()
        elif len(self._tokens) > 3 and all(not ch.isdigit() for ch in self._tokens[-1]):
            return self._handle_space_delimited_amount()

        _log.warning(f"Unexpected cost string: {self._text!r}")
        return None


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
    soup = fetch_soup(url)
    confederations = [h2.text.strip() for h2 in soup.find_all("h2")]
    uls = soup.find_all("ul", class_="country-list")
    for idx, ul in enumerate(uls):
        for li in ul.find_all("li"):
            a: Tag = li.find("a")
            if a is not None:
                suburl = a.attrs["href"]
                _, _, country_id = suburl.rpartition("/")
                name, _, _ = a.text.partition("(")
                yield Country(normalize(name.strip()), country_id, confederations[idx])


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


def dump_aggregated_fields() -> None:
    if AGGREGATED_FIELDS:
        timestamp = datetime.now().strftime(FILENAME_TIMESTAMP_FORMAT)
        dest = OUTPUT_DIR / f"aggregated_fields_{timestamp}.json"
        with dest.open("w", encoding="utf8") as f:
            json.dump(AGGREGATED_FIELDS, f, indent=4, ensure_ascii=False)
        if dest.exists():
            _log.info(f"Successfully dumped '{dest}'")


def _parse_countries(*c_specs: str, excluded: Iterable[str] = ()) -> list[Country]:
    scraped_countries = scrape_countries()
    if not c_specs and not excluded:
        return sorted(scraped_countries, key=attrgetter("id"))

    # maps
    countries_by_id, countries_by_name = {}, {}
    countries_by_conf = defaultdict(list)
    for country in scraped_countries:
        countries_by_id[country.id] = country
        countries_by_name[country.name] = country
        countries_by_conf[country.confederation].append(country)

    def _get_countries(*specifiers: str) -> set[Country]:
        result = set()
        for spec in specifiers:
            for c_map in (countries_by_id, countries_by_name, countries_by_conf):
                country = c_map.get(spec)
                if country:
                    if isinstance(country, list):
                        result.update(country)
                    else:
                        result.add(country)
                    break
            if not country:
                _log.warning(f"No valid country found for: {spec!r}")

        if not result:
            raise ValueError(f"No valid country found for: {specifiers}")

        return result

    countries = _get_countries(*c_specs)

    if not excluded:
        return sorted(countries, key=attrgetter("id"))

    excluded_countries = _get_countries(*excluded)

    return sorted([c for c in countries if c not in excluded_countries], key=attrgetter("id"))


@http_requests_counted("dump")
@timed("dump", precision=0)
def dump_stadiums(*countries: str, **kwargs: Any) -> None:
    """Scrape stadiums data and dump it to a JSON file.

    Recognized optional arguments:
        excluded: iterable of country specifiers to be excluded from dump
        use_timestamp: whether to append a timestamp to the dumpfile's name (default: True)
        prefix: a prefix for a dumpfile's name
        filename: a complete filename for the dumpfile (renders moot other filename-concerned arguments)
        output_dir: an output directory (if not provided, defaults to OUTPUT_DIR)

    Args:
        countries: variable number of country specifiers (name, ID or confederation)
        kwargs: optional arguments
    """
    now = datetime.now()
    excluded = kwargs.get("excluded")
    excluded = set(excluded) if excluded else set()
    countries = _parse_countries(*countries, excluded=excluded)
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


def load_stadiums(file: PathLike) -> list[CountryStadiumsData]:
    """Load stadiums data at file path.

    Path ought to point to the JSON file dumped by `dump_stadiums()`.

    Args:
        file: path to the JSON dump file

    Returns:
        list of CountryStadiumData objects
    """
    file = Path(file)
    with file.open() as f:
        raw_data = json.load(f)
    return [CountryStadiumsData.from_json(c) for c in raw_data["countries"]]


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
