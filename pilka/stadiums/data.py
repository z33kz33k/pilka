"""

    pilka.stadiums.data.py
    ~~~~~~~~~~~~~~~~~~~~~~~
    Data structures.

    @author: z33k

"""
from dataclasses import Field, asdict, dataclass, fields
from datetime import date, timedelta
from typing import Any, Type

from pilka.constants import Json, T
from pilka.utils import get_classes_in_module, get_properties, tolist, totuple


def _serialize(data: Json) -> Json:  # recursive
    if isinstance(data, tuple):
        data = tolist(data)
    if isinstance(data, list):
        for idx, item in enumerate(data):
            data[idx] = _serialize(item)
    elif isinstance(data, dict):
        data = {k: v for k, v in data.items() if v is not None}
        for k, v in data.items():
            data[k] = _serialize(v)
    elif isinstance(data, date):
        data = data.isoformat()
    return data


def _reconstruct_from_json(types: dict[str, Type[T]], field: str,  data: Json) -> T | Json:
    if not isinstance(data, dict):  # not a structure to reconstruct
        return data
    if type_ := types.get(field.capitalize()):
        if hasattr(type_, "from_json"):
            return type_.from_json(data)
    return data


def _deserialize_substructs(data: Json) -> dict:
    types = get_classes_in_module(__name__)
    for k, v in data.items():
        if isinstance(v, list) and k.endswith("s"):
            data[k] = [_reconstruct_from_json(types, k[:-1], item) for item in v]
        else:
            data[k] = _reconstruct_from_json(types, k, v)
    return data


def _deserialize_date(obj: Any) -> date | Any:
    try:
        return date.fromisoformat(obj)
    except ValueError:
        return obj


def _is_duration(data: Json) -> bool:
    if not isinstance(data, dict):
        return False
    for k, v in data.items():
        if k not in ("start", "end"):
            return False
        if not isinstance(_deserialize_date(v), date):
            return False
    return True


def _deserialize_dates_and_durations(data: Json, field: Field) -> Json:
    if date.__name__ in str(field.type):
        data[field.name] = _deserialize_date(data[field.name])
    elif list.__name__ in str(field.type) and isinstance(data[field.name], list):
        duration_cls = get_classes_in_module(__name__).get("Duration")
        new_list = []
        for item in data[field.name]:
            if duration_cls and _is_duration(item):
                new_list.append(duration_cls(**item))
            else:
                new_list.append(_deserialize_date(item))
        data[field.name] = new_list
    return data


@dataclass(frozen=True)
class _JsonSerializable:
    @property
    def json(self) -> Json:
        data = {k: v for k, v in asdict(self).items() if v is not None}
        return _serialize(data)

    @classmethod
    def from_json(cls, data: Json) -> "_JsonSerializable":
        field_names = {f.name for f in fields(cls)}
        data = {k: v for k, v in data.items() if k not in get_properties(cls) and k in field_names}
        for f in fields(cls):
            if data.get(f.name) is None:
                data[f.name] = None
            else:
                data = _deserialize_dates_and_durations(data, f)
        data = _deserialize_substructs(data)
        for f in fields(cls):
            if isinstance(data.get(f.name), list):
                data[f.name] = totuple(data[f.name])
        return cls(**data)


@dataclass(frozen=True)
class Town(_JsonSerializable):
    name: str
    county: str
    province: str
    population: int
    area_ha: int | None = None


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
class League(_JsonSerializable):
    name: str
    tier: int | None = None


@dataclass(frozen=True)
class BasicStadium(_JsonSerializable):
    name: str
    url: str
    country: str
    town: str | Town
    clubs: tuple[str, ...]
    capacity: int
    league: League

    @property
    def tier(self) -> str:
        return get_tier(self.capacity)


_KORONA_INAUGURATION = date(2006, 4, 1)


@dataclass(frozen=True)
class Cost(_JsonSerializable):
    amount: int
    currency: str

    def __add__(self, other: "Cost") -> "Cost":
        if not isinstance(other, Cost):
            return NotImplemented
        if self.currency != other.currency:
            raise ValueError("Cannot add costs with different currencies")
        return Cost(amount=self.amount + other.amount, currency=self.currency)


@dataclass(frozen=True)
class Duration(_JsonSerializable):
    start: date
    end: date

    @property
    def delta(self) -> timedelta:
        return self.end - self.start


@dataclass(frozen=True)
class Stadium(BasicStadium):
    address: str | None
    construction: date | Duration | None
    inauguration: date | None
    inauguration_details: str | None
    renovations: tuple[date | Duration, ...] | None
    cost: Cost | None
    illumination_lux: int | None
    record_attendance: int | None
    record_attendance_details: str | None
    description: str | None

    @property
    def is_modern(self) -> bool:
        last_renovation = self.renovations[-1] if self.renovations else None
        if isinstance(last_renovation, Duration):
            last_renovation = last_renovation.end
        dates = [d for d in (self.inauguration, last_renovation) if d is not None]
        if not dates:
            return False
        result = max(dates)
        return result >= _KORONA_INAUGURATION


@dataclass(frozen=True)
class Country(_JsonSerializable):
    name: str
    id: str
    confederation: str


POLAND = Country(name='Poland', id='pol', confederation='UEFA')
ENGLAND = Country(name='England', id='eng', confederation='UEFA')
HONG_KONG = Country(name="Hong Kong", id="hkg", confederation="AFC")


@dataclass(frozen=True)
class CountryStadiumsData(_JsonSerializable):
    country: Country
    url: str
    stadiums: tuple[Stadium, ...]

    @property
    def avg_capacity(self) -> float:
        total = sum(s.capacity for s in self.stadiums)
        return total / len(self.stadiums)

    @property
    def avg_capacity_tier1(self) -> float | None:
        stadiums = [s for s in self.stadiums if s.league.tier == 1]
        if not stadiums:
            return None
        total = sum(s.capacity for s in stadiums)
        return total / len(stadiums)

    @property
    def avg_capacity_tier2(self) -> float | None:
        stadiums = [s for s in self.stadiums if s.league.tier == 2]
        if not stadiums:
            return None
        total = sum(s.capacity for s in stadiums)
        return total / len(stadiums)

    @property
    def avg_capacity_tier3(self) -> float | None:
        stadiums = [s for s in self.stadiums if s.league.tier == 3]
        if not stadiums:
            return None
        total = sum(s.capacity for s in stadiums)
        return total / len(stadiums)
