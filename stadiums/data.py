"""

    stadiums.data.py
    ~~~~~~~~~~~~~~~~~~~~
    Data structures.

    @author: z33k

"""
from dataclasses import Field, asdict, dataclass, fields
from datetime import datetime
from typing import Type

from stadiums.constants import Json, READABLE_TIMESTAMP_FORMAT, T
from stadiums.utils import get_classes_in_current_module, get_properties


def _serialize_datetime(data: Json) -> Json:  # recursive
    if isinstance(data, list):
        for idx, item in enumerate(data):
            data[idx] = _serialize_datetime(item)
    elif isinstance(data, dict):
        for k, v in data.items():
            data[k] = _serialize_datetime(v)
    elif isinstance(data, datetime):
        data = data.strftime(READABLE_TIMESTAMP_FORMAT)
    return data


def _convert_from_json(types: dict[str, Type[T]], field: str,  data: Json) -> T | Json:
    if type_ := types.get(field.capitalize()):
        if hasattr(type_, "from_json"):
            return type_.from_json(data)
    return data


def _deserialize_substructs(data: Json) -> dict:
    types = get_classes_in_current_module()
    for k, v in data.items():
        if isinstance(v, tuple) and k.endswith("s"):
            data[k] = tuple(_convert_from_json(types, k[:-1], item) for item in v)
        else:
            data[k] = _convert_from_json(types, k, v)
    return data


@dataclass(frozen=True)
class _JsonSerializable:
    @property
    def json(self) -> Json:
        data = {k: v for k, v in asdict(self).items() if v is not None}
        return _serialize_datetime(data)

    @classmethod
    def _deserialize_datetime(cls, data: Json, field: Field) -> Json:
        try:
            if field.type is datetime:
                data[field.name] = datetime.strptime(data[field.name], READABLE_TIMESTAMP_FORMAT)
            elif field.type is list and isinstance(data[field.name], list):
                data[field.name] = [
                    datetime.strptime(item, READABLE_TIMESTAMP_FORMAT)
                    for item in data[field.name]]
        except ValueError:
            pass
        return data

    @classmethod
    def from_json(cls, data: Json) -> "_JsonSerializable":
        data = {k: v for k, v in data.items() if k not in get_properties(cls)}
        for f in fields(cls):
            if data.get(f.name) is None:
                data[f.name] = None
            else:
                data = cls._deserialize_datetime(data, f)
        data = _deserialize_substructs(data)
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
    tier: int


@dataclass(frozen=True)
class _BasicStadium(_JsonSerializable):
    name: str
    url: str
    town: str | Town
    clubs: tuple[str, ...]
    capacity: int
    league: League

    @property
    def tier(self) -> str:
        return get_tier(self.capacity)


_KORONA_INAUGURATION = datetime(2006, 4, 1, 0, 0)


@dataclass(frozen=True)
class Cost(_JsonSerializable):
    amount: int
    currency: str


@dataclass(frozen=True)
class Stadium(_BasicStadium):
    country: str
    address: str | None
    inauguration: datetime | None
    renovation: datetime | None  # TODO: rename to renovations and parse all of them
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


@dataclass(frozen=True)
class Country(_JsonSerializable):
    name: str
    id: str
    confederation: str


POLAND = Country(name='Poland', id='pol', confederation='UEFA')


@dataclass(frozen=True)
class CountryStadiums(_JsonSerializable):
    country: Country
    url: str
    stadiums: tuple[Stadium, ...]
