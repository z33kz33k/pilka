"""

    pilka.stadiums.utils.__init__.py
    ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    Project's utilities.

    @author: z33k

"""
import inspect
import os
import logging
import re
import sys
from datetime import date, datetime, timedelta
from functools import wraps
from logging.handlers import RotatingFileHandler
from pathlib import Path
from typing import Callable, Iterable, Optional, Protocol, Sequence, Set, Type

import langcodes
import pandas as pd
from contexttimer import Timer
from langcodes import Language, tag_is_valid

from pilka.constants import PathLike, T, SECONDS_IN_YEAR
from pilka.utils.check_type import type_checker


_log = logging.getLogger(__name__)


class ParsingError(ValueError):
    """Raised whenever parser's assumptions are not met.
    """


def seconds2readable(seconds: float) -> str:
    seconds = round(seconds)
    minutes, seconds = divmod(seconds, 60)
    hours, minutes = divmod(minutes, 60)
    return f"{hours}h:{minutes:02}m:{seconds:02}s"


def timed(operation="", precision=3) -> Callable:
    """Add time measurement to the decorated operation.

    Args:
        operation: name of the time-measured operation (default is function's name)
        precision: precision of the time measurement in seconds (decides output text formatting)

    Returns:
        the decorated function
    """
    if precision < 0:
        precision = 0

    def decorator(func: Callable) -> Callable:
        @wraps(func)
        def wrapper(*args, **kwargs):
            with Timer() as t:
                result = func(*args, **kwargs)
            activity = operation or f"'{func.__name__}()'"
            time = seconds2readable(t.elapsed)
            if not precision:
                _log.info(f"Completed {activity} in {time}")
            elif precision == 1:
                _log.info(f"Completed {activity} in {t.elapsed:.{precision}f} "
                          f"second(s) ({time})")
            else:
                _log.info(f"Completed {activity} in {t.elapsed:.{precision}f} "
                          f"second(s)")
            return result
        return wrapper
    return decorator


@type_checker(pd.DataFrame)
def first_df_row_as_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Make first row of ``df`` its columns.
    """
    return df.rename(columns=df.iloc[0]).drop(df.index[0]).reset_index(drop=True)


@type_checker(str)
def extract_float(text: str) -> float:
    """Extract floating point number from text.
    """
    num = "".join([char for char in text if char.isdigit() or char in ",."])
    if not num:
        raise ParsingError(f"No digits or decimal point in text: {text!r}")
    return float(num.replace(",", "."))


@type_checker(str)
def extract_int(text: str) -> int:
    """Extract an integer from text.
    """
    num = "".join([char for char in text if char.isdigit()])
    if not num:
        raise ParsingError(f"No digits in text: {text!r}")
    return int(num)


@type_checker(str)
def extract_date(text: str, month_in_the_middle=True) -> date:
    """Extract a date object from text.
    """
    sep, stack = None, ["/", ".", "-"]
    while stack:
        token = stack.pop()
        if token in text:
            sep = token
            break

    datestr = "".join([char for char in text if char.isdigit() or char == sep])
    if not datestr:
        raise ParsingError(f"Not a date text: {text!r}")

    if len(datestr) == 4:
        tokens = [datestr]
    else:
        if not sep:
            raise ParsingError(f"Not a date text: {text!r}")
        tokens = datestr.split(sep)

    if len(tokens) == 3:
        first, second, third = tokens
        if len(first) == 4:
            year, month, day = first, second, third
        elif len(third) == 4:
            year, month, day = third, second, first
        else:
            raise ParsingError(f"Not a date text: {text!r}")

        if not month_in_the_middle:
            month, day = day, month

    elif len(tokens) == 2:
        day = 1
        first, second = tokens
        if len(first) == 4:
            year, month = first, second
        elif len(second) == 4:
            year, month = second, first
        else:
            raise ParsingError(f"Not a date text: {text!r}")

    elif len(tokens) == 1:
        year, month, day = tokens[0], 1, 1

    else:
        raise ParsingError(f"Not a date text: {text!r}")

    try:
        year, month, day = int(year), int(month), int(day)
    except ValueError:
        raise ParsingError(f"Not a date text: {text!r}")

    return date(year, month, day)


def from_iterable(iterable: Iterable[T], predicate: Callable[[T], bool]) -> Optional[T]:
    """Return item from ``iterable`` based on ``predicate`` or ``None``, if it cannot be found.
    """
    return next((item for item in iterable if predicate(item)), None)


@type_checker(PathLike)
def getdir(path: PathLike, create_missing=True) -> Path:
    """Return a directory at ``path`` creating it (and all its needed parents) if missing.
    """
    dir_ = Path(path)
    if not dir_.exists() and create_missing:
        _log.warning(f"Creating missing directory at: '{dir_.resolve()}'...")
        dir_.mkdir(parents=True, exist_ok=True)
    else:
        if dir_.is_file():
            raise NotADirectoryError(f"Not a directory: '{dir_.resolve()}'")
    return dir_


@type_checker(PathLike)
def getfile(path: PathLike, ext="") -> Path:
    """Return an existing file at ``path``.
    """
    f = Path(path)
    if not f.is_file():
        raise FileNotFoundError(f"Not a file: '{f.resolve()}'")
    if ext and not f.suffix.lower() == ext.lower():
        raise ValueError(f"Not a {ext!r} file")
    return f


class Comparable(Protocol):
    """Protocol for annotating comparable types.
    """
    def __lt__(self, other) -> bool:
        ...


def is_increasing(seq: Sequence[Comparable]) -> bool:
    if len(seq) < 2:
        return False
    return all(seq[i] > seq[i-1] for i, _ in enumerate(seq, start=1) if i < len(seq))


@type_checker(str)
def langcode2name(langcode: str) -> str | None:
    """Convert ``langcode`` to language name or `None` if it cannot be converted.
    """
    if not tag_is_valid(langcode):
        return None
    lang = Language.get(langcode)
    return lang.display_name()


@type_checker(str)
def name2langcode(langname: str, alpha3=False) -> str | None:
    """Convert supplied language name to a 2-letter ISO language code or `None` if it cannot be
    converted. Optionally, convert it to 3-letter ISO code (aka "alpha3").
    """
    try:
        lang = langcodes.find(langname)
    except LookupError:
        return None
    if alpha3:
        return lang.to_alpha3()
    return str(lang)


_logging_initialized = False


def init_log() -> None:
    """Initialize logging.
    """
    global _logging_initialized

    if not _logging_initialized:
        output_dir = Path(os.getcwd()) / "var" / "logs"
        if output_dir.exists():
            logfile = output_dir / "pilka.log"
        else:
            logfile = "pilka.log"

        log_format = '%(asctime)s [%(name)s] %(levelname)s: %(message)s'
        log_level = logging.INFO

        root_logger = logging.getLogger()
        root_logger.setLevel(log_level)
        formatter = logging.Formatter(log_format)
        handler = RotatingFileHandler(logfile, maxBytes=1024*1024*10, backupCount=10)
        handler.setFormatter(formatter)
        handler.setLevel(log_level)
        root_logger.addHandler(handler)

        stream_handler = logging.StreamHandler()
        stream_handler.setFormatter(formatter)
        stream_handler.setLevel(log_level)
        root_logger.addHandler(stream_handler)

        _logging_initialized = True


@type_checker(datetime, datetime)
def timedelta2years(start: datetime, stop: datetime) -> float:
    delta = stop - start
    return delta.total_seconds() / SECONDS_IN_YEAR


def get_classes_in_module(module_name: str) -> dict[str, Type]:
    current_module = sys.modules[module_name]
    return {name: obj for name, obj in inspect.getmembers(current_module, inspect.isclass)
            if obj.__module__ == current_module.__name__}


def get_properties(cls: Type) -> Set[str]:
    return {name for name, obj in inspect.getmembers(cls) if isinstance(obj, property)}


def totuple(lst: list) -> tuple:
    """Convert ``lst`` and any list it contains (no matter the nesting level) recursively to tuple.

    Taken from:
        https://stackoverflow.com/a/27050037/4465708
    """
    return tuple(totuple(i) if isinstance(i, list) else i for i in lst)


def tolist(tpl: tuple) -> list:
    """Convert ``tpl`` and any tuple it contains (no matter the nesting level) recursively to list.

    Taken from and maid in reverse:
        https://stackoverflow.com/a/27050037/4465708
    """
    return list(tolist(i) if isinstance(i, tuple) else i for i in tpl)


def cleardir(obj: object) -> list[str]:
    """Return ``dir(obj)`` without extraneous fluff.
    """
    return [attr for attr in dir(obj) if not attr.startswith("_")]


def clean_parenthesized(text: str) -> str:
    """Get rid of anything in text within (single or multiple) parentheses.
    """
    if " (" in text:
        pattern = r"\s\(.*?\)"
        text = re.sub(pattern, "", text)
    if "(" in text:
        pattern = r"\(.*?\)"
        text = re.sub(pattern, "", text)
    return text


def trim_suffix(text: str, suffix="") -> str:
    return text[:-len(suffix)] if text.endswith(suffix) else text
