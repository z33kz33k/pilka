"""

    pilka.stadiums.constants
    ~~~~~~~~~~~~~~~~~~~~~~~~
    Script's constants

    @author: z33k

"""
import logging
import os
from datetime import date, datetime
from pathlib import Path
from typing import Dict, List, TypeVar, Union

_log = logging.getLogger(__name__)

# type aliases
type T = TypeVar("T")
type Json = Union[str, int, float, bool, datetime, date, None, Dict[str, "Json"], List["Json"]]
type PathLike = str | Path

REQUEST_TIMEOUT = 15  # seconds
FILENAME_TIMESTAMP_FORMAT = "%Y%m%d_%H%M%S"
READABLE_TIMESTAMP_FORMAT = "%Y-%m-%d %H:%M:%S"
SECONDS_IN_YEAR = 365.25 * 24 * 60 * 60  # with leap years

OUTPUT_DIR = Path(os.getcwd()) / "var" / "output"
