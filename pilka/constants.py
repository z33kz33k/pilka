"""

    pilka.stadiums.constants.py
    ~~~~~~~~~~~~~~~~~~~~~~~~~~~
    Script's constants

    @author: z33k

"""
import os
import logging
from pathlib import Path
from typing import Any, Callable, TypeVar

_log = logging.getLogger(__name__)

# type hints
T = TypeVar("T")
Json = dict[str, Any]
PathLike = str | Path
Method = Callable[[Any, tuple[Any, ...]], Any]  # method with signature def methodname(self, *args)
Function = Callable[[tuple[Any, ...]], Any]  # function with signature def funcname(*args)

REQUEST_TIMEOUT = 15  # seconds
FILENAME_TIMESTAMP_FORMAT = "%Y%m%d_%H%M%S"
READABLE_TIMESTAMP_FORMAT = "%Y-%m-%d %H:%M:%S"
SECONDS_IN_YEAR = 365.25 * 24 * 60 * 60  # with leap years

OUTPUT_DIR = Path(os.getcwd()) / "var" / "output"
