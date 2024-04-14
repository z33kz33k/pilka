"""

    stadiums.utils.scrape.py
    ~~~~~~~~~~~~~~~~~~~~~~~~~
    Utilities for scraping.

    @author: z33k

"""
import logging
import time
from functools import wraps
from typing import Callable, Dict

import requests
from requests.exceptions import HTTPError
from bs4 import BeautifulSoup

from stadiums.constants import REQUEST_TIMEOUT
from stadiums.utils import timed, type_checker


_log = logging.getLogger(__name__)


class ParsingError(ValueError):
    """Raised whenever parser's assumptions are not met.
    """


@timed("request")
@type_checker(str)
def getsoup(url: str, headers: Dict[str, str] | None = None) -> BeautifulSoup:
    """Return BeautifulSoup object based on ``url``.

    Args:
        url: URL string
        headers: a dictionary of headers to add to the request

    Returns:
        a BeautifulSoup object
    """
    _log.info(f"Requesting: {url!r}")
    response = requests.get(url, timeout=REQUEST_TIMEOUT, headers=headers)
    if str(response.status_code)[0] in ("4", "5"):
        msg = f"Request failed with: '{response.status_code} {response.reason}'"
        if response.status_code in (502, 503, 504):
            raise HTTPError(msg)
        _log.warning(msg)
    return BeautifulSoup(response.text, "lxml")


def throttle(delay: float) -> None:
    _log.info(f"Throttling for {delay} seconds...")
    time.sleep(delay)


def throttled(delay: float | Callable) -> Callable:
    """Add throttling delay after the decorated operation.

    Args:
        throttling delay in fraction of seconds

    Returns:
        the decorated function
    """
    def decorate(func: Callable) -> Callable:
        @wraps(func)
        def wrapper(*args, **kwargs):
            result = func(*args, **kwargs)
            if callable(delay):
                amount = delay()
            else:
                amount = delay
            throttle(amount)
            return result
        return wrapper
    return decorate
