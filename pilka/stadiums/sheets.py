"""

    pilka.stadiums.sheets.py
    ~~~~~~~~~~~~~~~~~~~~~~~~~
    Google Sheets as backend/frontend.

    @author: z33k

"""
import logging
from typing import List

import gspread

from pilka.stadiums.utils import timed
from pilka.stadiums.utils.check_type import generic_iterable_type_checker, type_checker

_log = logging.getLogger(__name__)


@type_checker(str, str)
def _worksheet(spreadsheet: str, worksheet: str) -> gspread.Worksheet:
    creds_file = "scraping_service_account.json"
    client = gspread.service_account(filename=creds_file)
    spreadsheet = client.open(spreadsheet)
    worksheet = spreadsheet.worksheet(worksheet)
    return worksheet


@timed("retrieving from Google Sheets")
def retrieve_from_gsheets_col(spreadsheet: str, worksheet: str, col=1, start_row=1,
                              ignore_none=True) -> List[str]:
    """Retrieve a list of string values from a Google Sheets worksheet.
    """
    if col < 1 or start_row < 1:
        raise ValueError("Column and start row must be positive integers")
    worksheet = _worksheet(spreadsheet, worksheet)
    values = worksheet.col_values(col, value_render_option="UNFORMATTED_VALUE")[start_row-1:]
    if ignore_none:
        return [value for value in values if value is not None]
    return values


@timed("saving to Google Sheets")
@generic_iterable_type_checker(str)
def save_to_gsheets_col(values: List[str], spreadsheet: str, worksheet: str, col=1,
                        start_row=1) -> None:
    """Save a list of strings to a Google Sheets worksheet.
    """
    if col < 1 or start_row < 1:
        raise ValueError("Column and start row must be positive integers")
    worksheet = _worksheet(spreadsheet, worksheet)
    worksheet.insert_rows([[value] for value in values], row=start_row)
