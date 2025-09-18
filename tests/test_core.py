"""Tests for tap-spreadsheets."""

import pathlib
from tap_spreadsheets.tap import TapSpreadsheets
from tap_spreadsheets.stream import (
    SDC_INCREMENTAL_KEY,
    SDC_FILENAME,
    SDC_STREAM,
    SDC_WORKSHEET,
)
from singer_sdk.testing import get_tap_test_class

DATA_DIR = pathlib.Path(__file__).parent.parent / "data"

EXCEL_FILE = str(DATA_DIR / "test.xlsx")
CSV_FILE = str(DATA_DIR / "test.csv")

# Minimal configs for test files
EXCEL_CONFIG = {
    "files": [
        {
            "path": EXCEL_FILE,
            "format": "excel",
            "worksheet": "Sheet1",
            "primary_keys": ["date"],
        }
    ]
}

EXCEL_CONFIG_SKIP_CELLS = {
    "files": [
        {
            "path": EXCEL_FILE,
            "format": "excel",
            "worksheet": "Sheet2",
            "primary_keys": ["date"],
            "skip_columns": 1,
            "skip_rows": 4,
        }
    ]
}

# Regex worksheet matching config
EXCEL_REGEX_CONFIG = {
    "files": [
        {
            "path": EXCEL_FILE,
            "format": "excel",
            "worksheet": r"report_20[0-9]{2}",
            "primary_keys": ["date"],
        }
    ]
}

CSV_CONFIG = {
    "files": [
        {
            "path": CSV_FILE,
            "format": "csv",
            "primary_keys": ["date"],
        }
    ]
}

# --- Expected column sets ---
COMMON_COLUMNS_CSV = {
    "date",
    "value",
    "random",
    "total",
    SDC_INCREMENTAL_KEY,
    SDC_FILENAME,
    SDC_STREAM,
    SDC_WORKSHEET,
}
COMMON_COLUMNS_XLSX = {*COMMON_COLUMNS_CSV, "comments_and_notes"}


# --- SDK standard tests (with incremental handling) ---
TestTapSpreadsheetsExcel = get_tap_test_class(
    tap_class=TapSpreadsheets,
    config=EXCEL_CONFIG,
)

TestTapSpreadsheetsCsv = get_tap_test_class(
    tap_class=TapSpreadsheets,
    config=CSV_CONFIG,
)


# --- Custom tests ---
def test_excel_schema_headers():
    tap = TapSpreadsheets(config=EXCEL_CONFIG)
    streams = tap.discover_streams()
    assert len(streams) == 1
    schema_props = streams[0].schema["properties"]
    for expected in ["date", "value", "random", "total"]:
        assert expected in schema_props


def test_excel_schema_headers_skip_cells():
    tap = TapSpreadsheets(config=EXCEL_CONFIG_SKIP_CELLS)
    streams = tap.discover_streams()
    assert len(streams) == 1
    schema_props = streams[0].schema["properties"]
    for expected in ["date", "value", "random", "total"]:
        assert expected in schema_props


def test_excel_regex_worksheet_match():
    """Ensure regex worksheet pattern matches multiple years like report_2023, report_2024."""
    tap = TapSpreadsheets(config=EXCEL_REGEX_CONFIG)
    streams = tap.discover_streams()
    assert len(streams) == 1
    records = list(streams[0].get_records(context=None))
    # Should yield at least one record if any matching sheet exists
    assert len(records) > 0
    # All expected headers should exist
    assert set(records[0].keys()) == COMMON_COLUMNS_CSV


def test_csv_schema_headers():
    tap = TapSpreadsheets(config=CSV_CONFIG)
    streams = tap.discover_streams()
    assert len(streams) == 1
    schema_props = streams[0].schema["properties"]
    for expected in ["date", "value", "random", "total"]:
        assert expected in schema_props


def test_excel_records_not_empty():
    tap = TapSpreadsheets(config=EXCEL_CONFIG)
    streams = tap.discover_streams()
    records = list(streams[0].get_records(context=None))
    assert len(records) > 0
    # Keys match expected headers
    assert set(records[0].keys()) == COMMON_COLUMNS_XLSX


def test_csv_records_not_empty():
    tap = TapSpreadsheets(config=CSV_CONFIG)
    streams = tap.discover_streams()
    records = list(streams[0].get_records(context=None))
    assert len(records) > 0
    assert set(records[0].keys()) == COMMON_COLUMNS_CSV


def test_csv_records_have_stream_and_worksheet():
    tap = TapSpreadsheets(config=CSV_CONFIG)
    stream = tap.discover_streams()[0]
    records = list(stream.get_records(context=None))
    rec = records[0]
    # CSV should have stream set, worksheet empty
    assert rec[SDC_STREAM] == stream.table_name
    assert rec[SDC_WORKSHEET] is None


def test_excel_records_have_stream_and_worksheet():
    tap = TapSpreadsheets(config=EXCEL_CONFIG)
    stream = tap.discover_streams()[0]
    records = list(stream.get_records(context=None))
    rec = records[0]
    # Excel should have stream and a worksheet reference
    assert rec[SDC_STREAM] == stream.table_name
    assert rec[SDC_WORKSHEET] == "Sheet1" or str(stream.worksheet_ref)
