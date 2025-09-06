"""Tests for tap-spreadsheets."""

import pathlib
from singer_sdk.testing import get_tap_test_class

from tap_spreadsheets.tap import TapSpreadsheets

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

EXCEL_CONFIG2 = {
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

CSV_CONFIG = {
    "files": [
        {
            "path": CSV_FILE,
            "format": "csv",
            "primary_keys": ["date"],
        }
    ]
}


# --- SDK built-in tests ---
TestTapSpreadsheetExcel = get_tap_test_class(
    tap_class=TapSpreadsheets,
    config=EXCEL_CONFIG,
)

TestTapSpreadsheetCsv = get_tap_test_class(
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
def test_excel_schema_headers2():
    tap = TapSpreadsheets(config=EXCEL_CONFIG2)
    streams = tap.discover_streams()
    assert len(streams) == 1
    schema_props = streams[0].schema["properties"]
    for expected in ["date", "value", "random", "total"]:
        assert expected in schema_props


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
    assert set(records[0].keys()) == {"date", "value", "random", "total"}


def test_csv_records_not_empty():
    tap = TapSpreadsheets(config=CSV_CONFIG)
    streams = tap.discover_streams()
    records = list(streams[0].get_records(context=None))
    assert len(records) > 0
    assert set(records[0].keys()) == {"date", "value", "random", "total"}
