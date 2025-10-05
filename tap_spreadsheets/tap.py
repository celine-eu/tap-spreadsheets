"""Spreadsheets tap class."""

from __future__ import annotations

from singer_sdk import Tap
from singer_sdk import typing as th  # JSON schema typing helpers

from tap_spreadsheets.stream import SpreadsheetStream


class TapSpreadsheets(Tap):
    """Spreadsheet tap class."""

    name = "tap-spreadsheets"

    config_jsonschema = th.PropertiesList(
        th.Property(
            "files",
            th.ArrayType(
                th.ObjectType(
                    th.Property(
                        "path",
                        th.StringType,
                        required=True,
                        description="Glob expression for files (e.g. '/data/*.xlsx').",
                    ),
                    th.Property(
                        "format",
                        th.StringType,
                        required=True,
                        description="File format: 'excel' or 'csv'.",
                    ),
                    th.Property(
                        "worksheet",
                        th.StringType,
                        description="Worksheet index or name (Excel only).",
                    ),
                    th.Property(
                        "table_name",
                        th.StringType,
                        description="Optional table/stream name. Defaults to file name.",
                    ),
                    th.Property(
                        "primary_keys",
                        th.ArrayType(th.StringType),
                        description="Primary key columns.",
                    ),
                    th.Property(
                        "date_column",
                        th.StringType,
                        description="Date column to use as replication key",
                    ),
                    th.Property(
                        "date_column_format",
                        th.StringType,
                        description="Date column format, in strptime format (e.g. %Y-%m-%d)",
                    ),
                    th.Property(
                        "drop_empty",
                        th.BooleanType,
                        default=True,
                        description="Drop rows with null PKs.",
                    ),
                    th.Property(
                        "skip_columns",
                        th.IntegerType,
                        default=0,
                        description="Columns to skip.",
                    ),
                    th.Property(
                        "skip_rows",
                        th.IntegerType,
                        default=0,
                        description="Rows to skip before headers.",
                    ),
                    th.Property(
                        "sample_rows",
                        th.IntegerType,
                        default=100,
                        description="Rows to sample for schema inference.",
                    ),
                    th.Property(
                        "column_headers",
                        th.ArrayType(th.StringType),
                        description="Explicit headers (optional).",
                    ),
                    th.Property(
                        "delimiter",
                        th.StringType,
                        description='CSV delimiter, inferred or default to "," ',
                    ),
                    th.Property(
                        "quotechar",
                        th.StringType,
                        description="CSV quote char, inferred or default to '\"' ",
                    ),
                    th.Property(
                        "schema_overrides",
                        th.ObjectType(additional_properties=True),
                        default={},
                        description="Override columns JSON schema definition.",
                    ),
                )
            ),
            required=True,
            description="List of file configurations.",
        ),
    ).to_dict()

    def discover_streams(self):
        """Return one stream per table_name (not per file)."""
        streams: list[SpreadsheetStream] = []
        for file_cfg in self.config["files"]:
            streams.append(SpreadsheetStream(self, file_cfg))
        return streams


if __name__ == "__main__":
    TapSpreadsheets.cli()
