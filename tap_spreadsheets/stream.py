"""SpreadsheetStream class."""

from __future__ import annotations

import typing as t
import re
import fnmatch
import csv
from openpyxl import load_workbook
from singer_sdk.streams import Stream
from singer_sdk import typing as th
from logging import getLogger
import csv

from tap_spreadsheets.storage import Storage

if t.TYPE_CHECKING:
    from singer_sdk.helpers.types import Context

logger = getLogger(__name__)


class SpreadsheetStream(Stream):
    """Stream class for spreadsheet (CSV/Excel) files."""

    def __init__(self, tap, file_cfg: dict) -> None:
        self.file_cfg = file_cfg
        self.path_glob: str = file_cfg["path"]
        self.format: str = file_cfg["format"].lower()
        self.worksheet_ref: str | int | None = file_cfg.get("worksheet")
        self.table_name = file_cfg.get("table_name")
        
        if not self.table_name:
            self.table_name = "spreadsheet_stream"
            if self.format == "excel" and self.worksheet_ref:
                self.table_name = f"{self.table_name}_{self.worksheet_ref}"

        super().__init__(tap, name=self.table_name)

        self.primary_keys = [n.lower() for n in file_cfg.get("primary_keys", [])]
        self.drop_empty = file_cfg.get("drop_empty", True)
        self.skip_columns = file_cfg.get("skip_columns", 0)
        self.skip_rows = file_cfg.get("skip_rows", 0)
        self.sample_rows = file_cfg.get("sample_rows", 100)
        self.column_headers = file_cfg.get("column_headers")

        self._schema = None
        self.storage = Storage(self.path_glob)

    def _iter_excel(self, file: str):
        """Iterate rows from all Excel worksheets that match index, name, or pattern."""
        with self.storage.open(file, "rb") as fh:
            wb = load_workbook(fh, read_only=True, data_only=True)

            worksheets = []

            if isinstance(self.worksheet_ref, int):
                # Select by index
                try:
                    worksheets = [wb.worksheets[self.worksheet_ref]]
                except IndexError:
                    raise ValueError(
                        f"Worksheet index {self.worksheet_ref} out of range in {file}. "
                        f"Available indexes: 0..{len(wb.worksheets) - 1}"
                    )

            elif isinstance(self.worksheet_ref, str):
                # 1. Exact match
                if self.worksheet_ref in wb.sheetnames:
                    worksheets = [wb[self.worksheet_ref]]
                else:
                    # 2. Glob or regex match → collect ALL matches
                    pattern = self.worksheet_ref

                    if any(ch in pattern for ch in ["*", "?"]):
                        regex = re.compile(fnmatch.translate(pattern))
                    else:
                        regex = re.compile(pattern)

                    matches = [name for name in wb.sheetnames if regex.match(name)]
                    if not matches:
                        raise ValueError(
                            f"No worksheets match '{pattern}' in {file}. "
                            f"Available: {wb.sheetnames}"
                        )
                    worksheets = [wb[name] for name in matches]

            else:
                raise ValueError(
                    "Excel format requires worksheet to be an integer (index), "
                    "a string (name), or a pattern (glob/regex)."
                )

            # Yield rows from all matched worksheets
            for ws in worksheets:
                for row in ws.iter_rows(min_row=self.skip_rows + 1, values_only=True):
                    yield row

    def _iter_csv(self, file: str):
        with self.storage.open(file, "rt") as fh:
            # Peek at a sample to detect dialect if not explicitly set
            sample = fh.read(4096)
            fh.seek(0)

            delimiter = self.file_cfg.get("delimiter")
            quotechar = self.file_cfg.get("quotechar")

            if not delimiter or not quotechar:
                try:
                    sniffer = csv.Sniffer()
                    dialect = sniffer.sniff(sample)
                    delimiter = delimiter or dialect.delimiter
                    quotechar = quotechar or dialect.quotechar
                except csv.Error:
                    # fallback defaults
                    delimiter = delimiter or ","
                    quotechar = quotechar or '"'

            reader = csv.reader(fh, delimiter=delimiter, quotechar=quotechar)

            # skip configured number of rows
            for _ in range(self.skip_rows):
                next(reader, None)

            for row in reader:
                yield row

    def _get_files(self) -> list[str]:
        files = self.storage.glob()
        if not files:
            raise FileNotFoundError(f"No {self.format} files found for {self.path_glob}")
        return files

    @property
    def schema(self) -> dict:
        if self._schema:
            return self._schema

        sample_file = self._get_files()[0]
        rows = (
            self._iter_excel(sample_file) if self.format == "excel" else self._iter_csv(sample_file)
        )

        # headers
        if self.column_headers:
            headers = self.column_headers
        else:
            try:
                raw_headers = next(rows)
            except StopIteration:
                raise ValueError(f"No header row found in {sample_file}")
            headers = [
                str(h).strip().lower() if h not in (None, "") else f"col_{i}"
                for i, h in enumerate(raw_headers)
            ]
        headers = headers[self.skip_columns :]

        # samples
        samples = []
        for i, row in enumerate(rows):
            if i >= self.sample_rows:
                break
            samples.append(row)

        # type inference
        types: dict[str, th.JSONTypeHelper] = {}
        for i, h in enumerate(headers):
            col_idx = i + self.skip_columns
            col_values = [
                r[col_idx] for r in samples if col_idx < len(r) and r[col_idx] not in (None, "")
            ]
            if not col_values:
                types[h] = th.StringType()
            elif all(isinstance(v, int) for v in col_values):
                types[h] = th.IntegerType()
            elif all(isinstance(v, (int, float)) for v in col_values):
                types[h] = th.NumberType()
            else:
                types[h] = th.StringType()

        self._schema = th.PropertiesList(
            *(th.Property(name.lower(), tpe) for name, tpe in types.items())
        ).to_dict()
        return self._schema

    def get_records(self, context: Context | None) -> t.Iterable[dict]:
        for file in self._get_files():
            rows = (
                self._iter_excel(file) if self.format == "excel" else self._iter_csv(file)
            )

            # headers
            if self.column_headers:
                headers = self.column_headers
            else:
                try:
                    raw_headers = next(rows)
                except StopIteration:
                    logger.warning("File %s has no data rows.", file)
                    continue
                headers = [
                    str(h).strip().lower() if h not in (None, "") else f"col_{i}"
                    for i, h in enumerate(raw_headers)
                ]
            headers = headers[self.skip_columns :]

            for row in rows:
                record = {
                    h: row[i + self.skip_columns] if i + self.skip_columns < len(row) else None
                    for i, h in enumerate(headers)
                }

                if self.drop_empty and any(record.get(pk) in (None, "") for pk in self.primary_keys):
                    continue

                yield record
