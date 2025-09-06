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
from decimal import Decimal
from datetime import datetime, date, time, timedelta

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
        self._headers: list[str] = []
        self.storage = Storage(self.path_glob)

    def _stem_header(self, h: t.Any, idx: int) -> str:
        """Normalize header names to safe identifiers."""
        # If we accidentally get a Cell or other object, extract its value
        if hasattr(h, "value"):
            h = h.value

        if h is None or str(h).strip() == "":
            return f"col_{idx}"

        h = str(h)
        import unicodedata, re
        h = unicodedata.normalize("NFKD", h).encode("ascii", "ignore").decode()
        h = h.replace("\n", " ").replace("/", " ")
        h = h.lower()
        h = re.sub(r"[^a-z0-9]+", "_", h)
        h = re.sub(r"_+", "_", h)
        h = h.strip("_")

        return h or f"col_{idx}"

    def _infer_type(self, col_values: list[t.Any]):
        """Infer a JSON schema type from sample values with safe fallback."""
        if not col_values:
            return th.StringType()

        norm = []
        for v in col_values:
            if isinstance(v, Decimal):
                norm.append(float(v))
            else:
                norm.append(v)
        col_values = norm

        if all(isinstance(v, int) for v in col_values):
            return th.IntegerType()
        if all(isinstance(v, (int, float)) for v in col_values):
            return th.NumberType()
        if all(isinstance(v, str) for v in col_values):
            return th.StringType()
        if any(isinstance(v, (int, float)) for v in col_values):
            return th.NumberType()
        return th.StringType()

    def _coerce_value(self, v: t.Any, expected_type: str | None = None) -> t.Any:
        if v is None:
            return None
        if isinstance(v, Decimal):
            return float(v)
        if expected_type == "integer":
            try:
                return int(v)
            except Exception:
                return None
        if expected_type == "number":
            try:
                return float(v)
            except Exception:
                return None
        if isinstance(v, (datetime, date, time)):
            return v.isoformat()
        if isinstance(v, timedelta):
            return v.total_seconds()
        return v

    def _extract_headers_excel(self, file: str) -> list[str]:
        with self.storage.open(file, "rb") as fh:
            wb = load_workbook(fh, read_only=True, data_only=True)

            if isinstance(self.worksheet_ref, int):
                try:
                    ws = wb.worksheets[self.worksheet_ref]
                except IndexError:
                    raise ValueError(
                        f"Worksheet index {self.worksheet_ref} out of range in {file}."
                    )
            elif isinstance(self.worksheet_ref, str):
                if self.worksheet_ref in wb.sheetnames:
                    ws = wb[self.worksheet_ref]
                else:
                    pattern = self.worksheet_ref
                    regex = (
                        re.compile(fnmatch.translate(pattern))
                        if any(ch in pattern for ch in ["*", "?"])
                        else re.compile(pattern)
                    )
                    matches = [name for name in wb.sheetnames if regex.match(name)]
                    if not matches:
                        raise ValueError(
                            f"No worksheets match '{pattern}' in {file}. "
                            f"Available: {wb.sheetnames}"
                        )
                    ws = wb[matches[0]]
            else:
                raise ValueError("worksheet_ref must be int, str, or regex pattern")

            header_row = ws.iter_rows(
                min_row=self.skip_rows + 1,
                max_row=self.skip_rows + 1,
                values_only=True,
            )
            raw_headers = next(header_row)

        return [self._stem_header(h, i) for i, h in enumerate(raw_headers)][self.skip_columns :]

    def _iter_excel(self, file: str):
        """Iterate data rows (excluding header) from all matched worksheets."""
        with self.storage.open(file, "rb") as fh:
            wb = load_workbook(fh, read_only=True, data_only=True)

            worksheets = []
            if isinstance(self.worksheet_ref, int):
                try:
                    worksheets = [wb.worksheets[self.worksheet_ref]]
                except IndexError:
                    raise ValueError(
                        f"Worksheet index {self.worksheet_ref} out of range in {file}. "
                        f"Available: 0..{len(wb.worksheets)-1}"
                    )
            elif isinstance(self.worksheet_ref, str):
                if self.worksheet_ref in wb.sheetnames:
                    worksheets = [wb[self.worksheet_ref]]
                else:
                    pattern = self.worksheet_ref
                    regex = (
                        re.compile(fnmatch.translate(pattern))
                        if any(ch in pattern for ch in ["*", "?"])
                        else re.compile(pattern)
                    )
                    matches = [name for name in wb.sheetnames if regex.match(name)]
                    if not matches:
                        raise ValueError(
                            f"No worksheets match '{pattern}' in {file}. "
                            f"Available: {wb.sheetnames}"
                        )
                    worksheets = [wb[name] for name in matches]
            else:
                raise ValueError(
                    "Excel worksheet must be an int (index), str (name), or pattern."
                )

            for ws in worksheets:
                # data starts after the header row
                start_row = self.skip_rows + 2
                for row in ws.iter_rows(min_row=start_row, values_only=True):
                    yield row

    def _extract_headers_csv(self, file: str) -> list[str]:
        """Extract and normalize headers from a CSV file."""
        with self.storage.open(file, "rt") as fh:
            reader = csv.reader(fh)

            # Skip configured rows before header
            for _ in range(self.skip_rows):
                next(reader, None)

            try:
                raw_headers = next(reader)
            except StopIteration:
                raise ValueError(f"No header row found in {file}")

        headers: list[str] = []
        for i, h in enumerate(raw_headers):
            # Fallback if header is empty or looks numeric
            if h is None or str(h).strip() == "":
                headers.append(f"col_{i}")
            elif str(h).strip().isnumeric():
                headers.append(f"col_{i}")
            else:
                headers.append(self._stem_header(h, i))

        return headers[self.skip_columns :]

    def _iter_csv(self, file: str):
        with self.storage.open(file, "rt") as fh:
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
                    delimiter = delimiter or ","
                    quotechar = quotechar or '"'

            reader = csv.reader(fh, delimiter=delimiter, quotechar=quotechar)

            # skip configured number of rows + the header
            for _ in range(self.skip_rows + 1):
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
            self._iter_excel(sample_file)
            if self.format == "excel"
            else self._iter_csv(sample_file)
        )

        if self.column_headers:
            headers = [self._stem_header(h, i) for i, h in enumerate(self.column_headers)]
        else:
            if self.format == "excel":
                headers = self._extract_headers_excel(sample_file)
            else:
                headers = self._extract_headers_csv(sample_file)

        self._headers = headers


        samples = []
        for i, row in enumerate(rows):
            if i >= self.sample_rows:
                break
            samples.append(row)

        types: dict[str, th.JSONTypeHelper] = {}
        for i, h in enumerate(headers):
            col_idx = i + self.skip_columns
            col_values = [
                r[col_idx] for r in samples if col_idx < len(r) and r[col_idx] not in (None, "")
            ]
            types[h] = self._infer_type(col_values)

        self._schema = th.PropertiesList(
            *(th.Property(name.lower(), tpe) for name, tpe in types.items())
        ).to_dict()
        return self._schema

    def get_records(self, context: Context | None) -> t.Iterable[dict]:
        if not self._headers:
            _ = self.schema  # ensures headers are set
        headers = self._headers

        for file in self._get_files():
            rows = (
                self._iter_excel(file)
                if self.format == "excel"
                else self._iter_csv(file)
            )

            expected_types = {
                name: schema_def.get("type", ["string"])
                for name, schema_def in self.schema["properties"].items()
            }

            for row in rows:
                record = {
                    h: self._coerce_value(
                        row[i + self.skip_columns] if i + self.skip_columns < len(row) else None,
                        "integer" if "integer" in expected_types.get(h, []) else
                        "number" if "number" in expected_types.get(h, []) else
                        "string"
                    )
                    for i, h in enumerate(headers)
                }
                if self.drop_empty and any(record.get(pk) in (None, "") for pk in self.primary_keys):
                    continue
                yield record
