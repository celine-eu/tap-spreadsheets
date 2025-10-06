# tap-spreadsheets

`tap-spreadsheets` is a Singer tap for spreadsheets.

Built with the [Meltano Tap SDK](https://sdk.meltano.com) for Singer Taps.

## Capabilities

- `catalog`
- `state`
- `discover`
- `activate-version`
- `about`
- `stream-maps`
- `schema-flattening`
- `batch`

## Supported Python Versions

- 3.10
- 3.11
- 3.12
- 3.13
- 3.14

## Settings

| Setting | Required | Default | Description |
|:--------|:--------:|:-------:|:------------|
| files | True | None | List of file configurations. |
| stream_maps | False | None | Config object for stream maps capability. For more information check out [Stream Maps](https://sdk.meltano.com/en/latest/stream_maps.html). |
| stream_maps.__else__ | False | None | Currently, only setting this to `__NULL__` is supported. This will remove all other streams. |
| stream_map_config | False | None | User-defined config values to be used within map expressions. |
| faker_config | False | None | Config for the [`Faker`](https://faker.readthedocs.io/en/master/) instance variable `fake` used within map expressions. Only applicable if the plugin specifies `faker` as an additional dependency (through the `singer-sdk` `faker` extra or directly). |
| faker_config.seed | False | None | Value to seed the Faker generator for deterministic output: https://faker.readthedocs.io/en/master/#seeding-the-generator |
| faker_config.locale | False | None | One or more LCID locale strings to produce localized output for: https://faker.readthedocs.io/en/master/#localization |
| flattening_enabled | False | None | 'True' to enable schema flattening and automatically expand nested properties. |
| flattening_max_depth | False | None | The max depth to flatten schemas. |
| batch_config | False | None | Configuration for BATCH message capabilities. |
| batch_config.encoding | False | None | Specifies the format and compression of the batch files. |
| batch_config.encoding.format | False | None | Format to use for batch files. |
| batch_config.encoding.compression | False | None | Compression format to use for batch files. |
| batch_config.storage | False | None | Defines the storage layer to use when writing batch files |
| batch_config.storage.root | False | None | Root path to use when writing batch files. |
| batch_config.storage.prefix | False | None | Prefix to use when writing batch files. |

A full list of supported settings and capabilities is available by running: `tap-spreadsheets --about`

## Configuration

### Accepted Config Options

`files` (array) List of file configurations. Each entry is an object with keys:
- `path` (string, required): Glob expression (local or S3).
- `format` (string): 'excel' or 'csv'.
- `worksheet` (string, required for type excel): Worksheet index, name or regular expression (Excel only). Using regular expressions, any matching worksheet will be processed.
- `table_name` (string): Optional stream name (defaults to file name).
- `primary_keys` (array): List of PK column names.
- `drop_empty` (boolean): Drop rows with empty/null PKs.
- `skip_columns` (integer): Number of leading columns to skip.
- `skip_rows` (integer): Rows to skip before headers.
- `sample_rows` (integer): Rows to sample for schema inference.
- `column_headers` (array): Explicit column headers.
- `delimiter` (string): CSV delimiter. Inferred if not provided or default to ",".
- `quotechar` (string): CSV quote char. Inferred if not provided or default '"'.
- `schema_overrides` (dict): Overrrides JSON schema definition per field. Eg. `schema_overrides: { my_column_name: { type: [string, "null"] } }`


### Example

```yaml
      config:
        files:
          - path: data/*.xlsx
            format: excel
            # table_name: test_sheet1
            primary_keys: [date]
            drop_empty: true
            worksheet: Sheet1

          - path: data/*.xlsx
            format: excel
            worksheet: "Report 20[0-9]{2}"
            table_name: my_xlsx_sheet2
            primary_keys: [date, total]
            drop_empty: true
            skip_columns: 1
            skip_rows: 4

          - path: s3://my-bucket/reports/*.csv
            format: csv
            table_name: csv_reports
            primary_keys: [id]
            delimiter: ";"
            quotechar: "'"
```

To use an S3-based storage ensure to provide those envirnoment variables: 

- `S3_ACCESS_KEY_ID`, `S3_SECRET_ACCESS_KEY` access key/secret pair
- `S3_ENDPOINT_URL` Custom S3 endpoint such as minio or compatible interface

Example:

`S3_ACCESS_KEY_ID=minioadmin S3_SECRET_ACCESS_KEY=minioadmin S3_ENDPOINT_URL=http://localhost:19000 meltano run tap-spreadsheets target-jsonl`


A full list of supported settings and capabilities for this
tap is available by running:

```bash
tap-spreadsheets --about
```

### Configure using environment variables

This Singer tap will automatically import any environment variables within the working directory's
`.env` if the `--config=ENV` is provided, such that config values will be considered if a matching
environment variable is set either in the terminal context or in the `.env` file.


## Installation

Install from PyPI:

Install from GitHub:

```bash
uv tool install git+https://github.com/ORG_NAME/tap-spreadsheets.git@main
```


## Usage

You can easily run `tap-spreadsheets` by itself or in a pipeline using [Meltano](https://meltano.com/).

### Executing the Tap Directly

```bash
tap-spreadsheets --version
tap-spreadsheets --help
tap-spreadsheets --config CONFIG --discover > ./catalog.json
```

## Developer Resources

Follow these instructions to contribute to this project.

### Initialize your Development Environment

Prerequisites:

- Python 3.10+
- [uv](https://docs.astral.sh/uv/)

```bash
uv sync
```

### Create and Run Tests

Create tests within the `tests` subfolder and
then run:

```bash
uv run pytest
```

You can also test the `tap-spreadsheets` CLI interface directly using `uv run`:

```bash
uv run tap-spreadsheets --help
```

### Testing with [Meltano](https://www.meltano.com)

_**Note:** This tap will work in any Singer environment and does not require Meltano.
Examples here are for convenience and to streamline end-to-end orchestration scenarios._

<!--
Developer TODO:
Your project comes with a custom `meltano.yml` project file already created. Open the `meltano.yml` and follow any "TODO" items listed in
the file.
-->

Next, install Meltano (if you haven't already) and any needed plugins:

```bash
# Install meltano
uv tool install meltano
# Initialize meltano within this directory
cd tap-spreadsheets
meltano install
```

Now you can test and orchestrate using Meltano:

```bash
# Test invocation:
meltano invoke tap-spreadsheets --version

# OR run a test ELT pipeline:
meltano run tap-spreadsheets target-jsonl
```

### SDK Dev Guide

See the [dev guide](https://sdk.meltano.com/en/latest/dev_guide.html) for more instructions on how to use the SDK to
develop your own taps and targets.
