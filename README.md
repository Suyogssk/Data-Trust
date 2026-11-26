# AI-Powered Data Quality Monitoring System

A modular customer-data quality pipeline and Streamlit dashboard. It accepts CSV and Excel uploads, profiles source data, applies configurable PySpark validations, writes Bronze/Silver/Quarantine Parquet layers, and exports quality reports. Optional LLM explanations use aggregate metrics only; the rule-based explanation works without credentials.

## Current implementation

- CSV and Excel ingestion with upload type, size, parse, and header checks.
- Synthetic 10,000-row dataset generator with reproducible quality defects.
- PySpark profiling, independent YAML-configured validation rules, and valid/quarantine classification.
- Bronze source, Silver valid rows, and Quarantine invalid rows with issue codes and details.
- JSON, CSV, and Excel exports; Streamlit dashboard screens for overview, upload, profiling, validation, quarantine, AI explanations, and downloads.
- Optional OpenAI-compatible LLM endpoint; no API key is required for the core application.
- Unit/integration checks in `tests/`.

The local 10,000-row dataset completed the pipeline in this workspace. That is a functional demonstration, not a benchmark or a claim of large-scale production readiness. The source repository is [Suyogssk/Data-Trust](https://github.com/Suyogssk/Data-Trust); Streamlit Community Cloud publication and a public app URL remain pending.

## Architecture and quality definitions

See [architecture and data flow](docs/architecture.md), [data dictionary](docs/data_dictionary.md), and [local setup notes](docs/setup.md).

- Valid records have no enabled rule failures; invalid records carry one or more rule IDs and descriptions.
- Duplicate records count all rows in duplicate `customer_id` groups, including the first row in each conflicting group.
- Missing values count blank or null source cells across source columns.
- Overall quality is `100 × valid_records / total_records`; an empty input is rejected during ingestion.
- AI receives aggregate metrics and schema-level issue names only. It never receives full rows or values, and it cannot modify data.

## Setup

Use Python 3.12 and Java 17. On Windows, a local virtual environment can be created with:

```powershell
python -m venv .venv
.\.venv\Scripts\Activate.ps1
python -m pip install -r requirements.txt
```

If shell activation is disabled, use `.\.venv\Scripts\python.exe` for commands. Java must be installed and `JAVA_HOME` must point to its JDK directory when it is not already configured. Windows Spark emits Hadoop native library warnings; this project uses a bounded PyArrow Parquet fallback for local Windows storage when `winutils.exe` is not configured. Spark still performs profiling and validation.

## Generate sample data

```powershell
.\.venv\Scripts\python.exe scripts\generate_sample_data.py
```

The default output is 10,000 deterministic synthetic rows at `data/raw/customers_dirty.csv` (seed 42). It includes blanks, duplicates, malformed emails/phones, invalid ages/dates, alternate date formats, and unexpected statuses. Use `--rows 5000 --seed 7` to change the size and seed. Generated datasets and Spark outputs are ignored by Git.

## Run the pipeline from a file

```powershell
.\.venv\Scripts\python.exe run_pipeline.py data\raw\customers_dirty.csv
```

The CLI prints a JSON summary and writes the three Parquet layers under `data/`. By default, the run limit is 10,000 records; change it with `--max-records` for a deliberately larger local run. `--rules` and `--storage` select a different rules YAML or output directory.

## Run the dashboard

```powershell
.\.venv\Scripts\streamlit.exe run app.py
```

Open the local URL printed by Streamlit, upload a CSV/XLSX/XLS file, and choose **Run quality checks**. The upload limit is 25 MB and the dashboard run limit is 10,000 rows. Quarantine tables show at most 500 rows; downloads contain the full quarantine set. Report exports materialize the quarantine data and are intended for the demonstration limit.

## Optional AI explanations

The app works without AI credentials. For an OpenAI-compatible endpoint, copy `.env.example` to `.env` and set `LLM_API_KEY`, `LLM_BASE_URL`, and `LLM_MODEL` in the shell before launching Streamlit. The application reads environment variables; it does not automatically load `.env`. Keep actual credentials out of source control. Any AI output is advisory and must be reviewed by a person.

## Tests

```powershell
.\.venv\Scripts\python.exe -m unittest discover -s tests -v
```

The suite covers file ingestion, validation, schema mismatches, metric arithmetic, and report exports. Spark startup can take several seconds on a local machine.

## Docker

```powershell
docker build -t ai-data-quality-monitor .
docker run --rm -p 8501:8501 ai-data-quality-monitor
```

The container installs Java 17 and runs Streamlit on port 8501. Docker build was not verified because the Docker client could not access the local engine. Local Parquet data and uploads are ephemeral unless a volume is mounted.

## Configuration and limitations

- Validation rules and required schema live in `config/validation_rules.yaml`.
- Local folders `data/bronze`, `data/silver`, and `data/quarantine` are overwritten by each CLI run; Streamlit-hosted filesystems may be ephemeral.
- The app targets an internship/public demonstration, not regulated data processing. Do not upload sensitive real customer data to a public deployment.
- The current ingestion boundary uses Pandas for CSV/Excel file decoding before creating a Spark DataFrame. The demo row cap and upload size bound this memory use. Large-scale streaming/distributed ingestion, PostgreSQL, authentication, durable object storage, and production monitoring are future work.
- Performance numbers beyond the observed 10,000-row functional run are intentionally not claimed.

## Deployment status

`packages.txt` declares the Java runtime required by Spark on Community Cloud; Streamlit documents `packages.txt` as the mechanism for apt dependencies on its Debian host ([dependency setup](https://docs.streamlit.io/deploy/streamlit-community-cloud/deploy-your-app/app-dependencies)). To deploy, sign in at [Streamlit Community Cloud](https://share.streamlit.io), create an app from `Suyogssk/Data-Trust`, choose branch `main` and entrypoint `app.py`, then deploy. Choose Python 3.12 in advanced settings. The app runs without secrets; AI credentials are optional. Community Cloud creates the public URL after a successful deployment ([deployment steps](https://docs.streamlit.io/deploy/streamlit-community-cloud/deploy-your-app/deploy)). The app has not yet been created and no public app URL is available.
