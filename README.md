# Homer Airflow ETL

![Python](https://img.shields.io/badge/Python-3.11-blue?logo=python)
![Airflow](https://img.shields.io/badge/Airflow-2.10.3-017CEE?logo=apache-airflow)
![BigQuery](https://img.shields.io/badge/BigQuery-mock-4285F4?logo=google-cloud)
![License](https://img.shields.io/badge/License-MIT-green)
![Tests](https://img.shields.io/badge/Tests-pytest-informational)

A portfolio project demonstrating production-grade **Apache Airflow** ETL
orchestration skills.  The pipeline ingests Israeli real-estate listings from
two mock sources (Yad2 and Facebook Marketplace), cleans and validates the
data, removes duplicates, and fan-outs to two sinks — a mock **Firestore**
(operational CRM) and a mock **BigQuery** table (analytical data warehouse)
— every night at 23:00 UTC.

---

## Architecture

```
┌─────────────┐ ──┐
│ scrape_yad2 │   │
└─────────────┘   ├──► normalize ──► validate ──► deduplicate ──┬──► load_firestore
┌──────────────┐  │                                              └──► load_bigquery
│ scrape_fb    │ ──┘
└──────────────┘

7 tasks | parallel fan-in → sequential processing → parallel fan-out
```

---

## Tech Stack

- **Apache Airflow 2.10** — DAG orchestration, scheduling, retries, XCom
- **Python 3.11** — type hints, dataclasses, `logging`, `hashlib`
- **Google BigQuery** — analytical data warehouse sink (mock; SDK-ready)
- **Google Firestore** — operational CRM sink (mock; SDK-ready)
- **pytest** — unit tests for all pipeline modules
- **Black** — code formatting

---

## Features

- **Parallel scraping** — Yad2 and Facebook tasks run concurrently
- **Data normalization** — cleans prices (strips ₪/,), Hebrew text, room notation (`3,5` → `3.5`)
- **±3σ outlier detection** — rejects price anomalies per deal type (sales vs rentals)
- **Spam filtering** — drops listings with missing cities, zero prices, or test keywords
- **MD5 fingerprinting** — deduplicates across sources with price rounding to ±₪100K
- **Firestore batch writes** — 400 docs/batch (Firestore API limit) for the operational CRM
- **BigQuery sync** — parallel fan-out to `homer_analytics.listings` for SQL analytics & BI
- **XCom data flow** — typed dicts passed between every task
- **Retry logic** — 3 retries with 5-minute delay on any task failure

---

## Project Structure

```
homer-airflow-etl/
├── dags/
│   ├── homer_daily_sync.py        # DAG definition (TaskFlow API)
│   └── pipeline/
│       ├── __init__.py
│       ├── models.py              # TypedDict schemas (incl. BigQueryLoaderStats)
│       ├── scrapers.py            # Mock Yad2 + Facebook scrapers
│       ├── normalizer.py          # Price / rooms / Hebrew text normalisation
│       ├── validator.py           # Spam filter + ±3σ outlier detection
│       ├── deduplicator.py        # MD5 fingerprint deduplication
│       └── loader.py              # Mock Firestore + BigQuery loaders
├── tests/
│   ├── conftest.py                # sys.path setup
│   ├── test_normalizer.py
│   ├── test_validator.py
│   ├── test_deduplicator.py
│   └── test_bigquery_loader.py    # BigQuery loader unit tests
├── docs/
│   └── architecture.md            # Deep-dive technical documentation
├── .gitignore
├── .python-version                # 3.11
├── requirements.txt
├── pyproject.toml                 # black + pytest config
└── README.md
```

---

## Setup

```bash
# 1. Clone
git clone https://github.com/omerdigitalsolutions-collab/homer-airflow-etl.git
cd homer-airflow-etl

# 2. Create virtual environment
python -m venv .venv
source .venv/bin/activate          # Windows: .venv\Scripts\activate

# 3. Set Airflow home (keeps DB and logs inside the project)
export AIRFLOW_HOME=$(pwd)/airflow_home

# 4. Install dependencies
pip install -r requirements.txt

# 5. Initialise the database
airflow db migrate

# 6. Create admin user
airflow users create \
  --username admin \
  --password admin \
  --firstname Omer \
  --lastname Assis \
  --role Admin \
  --email omerasis4@gmail.com
```

---

## Running the DAG

```bash
# Start all Airflow services (webserver + scheduler)
airflow standalone

# In another terminal — verify the DAG is visible
airflow dags list

# Trigger a manual run
airflow dags trigger homer_daily_sync

# Open the UI
open http://localhost:8080
```

The DAG graph and task logs are visible at `http://localhost:8080`.

![Homer Daily Sync DAG graph — all six tasks succeeded](docs/graph.png)

---

## Testing

```bash
# Run all tests with verbose output
pytest tests/ -v

# Run with coverage report
pytest tests/ -v --cov=dags/pipeline --cov-report=term-missing
```

Expected output:

```
tests/test_normalizer.py::TestNormalizePrice::test_plain_integer_string  PASSED
tests/test_normalizer.py::TestNormalizeRooms::test_comma_decimal         PASSED
...
15 passed in 0.XX seconds
```

---

## Production Note

This project uses **mock data** to demonstrate Airflow orchestration patterns.
The real **Homer CRM** pipeline runs on Firebase Cloud Functions.

**Activating Firestore:** replace the mock `_write_batch()` in `loader.py`
with `firebase_admin` SDK calls (see the docstring in that module).

**Activating BigQuery:** uncomment `google-cloud-bigquery` in `requirements.txt`,
set `GOOGLE_APPLICATION_CREDENTIALS`, and replace the mock block in
`load_bigquery()` with the `google.cloud.bigquery` SDK calls (full snippet
in the function's docstring).
