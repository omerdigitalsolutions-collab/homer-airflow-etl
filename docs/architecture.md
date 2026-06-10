# Homer ETL — Architecture Documentation

## Overview

Homer Daily Sync is a local Apache Airflow DAG that demonstrates production-grade
ETL orchestration skills.  It mirrors the real Homer CRM pipeline (which runs on
Firebase Cloud Functions) using mock data from two simulated scrapers.

---

## Pipeline Stages

```
┌─────────────┐ ──┐
│ scrape_yad2 │   │
└─────────────┘   ├──► normalize ──► validate ──► deduplicate ──┬──► load_firestore
┌──────────────┐  │                                              └──► load_bigquery
│ scrape_fb    │ ──┘
└──────────────┘
```

### Stage 1 — Scraping (parallel)

| Task | Source | Listings | Notes |
|------|--------|----------|-------|
| `scrape_yad2` | Yad2 | 280–320 | 5 % outlier prices injected |
| `scrape_facebook` | Facebook Marketplace | 180–220 + 15 % dups | Duplicate injection for testing |

Both tasks run in **parallel** (fan-out).  Airflow executes them concurrently
because neither depends on the other.  The downstream `normalize` task receives
both XCom outputs as arguments (fan-in).

In production these tasks call **Apify Actors** via the Apify Python client.

---

### Stage 2 — Normalization

**Module:** `pipeline/normalizer.py`

Cleans raw data so that all downstream logic can assume consistent types:

| Field | Raw | Normalised |
|-------|-----|------------|
| `price` | `"₪2,500,000"` | `2500000.0` |
| `rooms` | `"3,5"` | `3.5` |
| `description` | `"דירה  יפה  "` | `"דירה יפה"` |
| `city` | `None` | `""` |

The normalizer never raises — invalid values are coerced to safe defaults
(`0.0` for numbers, `""` for strings) and logged at `WARNING`.

---

### Stage 3 — Validation

**Module:** `pipeline/validator.py`

Two sequential passes:

#### 3a. Spam Filter

Rejects listings that:
- Have an empty `city` field
- Have `price == 0` or `price < 100`
- Contain keywords: `test`, `בדיקה`, `lorem ipsum`, `dummy`, `fake`, `placeholder`

#### 3b. Outlier Detection — ±3σ Algorithm

Groups listings by `deal_type` (sales and rentals have very different price scales)
and computes per-group statistics:

```
μ  = mean(prices)
σ  = population_std_dev(prices)

A listing is an OUTLIER if:
    price < μ − 3σ   OR   price > μ + 3σ
```

**Why 3σ?** Under a normal distribution, 99.73 % of values fall within ±3σ.
Values outside this range are statistically extreme and almost certainly data
errors (a missed decimal point, a zero appended by mistake, etc.).

**Why per deal_type?** Sale prices (₪800K–₪6M) and rental prices (₪3K–₪12K)
differ by three orders of magnitude.  Mixing them into one distribution would
make the σ so large that no real outlier would be detected.

---

### Stage 4 — Deduplication

**Module:** `pipeline/deduplicator.py`

#### Fingerprinting Algorithm

```python
fingerprint = MD5(
    city.lower()                                + "|" +
    street.lower()                              + "|" +
    str(rooms)                                  + "|" +
    str(round(price / 100_000) * 100_000)       + "|" +   # rounded to 100K
    deal_type
)
```

**Why MD5?** MD5 is fast, produces a compact 32-character hex string, and
collision probability is negligible for a few thousand listings per day.
This is not a security context, so MD5's cryptographic weaknesses are irrelevant.

**Why round price to 100K?** The same property frequently appears on multiple
platforms at slightly different asking prices (e.g. ₪1,950,000 vs ₪2,050,000).
Rounding to the nearest ₪100,000 absorbs these small variations so the same
property is correctly identified as a duplicate.

**Why not include `neighborhood` or `size_sqm`?** These fields are less reliable:
neighbourhood names vary between sources (e.g. "נווה צדק" vs "נווה-צדק"), and
`size_sqm` is sometimes rounded differently.  The chosen fields (`city`, `street`,
`rooms`, `price bucket`, `deal_type`) provide a stable identity with low false-positive risk.

---

### Stage 5a — Firestore Loader

**Module:** `pipeline/loader.py` → `load()`

Writes deduplicated listings to Firestore in batches of **400**.

**Why 400?** The Firestore batch-write API enforces a hard limit of 500 operations
per `batch.commit()` call.  Using 400 leaves a 100-operation safety buffer for
any metadata writes (e.g. audit log entries) added in future iterations.

**Why not insert one document at a time?** Each Firestore write incurs a
round-trip latency of ~50–100 ms.  Writing 500 documents one by one would take
~25–50 seconds; batching them takes ~1–2 seconds.

---

### Stage 5b — BigQuery Loader (Fan-out, parallel with Firestore)

**Module:** `pipeline/loader.py` → `load_bigquery()`

Runs **concurrently** with the Firestore loader after `deduplicate`.
Both tasks receive the same XCom payload from `deduplicate` — neither waits
for the other (Airflow fan-out pattern).

**Purpose:** Export the cleaned, deduplicated listings to
`homer_analytics.listings` in Google BigQuery so that analysts can run
arbitrary SQL (Window Functions, CTEs, GROUP BY) and connect BI tools
(e.g. Power BI, Looker Studio).

**Mock vs production:**

| | Mock (portfolio) | Production |
|--|---|---|
| Implementation | `time.sleep()` simulates latency | `google.cloud.bigquery.Client.load_table_from_json()` |
| Credentials | none required | `GOOGLE_APPLICATION_CREDENTIALS` env var |
| `table_id` | `homer-portfolio.homer_analytics.listings` (env-configurable) | your GCP project |
| Write mode | — | `WRITE_APPEND` (new rows daily) |

**Configuring for production** (env vars):

```bash
export GOOGLE_APPLICATION_CREDENTIALS="/path/to/service-account.json"
export BQ_PROJECT_ID="your-gcp-project"
export BQ_DATASET_ID="homer_analytics"   # default
export BQ_TABLE_ID="listings"            # default
```

Then uncomment `google-cloud-bigquery>=3.11.0` in `requirements.txt` and
replace the mock block in `load_bigquery()` with the SDK snippet in its docstring.

---

## XCom Data Flow

Airflow's XCom mechanism serialises task return values to the metadata database
and makes them available to downstream tasks.  Every task in this DAG returns
a plain `dict` (JSON-serialisable) containing:

```
scrape_*       →  { source, count, listings: [...] }
normalize      →  { input_count, output_count, listings: [...] }
validate       →  { input_count, valid_count, rejected_count, listings: [...], rejected: [...] }
deduplicate    →  { input_count, unique_count, duplicate_count, listings: [...] }
load_firestore →  { total_loaded, batch_count, duration_seconds }
load_bigquery  →  { total_loaded, rows_inserted, table_id, duration_seconds }
```

After `deduplicate`, both loader tasks read the **same XCom value** independently
(fan-out).  Airflow passes the serialised dict to each loader without copying
it — both tasks pull from the same XCom key.

---

## Design Decisions

### Why Airflow instead of a cron job?

| Concern | cron | Airflow |
|---------|------|---------|
| Retry logic | manual | built-in (`retries`, `retry_delay`) |
| Observability | grep logs | web UI with task-level status |
| Dependency management | none | declarative task graph |
| Backfilling | complex | `catchup=True` flag |
| Parallelism | complex | fan-out with multiple tasks |

### Why `@task` decorator (TaskFlow API) instead of `PythonOperator`?

The TaskFlow API (`@task`) was introduced in Airflow 2.0.  It:
- Eliminates XCom boilerplate (`xcom_push`/`xcom_pull`) — return values are
  automatically pushed and injected as arguments.
- Makes data flow explicit in the function signatures.
- Reduces DAG file length by ~40 % compared to operator-style DAGs.

---

## Running Locally

```bash
python -m venv .venv
source .venv/bin/activate
export AIRFLOW_HOME=$(pwd)/airflow_home
pip install -r requirements.txt
airflow db migrate
airflow users create \
  --username admin --password admin \
  --firstname Omer --lastname Assis \
  --role Admin --email omerasis4@gmail.com
airflow standalone
```

Navigate to `http://localhost:8080`, enable the `homer_daily_sync` DAG, and
trigger a manual run.

---

## Production Notes

This project uses **mock data** to demonstrate orchestration patterns.
The real Homer pipeline runs on **Firebase Cloud Functions** triggered by
Firestore events.  To adapt this DAG for production:

1. Replace `scrape_yad2()` / `scrape_facebook()` with Apify Actor calls.
2. Replace the mock `_write_batch()` in `loader.py` with `db.batch().commit()`
   (Firestore `firebase_admin` SDK).
3. Uncomment `google-cloud-bigquery` in `requirements.txt` and replace the
   mock block in `load_bigquery()` with the `google.cloud.bigquery` SDK calls
   (full snippet in the function docstring).
4. Set `AIRFLOW_HOME` to a persistent location.
5. Store service-account keys in Airflow Connections / Secret Manager— never in code.
