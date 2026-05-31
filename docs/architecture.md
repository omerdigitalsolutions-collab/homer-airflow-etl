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
└─────────────┘   ├──► normalize ──► validate ──► deduplicate ──► load
┌──────────────┐  │
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

### Stage 5 — Loading

**Module:** `pipeline/loader.py`

Writes deduplicated listings to Firestore in batches of **400**.

**Why 400?** The Firestore batch-write API enforces a hard limit of 500 operations
per `batch.commit()` call.  Using 400 leaves a 100-operation safety buffer for
any metadata writes (e.g. audit log entries) added in future iterations.

**Why not insert one document at a time?** Each Firestore write incurs a
round-trip latency of ~50–100 ms.  Writing 500 documents one by one would take
~25–50 seconds; batching them takes ~1–2 seconds.

---

## XCom Data Flow

Airflow's XCom mechanism serialises task return values to the metadata database
and makes them available to downstream tasks.  Every task in this DAG returns
a plain `dict` (JSON-serialisable) containing:

```
scrape_*   →  { source, count, listings: [...] }
normalize  →  { input_count, output_count, listings: [...] }
validate   →  { input_count, valid_count, rejected_count, listings: [...], rejected: [...] }
deduplicate→  { input_count, unique_count, duplicate_count, listings: [...] }
load       →  { total_loaded, batch_count, duration_seconds }
```

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
2. Replace the mock `_write_batch()` in `loader.py` with `db.batch().commit()`.
3. Set `AIRFLOW_HOME` to a persistent location.
4. Store the Firebase service account key in Airflow Connections (not in code).
