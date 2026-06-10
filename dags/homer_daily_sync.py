"""
Homer Daily Sync — Airflow DAG

Orchestrates a daily ETL pipeline that:
1. Scrapes property listings from Yad2 and Facebook Marketplace in parallel
2. Normalises and validates the combined data
3. Deduplicates listings using MD5 fingerprinting
4. Loads unique listings into Firestore

Schedule: 23:00 UTC daily
"""

from __future__ import annotations

import logging
from datetime import datetime, timedelta

from airflow.decorators import dag, task
from airflow.models import TaskInstance

from pipeline.deduplicator import deduplicate
from pipeline.loader import load, load_bigquery
from pipeline.models import (
    BigQueryLoaderStats,
    DedupedListing,
    NormalizedListing,
    RawListing,
    ScraperStats,
)
from pipeline.normalizer import normalize
from pipeline.scrapers import scrape_facebook, scrape_yad2
from pipeline.validator import validate

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# DAG-level defaults
# ---------------------------------------------------------------------------

DEFAULT_ARGS: dict = {
    "owner": "omer-assis",
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
    "execution_timeout": timedelta(hours=2),
    "email_on_failure": False,
    "email_on_retry": False,
}

DAG_DOC: str = """
## Homer Daily Sync

**Purpose:** Ingest property listings from Yad2 and Facebook Marketplace,
clean and deduplicate them, then fan-out to two sinks every night at 23:00 UTC:

- **Firestore** — operational CRM database
- **BigQuery** — analytical data warehouse (homer_analytics.listings)

### Pipeline Architecture
```
┌─────────────┐ ──┐
│ scrape_yad2 │   │
└─────────────┘   ├──► normalize ──► validate ──► deduplicate ─┬─► load_firestore
┌──────────────┐  │                                            └─► load_bigquery
│ scrape_fb    │ ─┘
└──────────────┘
```

### Stages
| Step | Description |
|------|-------------|
| scrape_yad2 | Calls Apify Yad2 Actor (mock in portfolio) |
| scrape_facebook | Calls Apify Facebook Actor with 15 % duplicate injection |
| normalize | Cleans prices, rooms notation, Hebrew text |
| validate | Spam filter + ±3σ outlier detection per deal_type |
| deduplicate | MD5 fingerprint deduplication (price rounded to 100K) |
| load_firestore | Batch writes to Firestore (400 docs/batch) |
| load_bigquery | Streams rows to BigQuery (homer_analytics.listings) |

### Data Flow (XCom)
Each task pushes its result dict to XCom under the key `return_value`.
The next task pulls and deserialises before processing.
After `deduplicate`, both loader tasks receive the same XCom value (fan-out).

### Links
- [Apify Yad2 Actor](https://apify.com/actors)
- [Firestore Batch Writes](https://firebase.google.com/docs/firestore/manage-data/transactions)
- [BigQuery Load Jobs](https://cloud.google.com/bigquery/docs/loading-data)
"""


# ---------------------------------------------------------------------------
# DAG definition
# ---------------------------------------------------------------------------

@dag(
    dag_id="homer_daily_sync",
    description="Daily ETL: Yad2 + Facebook → Firestore + BigQuery",
    schedule="0 23 * * *",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["homer", "etl", "portfolio", "bigquery"],
    default_args=DEFAULT_ARGS,
    doc_md=DAG_DOC,
)
def homer_daily_sync() -> None:

    # -----------------------------------------------------------------------
    # Task 1a: Scrape Yad2
    # -----------------------------------------------------------------------
    @task(task_id="scrape_yad2")
    def task_scrape_yad2() -> dict:
        """
        Scrape property listings from Yad2.

        Returns:
            ScraperStats dict (serialisable for XCom).
        """
        stats = scrape_yad2()
        logger.info("task_scrape_yad2 | pushed %d listings to XCom", stats["count"])
        return dict(stats)

    # -----------------------------------------------------------------------
    # Task 1b: Scrape Facebook (parallel with Yad2)
    # -----------------------------------------------------------------------
    @task(task_id="scrape_facebook")
    def task_scrape_facebook() -> dict:
        """
        Scrape property listings from Facebook Marketplace.

        Returns:
            ScraperStats dict (serialisable for XCom).
        """
        stats = scrape_facebook()
        logger.info("task_scrape_facebook | pushed %d listings to XCom", stats["count"])
        return dict(stats)

    # -----------------------------------------------------------------------
    # Task 2: Normalize
    # -----------------------------------------------------------------------
    @task(task_id="normalize")
    def task_normalize(yad2_result: dict, fb_result: dict) -> dict:
        """
        Merge scraper outputs and normalise all listings.

        Args:
            yad2_result: XCom value from task_scrape_yad2.
            fb_result:   XCom value from task_scrape_facebook.

        Returns:
            NormalizerStats dict.
        """
        yad2_listings: list[RawListing] = yad2_result["listings"]
        fb_listings: list[RawListing] = fb_result["listings"]
        combined: list[RawListing] = yad2_listings + fb_listings

        logger.info(
            "task_normalize | merging yad2=%d + fb=%d = %d total",
            len(yad2_listings),
            len(fb_listings),
            len(combined),
        )

        stats = normalize(combined)
        return dict(stats)

    # -----------------------------------------------------------------------
    # Task 3: Validate
    # -----------------------------------------------------------------------
    @task(task_id="validate")
    def task_validate(normalize_result: dict) -> dict:
        """
        Validate normalised listings (spam + outlier detection).

        Args:
            normalize_result: XCom value from task_normalize.

        Returns:
            ValidatorStats dict.
        """
        listings: list[NormalizedListing] = normalize_result["listings"]
        stats = validate(listings)
        logger.info(
            "task_validate | valid=%d rejected=%d",
            stats["valid_count"],
            stats["rejected_count"],
        )
        return dict(stats)

    # -----------------------------------------------------------------------
    # Task 4: Deduplicate
    # -----------------------------------------------------------------------
    @task(task_id="deduplicate")
    def task_deduplicate(validate_result: dict) -> dict:
        """
        Remove duplicate listings via MD5 fingerprinting.

        Args:
            validate_result: XCom value from task_validate.

        Returns:
            DeduplicatorStats dict.
        """
        listings: list[NormalizedListing] = validate_result["listings"]
        stats = deduplicate(listings)
        logger.info(
            "task_deduplicate | unique=%d duplicates_removed=%d",
            stats["unique_count"],
            stats["duplicate_count"],
        )
        return dict(stats)

    # -----------------------------------------------------------------------
    # Task 5a: Load → Firestore
    # -----------------------------------------------------------------------
    @task(task_id="load_firestore")
    def task_load(dedup_result: dict) -> dict:
        """
        Load unique listings into Firestore.

        Args:
            dedup_result: XCom value from task_deduplicate.

        Returns:
            LoaderStats dict.
        """
        listings: list[DedupedListing] = dedup_result["listings"]
        stats = load(listings)
        logger.info(
            "task_load_firestore | loaded=%d batches=%d duration=%.2fs",
            stats["total_loaded"],
            stats["batch_count"],
            stats["duration_seconds"],
        )
        return dict(stats)

    # -----------------------------------------------------------------------
    # Task 5b: Load → BigQuery  (parallel with Firestore — fan-out)
    # -----------------------------------------------------------------------
    @task(task_id="load_bigquery")
    def task_load_bigquery(dedup_result: dict) -> dict:
        """
        Sync unique listings to Google BigQuery for analytical queries.

        Runs in parallel with ``load_firestore`` after the deduplicate step
        (fan-out pattern).  Both tasks receive the same XCom payload so neither
        waits for the other.

        In this portfolio project the BigQuery call is mocked.  To activate
        production mode set the following environment variables before starting
        the Airflow scheduler::

            export GOOGLE_APPLICATION_CREDENTIALS="/path/to/service-account.json"
            export BQ_PROJECT_ID="your-gcp-project"
            export BQ_DATASET_ID="homer_analytics"
            export BQ_TABLE_ID="listings"

        Args:
            dedup_result: XCom value from task_deduplicate.

        Returns:
            BigQueryLoaderStats dict.
        """
        listings: list[DedupedListing] = dedup_result["listings"]
        stats: BigQueryLoaderStats = load_bigquery(listings)
        logger.info(
            "task_load_bigquery | rows=%d table=%s duration=%.2fs",
            stats["rows_inserted"],
            stats["table_id"],
            stats["duration_seconds"],
        )
        return dict(stats)

    # -----------------------------------------------------------------------
    # Wire up dependencies
    # [scrape_yad2, scrape_facebook] >> normalize >> validate >> deduplicate
    #                                                                  ├──► load_firestore
    #                                                                  └──► load_bigquery
    # -----------------------------------------------------------------------
    yad2_result = task_scrape_yad2()
    fb_result = task_scrape_facebook()
    normalize_result = task_normalize(yad2_result, fb_result)
    validate_result = task_validate(normalize_result)
    dedup_result = task_deduplicate(validate_result)
    task_load(dedup_result)
    task_load_bigquery(dedup_result)


# Instantiate the DAG
homer_daily_sync()
