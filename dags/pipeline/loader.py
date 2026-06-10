"""
Loader module for Homer ETL pipeline.

Contains two loaders:

1. **Firestore loader** (``load``) — writes deduplicated listings to Firestore
   in batches of 400 (the Firestore batch-write API limit).

2. **BigQuery loader** (``load_bigquery``) — syncs the same cleaned listings
   to a BigQuery table for analytical SQL queries and BI tooling (e.g. Power BI).

Both are **mock implementations** in this portfolio project.  See the
``Production integration`` sections in each function's docstring for the
exact SDK calls needed to activate them in a live environment.
"""

from __future__ import annotations

import logging
import time
from typing import Generator

from pipeline.models import BigQueryLoaderStats, DedupedListing, LoaderStats

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

FIRESTORE_BATCH_LIMIT: int = 400   # Hard limit imposed by Firestore API
MOCK_WRITE_DELAY_S: float = 0.01   # Simulates Firestore network latency per batch

# BigQuery
BQ_DEFAULT_PROJECT: str = "homer-portfolio"  # Override via BQ_PROJECT_ID env var
BQ_DEFAULT_DATASET: str = "homer_analytics"  # Override via BQ_DATASET_ID env var
BQ_DEFAULT_TABLE: str = "listings"           # Override via BQ_TABLE_ID env var
MOCK_BQ_WRITE_DELAY_S: float = 0.02          # Simulates BigQuery streaming latency


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

def _chunk(
    items: list[DedupedListing], size: int
) -> Generator[list[DedupedListing], None, None]:
    """
    Yield successive fixed-size chunks from a list.

    Args:
        items: The list to chunk.
        size:  Maximum number of items per chunk.

    Yields:
        Sub-lists of at most `size` items.
    """
    for i in range(0, len(items), size):
        yield items[i : i + size]


def _write_batch(batch: list[DedupedListing], batch_num: int) -> None:
    """
    Write a single batch to Firestore (mock).

    In production this calls db.batch().commit().

    Args:
        batch:     Listings to write.
        batch_num: 1-based batch index (for logging).

    Raises:
        RuntimeError: If the batch exceeds FIRESTORE_BATCH_LIMIT.
    """
    if len(batch) > FIRESTORE_BATCH_LIMIT:
        raise RuntimeError(
            f"Batch {batch_num} has {len(batch)} items — "
            f"exceeds Firestore limit of {FIRESTORE_BATCH_LIMIT}"
        )

    # --- mock network I/O ---
    time.sleep(MOCK_WRITE_DELAY_S)
    # -------------------------

    logger.info(
        "loader | batch %d committed — %d documents",
        batch_num,
        len(batch),
    )


# ---------------------------------------------------------------------------
# Public entry point
# ---------------------------------------------------------------------------

def load(listings: list[DedupedListing]) -> LoaderStats:
    """
    Load deduplicated listings into Firestore in batches.

    Args:
        listings: Output from the deduplicator step.

    Returns:
        LoaderStats with total documents loaded, batch count, and duration.
    """
    start = time.monotonic()
    total = len(listings)
    logger.info("load | start — %d listings to write", total)

    if not listings:
        logger.warning("load | received empty listings list — nothing to write")
        return LoaderStats(total_loaded=0, batch_count=0, duration_seconds=0.0)

    batch_count = 0
    total_loaded = 0

    for batch_num, batch in enumerate(_chunk(listings, FIRESTORE_BATCH_LIMIT), start=1):
        _write_batch(batch, batch_num)
        total_loaded += len(batch)
        batch_count += 1

    duration = time.monotonic() - start

    logger.info(
        "load | done — %d documents loaded in %d batch(es) over %.2fs",
        total_loaded,
        batch_count,
        duration,
    )
    logger.info(
        "load | summary: total=%d batches=%d avg_per_batch=%.1f duration=%.2fs",
        total_loaded,
        batch_count,
        total_loaded / batch_count if batch_count else 0,
        duration,
    )

    return LoaderStats(
        total_loaded=total_loaded,
        batch_count=batch_count,
        duration_seconds=round(duration, 3),
    )


# ---------------------------------------------------------------------------
# BigQuery loader
# ---------------------------------------------------------------------------

def load_bigquery(listings: list[DedupedListing]) -> BigQueryLoaderStats:
    """
    Sync deduplicated listings to a Google BigQuery table.

    In this portfolio project the function is a **mock** — it simulates the
    latency of a BigQuery streaming-insert job without making any real network
    calls.  All logic (empty-list guard, timing, logging) is production-grade
    and identical to what a live integration would do.

    Production integration
    ----------------------
    1. Install the SDK::

           pip install google-cloud-bigquery

    2. Set credentials::

           export GOOGLE_APPLICATION_CREDENTIALS="/path/to/service-account.json"

    3. Replace the mock block below with::

           from google.cloud import bigquery
           import os

           client = bigquery.Client()
           table_id = (
               f"{os.getenv('BQ_PROJECT_ID', client.project)}"
               f".{os.getenv('BQ_DATASET_ID', BQ_DEFAULT_DATASET)}"
               f".{os.getenv('BQ_TABLE_ID',   BQ_DEFAULT_TABLE)}"
           )
           job_config = bigquery.LoadJobConfig(
               autodetect=True,
               write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
               source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
           )
           load_job = client.load_table_from_json(
               listings, table_id, job_config=job_config
           )
           load_job.result()   # blocks until the job completes

    The function signature and return type are identical in production.

    Args:
        listings: Deduplicated listings from the deduplicator step.

    Returns:
        BigQueryLoaderStats with total rows inserted, target table ID,
        and wall-clock duration.
    """
    import os

    start = time.monotonic()
    total = len(listings)

    project_id = os.getenv("BQ_PROJECT_ID", BQ_DEFAULT_PROJECT)
    dataset_id = os.getenv("BQ_DATASET_ID", BQ_DEFAULT_DATASET)
    table_name = os.getenv("BQ_TABLE_ID",   BQ_DEFAULT_TABLE)
    table_id = f"{project_id}.{dataset_id}.{table_name}"

    logger.info("load_bigquery | start — %d listings → %s", total, table_id)

    if not listings:
        logger.warning("load_bigquery | received empty listings list — nothing to sync")
        return BigQueryLoaderStats(
            total_loaded=0,
            rows_inserted=0,
            table_id=table_id,
            duration_seconds=0.0,
        )

    # --- mock BigQuery streaming insert ---
    time.sleep(MOCK_BQ_WRITE_DELAY_S * max(1, total // 100))
    # --------------------------------------

    duration = time.monotonic() - start

    logger.info(
        "load_bigquery | done — %d rows inserted into %s in %.2fs",
        total,
        table_id,
        duration,
    )

    return BigQueryLoaderStats(
        total_loaded=total,
        rows_inserted=total,
        table_id=table_id,
        duration_seconds=round(duration, 3),
    )
