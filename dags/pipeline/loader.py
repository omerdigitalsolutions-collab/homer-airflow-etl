"""
Mock Firestore loader for Homer ETL pipeline.

Loads deduplicated listings into Firestore in batches of 400 (the real
Firestore batch-write limit).

Production integration
----------------------
Replace the mock implementation with the firebase_admin SDK::

    import firebase_admin
    from firebase_admin import credentials, firestore

    cred = credentials.Certificate("serviceAccountKey.json")
    firebase_admin.initialize_app(cred)
    db = firestore.client()

    batch = db.batch()
    for listing in chunk:
        ref = db.collection("listings").document(listing["id"])
        batch.set(ref, listing)
    batch.commit()

The function signature and return type stay identical in production.
"""

from __future__ import annotations

import logging
import time
from typing import Generator

from pipeline.models import DedupedListing, LoaderStats

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

FIRESTORE_BATCH_LIMIT: int = 400   # Hard limit imposed by Firestore API
MOCK_WRITE_DELAY_S: float = 0.01   # Simulates network latency per batch


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
