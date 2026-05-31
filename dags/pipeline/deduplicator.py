"""
Deduplicator for Homer ETL pipeline.

Uses an MD5 fingerprint built from stable listing attributes to identify
duplicate property listings that arrive from multiple sources or scraping runs.

Fingerprint formula
-------------------
MD5(
    city.lower()                         + "|" +
    street.lower()                       + "|" +
    str(rooms)                           + "|" +
    str(round(price / 100_000) * 100_000) + "|" +   # rounded to nearest 100K
    deal_type
)

The price is rounded to the nearest 100,000 ILS so that the same property
advertised at slightly different prices (e.g. 1,950,000 vs 2,050,000) is
correctly deduplicated.  Sellers frequently adjust asking prices by small
amounts across platforms.

The first occurrence of each fingerprint is kept; subsequent occurrences
are discarded and counted.
"""

from __future__ import annotations

import hashlib
import logging
import time

from pipeline.models import DedupedListing, DeduplicatorStats, NormalizedListing

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

PRICE_BUCKET_SIZE: int = 100_000   # round prices to nearest 100K ILS


# ---------------------------------------------------------------------------
# Fingerprint computation
# ---------------------------------------------------------------------------

def compute_fingerprint(listing: NormalizedListing) -> str:
    """
    Compute an MD5 fingerprint for a normalised listing.

    The fingerprint is deterministic: two listings that represent the same
    real-world property will produce an identical fingerprint regardless of
    when or from which source they were scraped.

    Args:
        listing: A NormalizedListing dict.

    Returns:
        A 32-character lowercase hex MD5 digest string.
    """
    rounded_price = round(listing["price"] / PRICE_BUCKET_SIZE) * PRICE_BUCKET_SIZE

    components = "|".join([
        listing["city"].lower(),
        listing["street"].lower(),
        str(listing["rooms"]),
        str(int(rounded_price)),
        listing["deal_type"],
    ])
    return hashlib.md5(components.encode("utf-8")).hexdigest()


# ---------------------------------------------------------------------------
# Public entry point
# ---------------------------------------------------------------------------

def deduplicate(listings: list[NormalizedListing]) -> DeduplicatorStats:
    """
    Remove duplicate listings using MD5 fingerprinting.

    Iterates over listings in order; the first occurrence of each fingerprint
    is kept, all later occurrences are counted as duplicates.

    Args:
        listings: Validated listings from the validator step.

    Returns:
        DeduplicatorStats with unique listings (each carrying its fingerprint),
        counts of unique vs duplicate, and total input count.
    """
    start = time.monotonic()
    input_count = len(listings)
    logger.info("deduplicate | start — %d input listings", input_count)

    seen: set[str] = set()
    unique: list[DedupedListing] = []
    duplicate_count: int = 0

    for listing in listings:
        fp = compute_fingerprint(listing)

        if fp in seen:
            duplicate_count += 1
            logger.debug(
                "deduplicate | duplicate found — id=%s fingerprint=%s",
                listing["id"],
                fp,
            )
            continue

        seen.add(fp)
        # Build a DedupedListing by merging the fingerprint into the listing
        deduped = DedupedListing(**listing, fingerprint=fp)
        unique.append(deduped)

    duration = time.monotonic() - start
    logger.info(
        "deduplicate | done — %d unique, %d duplicates removed in %.2fs",
        len(unique),
        duplicate_count,
        duration,
    )

    return DeduplicatorStats(
        input_count=input_count,
        unique_count=len(unique),
        duplicate_count=duplicate_count,
        listings=unique,
    )
