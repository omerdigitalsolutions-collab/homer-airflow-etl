"""
Validator for Homer ETL pipeline.

Two validation passes:
1. Spam filter  — rejects listings missing mandatory fields or containing
                  test/spam keywords.
2. Outlier detection — rejects listings whose price deviates more than ±3σ
                       from the per-deal-type mean.

Returns a (valid, rejected) tuple so callers can log rejection counts
without losing traceability.
"""

from __future__ import annotations

import logging
import math
import time

from pipeline.models import NormalizedListing, RejectedListing, ValidatorStats

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

SPAM_KEYWORDS: frozenset[str] = frozenset([
    "test", "בדיקה", "lorem ipsum", "dummy", "fake", "placeholder",
])

OUTLIER_SIGMA: float = 3.0   # reject listings outside mean ± N×σ
MIN_PRICE: float = 100.0     # absolute floor — anything below is nonsensical


# ---------------------------------------------------------------------------
# Spam filter
# ---------------------------------------------------------------------------

def _is_spam(listing: NormalizedListing) -> str | None:
    """
    Check a listing for spam signals.

    Args:
        listing: A normalised listing dict.

    Returns:
        A human-readable rejection reason string, or None if the listing is clean.
    """
    if not listing.get("city"):
        return "missing city"
    if not listing.get("price") or listing["price"] < MIN_PRICE:
        return f"price too low ({listing.get('price')})"

    description_lower = (listing.get("description") or "").lower()
    for keyword in SPAM_KEYWORDS:
        if keyword in description_lower:
            return f"spam keyword detected: '{keyword}'"

    return None


# ---------------------------------------------------------------------------
# Outlier detection  (±3σ per deal_type)
# ---------------------------------------------------------------------------

def _compute_stats(prices: list[float]) -> tuple[float, float]:
    """
    Compute mean and population standard deviation.

    Args:
        prices: Non-empty list of price values.

    Returns:
        Tuple of (mean, std_dev).  std_dev is 0.0 for single-element lists.
    """
    n = len(prices)
    mean = sum(prices) / n
    if n < 2:
        return mean, 0.0
    variance = sum((p - mean) ** 2 for p in prices) / n
    return mean, math.sqrt(variance)


def _detect_outliers(
    listings: list[NormalizedListing],
) -> tuple[list[NormalizedListing], list[RejectedListing]]:
    """
    Detect and remove price outliers using the ±3σ algorithm.

    Groups listings by deal_type so rental and sale prices are compared
    against their respective distributions (they differ by orders of magnitude).

    Args:
        listings: Listings that passed the spam filter.

    Returns:
        Tuple of (clean_listings, rejected_listings).
    """
    # Group prices by deal_type
    by_deal: dict[str, list[float]] = {}
    for lst in listings:
        by_deal.setdefault(lst["deal_type"], []).append(lst["price"])

    # Compute per-deal-type statistics
    stats: dict[str, tuple[float, float]] = {
        deal: _compute_stats(prices) for deal, prices in by_deal.items()
    }

    for deal, (mu, sigma) in stats.items():
        logger.info(
            "outlier_detection | deal_type=%s μ=%.0f σ=%.0f threshold=±%.0f",
            deal, mu, sigma, OUTLIER_SIGMA * sigma,
        )

    clean: list[NormalizedListing] = []
    rejected: list[RejectedListing] = []

    for listing in listings:
        mu, sigma = stats[listing["deal_type"]]
        price = listing["price"]

        if sigma > 0 and abs(price - mu) > OUTLIER_SIGMA * sigma:
            reason = (
                f"price outlier: {price:.0f} is outside "
                f"μ±{OUTLIER_SIGMA}σ = [{mu - OUTLIER_SIGMA * sigma:.0f}, "
                f"{mu + OUTLIER_SIGMA * sigma:.0f}]"
            )
            rejected.append(RejectedListing(listing=listing, reason=reason))
        else:
            clean.append(listing)

    return clean, rejected


# ---------------------------------------------------------------------------
# Public entry point
# ---------------------------------------------------------------------------

def validate(
    listings: list[NormalizedListing],
) -> ValidatorStats:
    """
    Validate a list of normalised listings.

    Runs the spam filter first, then outlier detection on the survivors.

    Args:
        listings: Output from the normalizer step.

    Returns:
        ValidatorStats containing valid listings, rejected listings, and counts.
    """
    start = time.monotonic()
    input_count = len(listings)
    logger.info("validate | start — %d input listings", input_count)

    # --- Pass 1: spam filter ---
    spam_clean: list[NormalizedListing] = []
    rejected: list[RejectedListing] = []

    for listing in listings:
        reason = _is_spam(listing)
        if reason:
            rejected.append(RejectedListing(listing=listing, reason=reason))
        else:
            spam_clean.append(listing)

    logger.info(
        "validate | spam filter — %d passed, %d rejected",
        len(spam_clean),
        len(rejected),
    )

    # --- Pass 2: outlier detection ---
    if spam_clean:
        outlier_clean, outlier_rejected = _detect_outliers(spam_clean)
        rejected.extend(outlier_rejected)
    else:
        outlier_clean = []

    duration = time.monotonic() - start
    logger.info(
        "validate | done — %d valid, %d rejected in %.2fs",
        len(outlier_clean),
        len(rejected),
        duration,
    )

    return ValidatorStats(
        input_count=input_count,
        valid_count=len(outlier_clean),
        rejected_count=len(rejected),
        listings=outlier_clean,
        rejected=rejected,
    )
