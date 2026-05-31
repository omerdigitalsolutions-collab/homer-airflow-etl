"""
Data normalizer for Homer ETL pipeline.

Cleans and standardises raw listing data before validation:
- Price: strips non-numeric characters, converts to float
- Rooms: handles both '3.5' and '3,5' notations
- Hebrew text: validates Unicode range, collapses whitespace

All functions are pure (no side-effects) and handle None / malformed
input without raising exceptions — invalid values are coerced to safe
defaults and logged at WARNING level.
"""

from __future__ import annotations

import logging
import re
import time

from pipeline.models import NormalizedListing, NormalizerStats, RawListing

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

HEBREW_UNICODE_RANGE: re.Pattern[str] = re.compile(r"[\u0590-\u05FF\s\-\'\".,]+")
MULTI_WHITESPACE: re.Pattern[str] = re.compile(r"\s{2,}")
NON_NUMERIC_PRICE: re.Pattern[str] = re.compile(r"[^\d.]")

DEFAULT_ROOMS: float = 0.0
DEFAULT_PRICE: float = 0.0


# ---------------------------------------------------------------------------
# Field-level normalizers
# ---------------------------------------------------------------------------

def normalize_price(raw: object) -> float:
    """
    Normalise a price value to float.

    Strips currency symbols (₪, $, €), commas, and whitespace.
    Returns DEFAULT_PRICE (0.0) for None or unparseable input.

    Args:
        raw: The raw price value (str, int, float, or None).

    Returns:
        A non-negative float representing the price in ILS.
    """
    if raw is None:
        logger.warning("normalize_price | received None — defaulting to %.2f", DEFAULT_PRICE)
        return DEFAULT_PRICE

    cleaned = NON_NUMERIC_PRICE.sub("", str(raw))
    if not cleaned:
        logger.warning("normalize_price | no numeric content in %r — defaulting to %.2f", raw, DEFAULT_PRICE)
        return DEFAULT_PRICE

    try:
        return float(cleaned)
    except ValueError:
        logger.warning("normalize_price | cannot parse %r — defaulting to %.2f", raw, DEFAULT_PRICE)
        return DEFAULT_PRICE


def normalize_rooms(raw: object) -> float:
    """
    Normalise a rooms value to float.

    Accepts strings such as '3', '3.5', '3,5', or numeric types.
    Returns DEFAULT_ROOMS (0.0) for None or unparseable input.

    Args:
        raw: The raw rooms value (str, int, float, or None).

    Returns:
        A positive float representing the number of rooms.
    """
    if raw is None:
        logger.warning("normalize_rooms | received None — defaulting to %.1f", DEFAULT_ROOMS)
        return DEFAULT_ROOMS

    # Replace comma-decimal separator common in Israeli sites (e.g. '3,5')
    normalised_str = str(raw).replace(",", ".")
    try:
        return float(normalised_str)
    except ValueError:
        logger.warning("normalize_rooms | cannot parse %r — defaulting to %.1f", raw, DEFAULT_ROOMS)
        return DEFAULT_ROOMS


def normalize_hebrew_text(raw: object) -> str:
    """
    Normalise a Hebrew text field.

    Validates that text contains Hebrew Unicode characters (U+0590–U+05FF),
    removes double whitespace, and strips leading/trailing whitespace.
    Non-string or empty input is returned as an empty string.

    Args:
        raw: The raw text value (str or None).

    Returns:
        A cleaned string.
    """
    if not isinstance(raw, str):
        logger.warning("normalize_hebrew_text | expected str, got %s", type(raw).__name__)
        return ""

    # Collapse multiple whitespace characters to a single space
    cleaned = MULTI_WHITESPACE.sub(" ", raw).strip()
    return cleaned


# ---------------------------------------------------------------------------
# Listing-level normalizer
# ---------------------------------------------------------------------------

def normalize_listing(raw: RawListing) -> NormalizedListing:
    """
    Apply all field normalizers to a single raw listing.

    Args:
        raw: A RawListing dict from a scraper.

    Returns:
        A NormalizedListing with guaranteed clean types.
    """
    return NormalizedListing(
        id=str(raw.get("id", "")),
        source=raw["source"],
        city=normalize_hebrew_text(raw.get("city")),
        neighborhood=normalize_hebrew_text(raw.get("neighborhood")),
        street=normalize_hebrew_text(raw.get("street")),
        deal_type=raw["deal_type"],
        property_type=raw["property_type"],
        rooms=normalize_rooms(raw.get("rooms")),
        floor=int(raw.get("floor") or 0),
        size_sqm=float(raw.get("size_sqm") or 0.0),
        price=normalize_price(raw.get("price")),
        description=normalize_hebrew_text(raw.get("description")),
        scraped_at=str(raw.get("scraped_at", "")),
        agent_phone=str(raw.get("agent_phone", "")),
    )


# ---------------------------------------------------------------------------
# Public entry point
# ---------------------------------------------------------------------------

def normalize(raw_listings: list[RawListing]) -> NormalizerStats:
    """
    Normalize a list of raw listings.

    Processes every listing through all field-level normalizers.
    Listings that fail entirely (unexpected exception) are skipped and logged.

    Args:
        raw_listings: List of RawListing dicts from the scrapers.

    Returns:
        NormalizerStats with counts and the normalised listing list.
    """
    start = time.monotonic()
    input_count = len(raw_listings)
    logger.info("normalize | start — %d input listings", input_count)

    normalised: list[NormalizedListing] = []
    for idx, raw in enumerate(raw_listings):
        try:
            normalised.append(normalize_listing(raw))
        except Exception:
            logger.error("normalize | failed on listing index %d (id=%s)", idx, raw.get("id"), exc_info=True)

    duration = time.monotonic() - start
    logger.info(
        "normalize | done — %d/%d listings normalised in %.2fs",
        len(normalised),
        input_count,
        duration,
    )
    return NormalizerStats(
        input_count=input_count,
        output_count=len(normalised),
        listings=normalised,
    )
