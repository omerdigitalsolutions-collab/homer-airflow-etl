"""
Data models for the Homer ETL pipeline.

Defines TypedDict schemas for property listings and pipeline statistics.
All pipeline modules share these definitions for type consistency.

Loader stats
------------
- LoaderStats        — Firestore batch loader
- BigQueryLoaderStats — BigQuery sync loader
"""

from __future__ import annotations

from typing import Literal, TypedDict


# ---------------------------------------------------------------------------
# Listing schema
# ---------------------------------------------------------------------------

DealType = Literal["למכירה", "להשכרה"]
PropertyType = Literal["דירה", "פנטהאוז", "קוטג'", "דופלקס", "דירת גן"]
Source = Literal["yad2", "facebook"]


class RawListing(TypedDict):
    """A single property listing as returned by a scraper."""

    id: str
    source: Source
    city: str
    neighborhood: str
    street: str
    deal_type: DealType
    property_type: PropertyType
    rooms: float
    floor: int
    size_sqm: float
    price: float
    description: str
    scraped_at: str          # ISO-8601 timestamp string
    agent_phone: str


class NormalizedListing(TypedDict):
    """A listing after normalization — guaranteed clean types."""

    id: str
    source: Source
    city: str
    neighborhood: str
    street: str
    deal_type: DealType
    property_type: PropertyType
    rooms: float
    floor: int
    size_sqm: float
    price: float
    description: str
    scraped_at: str
    agent_phone: str


class DedupedListing(NormalizedListing):
    """A listing after deduplication — carries its MD5 fingerprint."""

    fingerprint: str


# ---------------------------------------------------------------------------
# Validation result
# ---------------------------------------------------------------------------

class RejectedListing(TypedDict):
    """A listing that failed validation, with a rejection reason."""

    listing: NormalizedListing
    reason: str


# ---------------------------------------------------------------------------
# Pipeline statistics (passed via XCom)
# ---------------------------------------------------------------------------

class ScraperStats(TypedDict):
    """XCom payload produced by each scraper task."""

    source: Source
    count: int
    listings: list[RawListing]


class NormalizerStats(TypedDict):
    """XCom payload produced by the normalizer task."""

    input_count: int
    output_count: int
    listings: list[NormalizedListing]


class ValidatorStats(TypedDict):
    """XCom payload produced by the validator task."""

    input_count: int
    valid_count: int
    rejected_count: int
    listings: list[NormalizedListing]
    rejected: list[RejectedListing]


class DeduplicatorStats(TypedDict):
    """XCom payload produced by the deduplicator task."""

    input_count: int
    unique_count: int
    duplicate_count: int
    listings: list[DedupedListing]


class LoaderStats(TypedDict):
    """XCom payload produced by the Firestore loader task."""

    total_loaded: int
    batch_count: int
    duration_seconds: float


class BigQueryLoaderStats(TypedDict):
    """XCom payload produced by the BigQuery loader task."""

    total_loaded: int
    rows_inserted: int
    table_id: str
    duration_seconds: float
