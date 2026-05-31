"""Unit tests for pipeline.deduplicator."""

from __future__ import annotations

import pytest

from dags.pipeline.deduplicator import compute_fingerprint, deduplicate
from dags.pipeline.models import NormalizedListing


def _make_listing(**overrides) -> NormalizedListing:
    """Factory for NormalizedListing with sensible defaults."""
    base: NormalizedListing = NormalizedListing(
        id="test-id",
        source="yad2",
        city="תל אביב",
        neighborhood="פלורנטין",
        street="רחוב הרצל 10",
        deal_type="למכירה",
        property_type="דירה",
        rooms=3.0,
        floor=2,
        size_sqm=80.0,
        price=2_000_000.0,
        description="דירה יפה",
        scraped_at="2024-01-15T23:00:00+00:00",
        agent_phone="050-1234567",
    )
    return {**base, **overrides}  # type: ignore[return-value]


class TestComputeFingerprint:
    """Unit tests for the fingerprint function itself."""

    def test_identical_listings_same_fingerprint(self) -> None:
        """Two identical listings produce the same fingerprint."""
        a = _make_listing(id="a")
        b = _make_listing(id="b")   # different id, same content
        assert compute_fingerprint(a) == compute_fingerprint(b)

    def test_different_city_different_fingerprint(self) -> None:
        """Changing the city changes the fingerprint."""
        a = _make_listing(city="תל אביב")
        b = _make_listing(city="חיפה")
        assert compute_fingerprint(a) != compute_fingerprint(b)

    def test_price_rounding_to_100k(self) -> None:
        """
        Prices rounded to the same 100K bucket produce identical fingerprints.
        1,950,000 and 2,050,000 both round to 2,000,000.
        """
        a = _make_listing(price=1_950_000.0)
        b = _make_listing(price=2_050_000.0)
        assert compute_fingerprint(a) == compute_fingerprint(b)

    def test_price_different_bucket_different_fingerprint(self) -> None:
        """Prices in different 100K buckets produce different fingerprints."""
        a = _make_listing(price=1_000_000.0)
        b = _make_listing(price=2_000_000.0)
        assert compute_fingerprint(a) != compute_fingerprint(b)

    def test_fingerprint_is_32_char_hex(self) -> None:
        """MD5 fingerprint must be a 32-character hex string."""
        fp = compute_fingerprint(_make_listing())
        assert len(fp) == 32
        assert all(c in "0123456789abcdef" for c in fp)


class TestDeduplicate:
    """Integration tests for deduplicate()."""

    def test_single_listing_passes_through(self) -> None:
        """A single listing is not dropped."""
        stats = deduplicate([_make_listing()])
        assert stats["unique_count"] == 1
        assert stats["duplicate_count"] == 0

    def test_exact_duplicate_removed(self) -> None:
        """Two identical listings (different id) → only one survives."""
        a = _make_listing(id="a")
        b = _make_listing(id="b")   # duplicate of a
        stats = deduplicate([a, b])
        assert stats["unique_count"] == 1
        assert stats["duplicate_count"] == 1

    def test_distinct_listings_all_kept(self) -> None:
        """Completely different listings are all kept."""
        listings = [
            _make_listing(id="1", city="תל אביב", price=1_000_000.0),
            _make_listing(id="2", city="חיפה",    price=1_000_000.0),
            _make_listing(id="3", city="ירושלים", price=1_000_000.0),
        ]
        stats = deduplicate(listings)
        assert stats["unique_count"] == 3
        assert stats["duplicate_count"] == 0

    def test_output_carries_fingerprint(self) -> None:
        """Every listing in the output has a non-empty fingerprint field."""
        stats = deduplicate([_make_listing()])
        for item in stats["listings"]:
            assert "fingerprint" in item
            assert len(item["fingerprint"]) == 32

    def test_first_occurrence_is_kept(self) -> None:
        """When duplicates exist, the first occurrence (by list order) is kept."""
        a = _make_listing(id="first")
        b = _make_listing(id="second")
        stats = deduplicate([a, b])
        assert stats["listings"][0]["id"] == "first"

    def test_empty_list_returns_zero_counts(self) -> None:
        """Empty input produces zero counts without errors."""
        stats = deduplicate([])
        assert stats["unique_count"] == 0
        assert stats["duplicate_count"] == 0
        assert stats["listings"] == []
