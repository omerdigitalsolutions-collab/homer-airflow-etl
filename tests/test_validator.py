"""Unit tests for pipeline.validator."""

from __future__ import annotations

import pytest

from dags.pipeline.models import NormalizedListing
from dags.pipeline.validator import validate


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
        description="דירה יפה במיקום מרכזי",
        scraped_at="2024-01-15T23:00:00+00:00",
        agent_phone="050-1234567",
    )
    return {**base, **overrides}  # type: ignore[return-value]


class TestSpamFilter:
    """Listings that fail basic field checks should be rejected."""

    def test_missing_city_rejected(self) -> None:
        """A listing without a city is rejected."""
        listing = _make_listing(city="")
        stats = validate([listing])
        assert stats["valid_count"] == 0
        assert stats["rejected_count"] == 1
        assert "missing city" in stats["rejected"][0]["reason"]

    def test_zero_price_rejected(self) -> None:
        """A listing with price=0 is rejected."""
        listing = _make_listing(price=0.0)
        stats = validate([listing])
        assert stats["valid_count"] == 0
        assert stats["rejected_count"] == 1

    def test_spam_keyword_rejected(self) -> None:
        """A listing whose description contains 'בדיקה' is rejected."""
        listing = _make_listing(description="זוהי בדיקה בלבד")
        stats = validate([listing])
        assert stats["valid_count"] == 0
        assert stats["rejected_count"] == 1
        assert "spam keyword" in stats["rejected"][0]["reason"]

    def test_clean_listing_passes(self) -> None:
        """A fully valid listing is not rejected."""
        listing = _make_listing()
        stats = validate([listing])
        assert stats["valid_count"] == 1
        assert stats["rejected_count"] == 0


class TestOutlierDetection:
    """Price outliers outside ±3σ should be detected and removed."""

    def test_outlier_above_detected(self) -> None:
        """
        Build a cluster of normal prices and one price that is ×10 the mean.
        The outlier must be rejected; all others must pass.
        """
        normal_price = 2_000_000.0
        outlier_price = normal_price * 10   # definitely > μ + 3σ

        normal_listings = [
            _make_listing(id=str(i), price=normal_price + i * 10_000)
            for i in range(20)
        ]
        outlier_listing = _make_listing(id="outlier", price=outlier_price)

        stats = validate(normal_listings + [outlier_listing])

        assert stats["rejected_count"] == 1
        assert stats["rejected"][0]["listing"]["id"] == "outlier"
        assert "outlier" in stats["rejected"][0]["reason"]

    def test_outlier_below_detected(self) -> None:
        """A price that is ÷10 the mean is also detected as an outlier."""
        normal_price = 2_000_000.0
        outlier_price = normal_price / 10

        normal_listings = [
            _make_listing(id=str(i), price=normal_price + i * 10_000)
            for i in range(20)
        ]
        outlier_listing = _make_listing(id="low-outlier", price=outlier_price)

        stats = validate(normal_listings + [outlier_listing])

        assert stats["rejected_count"] == 1
        assert stats["rejected"][0]["listing"]["id"] == "low-outlier"

    def test_all_valid_when_no_outliers(self) -> None:
        """A tight cluster of prices should produce zero rejections."""
        listings = [
            _make_listing(id=str(i), price=2_000_000.0 + i * 5_000)
            for i in range(10)
        ]
        stats = validate(listings)
        assert stats["rejected_count"] == 0
        assert stats["valid_count"] == 10
