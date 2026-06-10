"""
Unit tests for the BigQuery loader (pipeline.loader.load_bigquery).

Tests cover:
- Empty input returns zero stats
- Normal input returns correct row counts
- Returned table_id uses the expected default dataset and table name
- Returned table_id respects BQ_PROJECT_ID / BQ_DATASET_ID / BQ_TABLE_ID env vars
- duration_seconds is non-negative
"""

from __future__ import annotations

import os
from typing import Any

import pytest

from pipeline.loader import (
    BQ_DEFAULT_DATASET,
    BQ_DEFAULT_PROJECT,
    BQ_DEFAULT_TABLE,
    load_bigquery,
)
from pipeline.models import DedupedListing


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_listing(i: int = 0) -> DedupedListing:
    """Return a minimal DedupedListing suitable for testing."""
    return DedupedListing(
        id=f"listing-{i}",
        source="yad2",
        city="תל אביב",
        neighborhood="פלורנטין",
        street=f"רחוב {i}",
        deal_type="למכירה",
        property_type="דירה",
        rooms=3.0,
        floor=2,
        size_sqm=75.0,
        price=2_000_000.0 + i * 1_000,
        description="נוף לים",
        scraped_at="2024-01-01T00:00:00Z",
        agent_phone="050-0000000",
        fingerprint=f"fp{i:032d}",
    )


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------

class TestLoadBigqueryEmpty:
    def test_empty_returns_zero_loaded(self) -> None:
        stats = load_bigquery([])
        assert stats["total_loaded"] == 0

    def test_empty_returns_zero_rows_inserted(self) -> None:
        stats = load_bigquery([])
        assert stats["rows_inserted"] == 0

    def test_empty_duration_is_zero(self) -> None:
        stats = load_bigquery([])
        assert stats["duration_seconds"] == 0.0

    def test_empty_table_id_contains_default_dataset(self) -> None:
        stats = load_bigquery([])
        assert BQ_DEFAULT_DATASET in stats["table_id"]

    def test_empty_table_id_contains_default_table(self) -> None:
        stats = load_bigquery([])
        assert BQ_DEFAULT_TABLE in stats["table_id"]


class TestLoadBigqueryNormal:
    def test_total_loaded_matches_input(self) -> None:
        listings = [_make_listing(i) for i in range(10)]
        stats = load_bigquery(listings)
        assert stats["total_loaded"] == 10

    def test_rows_inserted_matches_input(self) -> None:
        listings = [_make_listing(i) for i in range(10)]
        stats = load_bigquery(listings)
        assert stats["rows_inserted"] == 10

    def test_duration_is_positive(self) -> None:
        listings = [_make_listing(i) for i in range(5)]
        stats = load_bigquery(listings)
        assert stats["duration_seconds"] >= 0.0

    def test_table_id_format(self) -> None:
        """table_id should be 'project.dataset.table'."""
        listings = [_make_listing(0)]
        stats = load_bigquery(listings)
        parts = stats["table_id"].split(".")
        assert len(parts) == 3, f"Expected 3-part table_id, got: {stats['table_id']!r}"

    def test_table_id_contains_default_dataset(self) -> None:
        listings = [_make_listing(0)]
        stats = load_bigquery(listings)
        assert BQ_DEFAULT_DATASET in stats["table_id"]

    def test_table_id_contains_default_table(self) -> None:
        listings = [_make_listing(0)]
        stats = load_bigquery(listings)
        assert BQ_DEFAULT_TABLE in stats["table_id"]

    def test_large_batch_total_loaded(self) -> None:
        listings = [_make_listing(i) for i in range(500)]
        stats = load_bigquery(listings)
        assert stats["total_loaded"] == 500
        assert stats["rows_inserted"] == 500


class TestLoadBigqueryEnvVars:
    def test_custom_project_in_table_id(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv("BQ_PROJECT_ID", "my-custom-project")
        listings = [_make_listing(0)]
        stats = load_bigquery(listings)
        assert stats["table_id"].startswith("my-custom-project.")
        monkeypatch.delenv("BQ_PROJECT_ID", raising=False)

    def test_custom_dataset_in_table_id(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv("BQ_DATASET_ID", "custom_dataset")
        listings = [_make_listing(0)]
        stats = load_bigquery(listings)
        assert "custom_dataset" in stats["table_id"]
        monkeypatch.delenv("BQ_DATASET_ID", raising=False)

    def test_custom_table_in_table_id(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv("BQ_TABLE_ID", "custom_table")
        listings = [_make_listing(0)]
        stats = load_bigquery(listings)
        assert stats["table_id"].endswith(".custom_table")
        monkeypatch.delenv("BQ_TABLE_ID", raising=False)
