"""Unit tests for pipeline.normalizer."""

from __future__ import annotations

import pytest

from dags.pipeline.normalizer import normalize_hebrew_text, normalize_price, normalize_rooms


class TestNormalizePrice:
    """Tests for normalize_price()."""

    def test_plain_integer_string(self) -> None:
        """Integer string is converted to float."""
        assert normalize_price("1500000") == 1_500_000.0

    def test_currency_symbol_stripped(self) -> None:
        """₪ and other non-numeric characters are removed."""
        assert normalize_price("₪2,500,000") == 2_500_000.0

    def test_float_preserved(self) -> None:
        """Float value is returned unchanged."""
        assert normalize_price(3_500.50) == pytest.approx(3_500.50)

    def test_none_returns_default(self) -> None:
        """None input returns 0.0 without raising."""
        assert normalize_price(None) == 0.0

    def test_garbage_string_returns_default(self) -> None:
        """A completely non-numeric string returns 0.0."""
        assert normalize_price("N/A") == 0.0

    def test_dollar_prefix_stripped(self) -> None:
        """Dollar sign is stripped correctly."""
        assert normalize_price("$500") == 500.0


class TestNormalizeRooms:
    """Tests for normalize_rooms()."""

    def test_integer_string(self) -> None:
        """Integer string returns correct float."""
        assert normalize_rooms("3") == 3.0

    def test_dot_decimal(self) -> None:
        """European dot notation (3.5) is parsed correctly."""
        assert normalize_rooms("3.5") == 3.5

    def test_comma_decimal(self) -> None:
        """Israeli comma notation (3,5) is normalised to 3.5."""
        assert normalize_rooms("3,5") == 3.5

    def test_float_passthrough(self) -> None:
        """Numeric float is returned as-is."""
        assert normalize_rooms(4.5) == 4.5

    def test_none_returns_default(self) -> None:
        """None input returns 0.0 without raising."""
        assert normalize_rooms(None) == 0.0

    def test_garbage_returns_default(self) -> None:
        """Non-parseable string returns 0.0."""
        assert normalize_rooms("חמש") == 0.0


class TestNormalizeHebrewText:
    """Tests for normalize_hebrew_text()."""

    def test_double_whitespace_collapsed(self) -> None:
        """Multiple spaces are collapsed to one."""
        assert normalize_hebrew_text("תל   אביב") == "תל אביב"

    def test_leading_trailing_stripped(self) -> None:
        """Leading/trailing whitespace is removed."""
        assert normalize_hebrew_text("  ירושלים  ") == "ירושלים"

    def test_none_returns_empty(self) -> None:
        """None input returns empty string without raising."""
        assert normalize_hebrew_text(None) == ""

    def test_non_string_returns_empty(self) -> None:
        """Non-string input returns empty string."""
        assert normalize_hebrew_text(42) == ""

    def test_clean_text_unchanged(self) -> None:
        """Clean Hebrew text is returned as-is."""
        assert normalize_hebrew_text("רמת גן") == "רמת גן"
