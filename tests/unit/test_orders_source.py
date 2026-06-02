"""Unit tests for ingestion/sources/orders.py validation logic."""
import csv
import io
from unittest.mock import patch

import pytest

from ingestion.sources.orders import _validate_date, orders_resource


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_row(**overrides) -> dict:
    base = {
        "order_id": "1001",
        "customer_code": "CUST-001",
        "customer_name": "Acme Corp",
        "product_code": "TIS-001-GF",
        "product_description": "Graft",
        "ordered_qty": "5",
        "order_date": "2024-01-15",
        "requested_ship_date": "2024-01-20",
        "region": "SOUTH",
        "rep_id": "REP-12",
    }
    base.update(overrides)
    return base


def _run_on_rows(rows: list[dict]) -> list[dict]:
    """Feed synthetic CSV rows through the orders resource generator."""
    import ingestion.sources.orders as mod

    fieldnames = list(rows[0].keys())
    buf = io.StringIO()
    writer = csv.DictWriter(buf, fieldnames=fieldnames)
    writer.writeheader()
    writer.writerows(rows)
    buf.seek(0)

    with patch.object(mod, "ORDERS_FILE", None):
        with patch("builtins.open", return_value=buf):
            return list(orders_resource())


# ---------------------------------------------------------------------------
# _validate_date
# ---------------------------------------------------------------------------

class TestValidateDate:
    def test_accepts_iso_format(self):
        _validate_date("2024-01-15", "order_date", 1001)  # no raise

    def test_accepts_us_format(self):
        _validate_date("01/15/2024", "order_date", 1001)  # no raise

    def test_rejects_unknown_format(self):
        with pytest.raises(ValueError, match="does not match"):
            _validate_date("15-01-2024", "order_date", 1001)

    def test_rejects_garbage(self):
        with pytest.raises(ValueError, match="does not match"):
            _validate_date("not-a-date", "order_date", 1001)


# ---------------------------------------------------------------------------
# orders_resource — happy path against the real CSV
# ---------------------------------------------------------------------------

class TestOrdersResourceHappyPath:
    def test_loads_all_rows(self):
        rows = list(orders_resource())
        assert len(rows) == 20

    def test_order_id_is_int(self):
        rows = list(orders_resource())
        assert all(isinstance(r["order_id"], int) for r in rows)

    def test_non_null_regions_are_valid(self):
        from ingestion.sources.orders import _VALID_REGIONS
        rows = list(orders_resource())
        for r in rows:
            if r["region"] is not None:
                assert r["region"] in _VALID_REGIONS

    def test_all_ordered_qty_positive(self):
        rows = list(orders_resource())
        assert all(r["ordered_qty"] > 0 for r in rows)

    def test_both_date_formats_survive(self):
        rows = list(orders_resource())
        dates = {r["order_date"] for r in rows}
        has_slash = any("/" in d for d in dates)
        has_dash = any("-" in d for d in dates)
        assert has_slash and has_dash, "Expected both date formats in raw order_date column"


# ---------------------------------------------------------------------------
# orders_resource — validation failures
# dlt wraps generator exceptions in ResourceExtractionError, so we match on
# Exception and verify the original message appears in the exception chain.
# ---------------------------------------------------------------------------

class TestOrdersResourceValidation:
    def test_raises_on_blank_order_id(self):
        with pytest.raises(Exception, match="order_id is blank"):
            _run_on_rows([_make_row(order_id="")])

    def test_raises_on_blank_customer_code(self):
        with pytest.raises(Exception, match="customer_code is required"):
            _run_on_rows([_make_row(customer_code="")])

    def test_raises_on_blank_product_code(self):
        with pytest.raises(Exception, match="product_code is required"):
            _run_on_rows([_make_row(product_code="")])

    def test_raises_on_zero_ordered_qty(self):
        with pytest.raises(Exception, match="ordered_qty must be positive"):
            _run_on_rows([_make_row(ordered_qty="0")])

    def test_raises_on_negative_ordered_qty(self):
        with pytest.raises(Exception, match="ordered_qty must be positive"):
            _run_on_rows([_make_row(ordered_qty="-1")])

    def test_raises_on_bad_date_format(self):
        with pytest.raises(Exception, match="does not match"):
            _run_on_rows([_make_row(order_date="15-01-2024")])

    def test_raises_on_bad_ship_date_format(self):
        with pytest.raises(Exception, match="does not match"):
            _run_on_rows([_make_row(requested_ship_date="not-a-date")])

    def test_raises_on_unknown_region(self):
        with pytest.raises(Exception, match="region="):
            _run_on_rows([_make_row(region="PACIFIC")])

    def test_null_region_is_allowed(self):
        result = _run_on_rows([_make_row(region="")])
        assert result[0]["region"] is None

    def test_null_ship_date_is_allowed(self):
        result = _run_on_rows([_make_row(requested_ship_date="")])
        assert result[0]["requested_ship_date"] is None
