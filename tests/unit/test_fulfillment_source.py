"""Unit tests for ingestion/sources/fulfillment.py validation logic."""
import csv
import io
import logging
from unittest.mock import patch

import pytest

from ingestion.sources.fulfillment import _parse_bool, fulfillment_resource


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_row(**overrides) -> dict:
    base = {
        "fulfillment_id": "F-2001",
        "order_ref": "1001",
        "account_id": "ACC-001",
        "sku": "TIS001GF",
        "shipped_qty": "5",
        "ship_date": "2024-01-20",
        "carrier": "FedEx",
        "delivery_confirmed": "true",
        "notes": "",
    }
    base.update(overrides)
    return base


def _run_on_rows(rows: list[dict]) -> list[dict]:
    """Feed synthetic CSV rows through the fulfillment resource generator."""
    import ingestion.sources.fulfillment as mod

    fieldnames = list(rows[0].keys())
    buf = io.StringIO()
    writer = csv.DictWriter(buf, fieldnames=fieldnames)
    writer.writeheader()
    writer.writerows(rows)
    buf.seek(0)

    with patch.object(mod, "FULFILLMENT_FILE", None):
        with patch("builtins.open", return_value=buf):
            return list(fulfillment_resource())


# ---------------------------------------------------------------------------
# _parse_bool
# ---------------------------------------------------------------------------

class TestParseBool:
    @pytest.mark.parametrize("raw", ["true", "True", "TRUE", "1", "yes", "YES"])
    def test_truthy_values(self, raw):
        assert _parse_bool(raw) is True

    @pytest.mark.parametrize("raw", ["false", "False", "FALSE", "0", "no", "NO"])
    def test_falsy_values(self, raw):
        assert _parse_bool(raw) is False

    @pytest.mark.parametrize("raw", ["", " ", "maybe", "n/a"])
    def test_unknown_returns_none(self, raw):
        assert _parse_bool(raw) is None


# ---------------------------------------------------------------------------
# fulfillment_resource — happy path against the real CSV
# ---------------------------------------------------------------------------

class TestFulfillmentResourceHappyPath:
    def test_loads_all_rows(self):
        # Source CSV has 21 rows (including one F-2013 duplicate)
        rows = list(fulfillment_resource())
        assert len(rows) == 21

    def test_fulfillment_id_non_null(self):
        rows = list(fulfillment_resource())
        assert all(r["fulfillment_id"] is not None for r in rows)

    def test_sku_non_null(self):
        rows = list(fulfillment_resource())
        assert all(r["sku"] is not None for r in rows)

    def test_shipped_qty_positive(self):
        rows = list(fulfillment_resource())
        assert all(r["shipped_qty"] > 0 for r in rows)



# ---------------------------------------------------------------------------
# fulfillment_resource — validation failures
# dlt wraps generator exceptions in ResourceExtractionError, so we match on
# Exception and verify the original message appears in the exception chain.
# ---------------------------------------------------------------------------

class TestFulfillmentResourceValidation:
    def test_raises_on_blank_fulfillment_id(self):
        with pytest.raises(Exception, match="fulfillment_id is blank"):
            _run_on_rows([_make_row(fulfillment_id="")])

    def test_raises_on_malformed_fulfillment_id(self):
        with pytest.raises(Exception, match="does not match F-NNNN pattern"):
            _run_on_rows([_make_row(fulfillment_id="FUL-2001")])

    def test_raises_on_blank_sku(self):
        with pytest.raises(Exception, match="sku is required"):
            _run_on_rows([_make_row(sku="")])

    def test_raises_on_zero_shipped_qty(self):
        with pytest.raises(Exception, match="shipped_qty must be positive"):
            _run_on_rows([_make_row(shipped_qty="0")])

    def test_raises_on_negative_shipped_qty(self):
        with pytest.raises(Exception, match="shipped_qty must be positive"):
            _run_on_rows([_make_row(shipped_qty="-3")])

    def test_raises_on_unknown_carrier(self):
        with pytest.raises(Exception, match="carrier="):
            _run_on_rows([_make_row(carrier="DHL")])

    def test_null_carrier_is_allowed(self):
        result = _run_on_rows([_make_row(carrier="")])
        assert result[0]["carrier"] is None

    def test_null_order_ref_is_allowed(self):
        result = _run_on_rows([_make_row(order_ref="")])
        assert result[0]["order_ref"] is None

    def test_duplicate_emits_warning(self, caplog):
        rows = [_make_row(), _make_row()]  # same fulfillment_id twice
        with caplog.at_level(logging.WARNING, logger="ingestion.sources.fulfillment"):
            _run_on_rows(rows)
        assert any("Duplicate fulfillment_id" in m for m in caplog.messages)
