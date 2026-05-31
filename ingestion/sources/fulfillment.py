import csv
from pathlib import Path
from typing import Iterator

import dlt

FULFILLMENT_FILE = Path(__file__).parents[2] / "files" / "source2_fulfillment.csv"

_TRUTHY = {"true", "1", "yes"}
_FALSY = {"false", "0", "no"}


def _parse_bool(raw: str) -> bool | None:
    v = raw.strip().lower()
    if v in _TRUTHY:
        return True
    if v in _FALSY:
        return False
    return None


@dlt.resource(
    name="fulfillment",
    write_disposition="replace",
    columns={
        "fulfillment_id":     {"data_type": "text",   "nullable": False},
        # ~14% null: F-2005, F-2015, F-2099 — load nulls faithfully, resolve in dbt
        "order_ref":          {"data_type": "text",   "nullable": True},
        "account_id":         {"data_type": "text",   "nullable": True},
        # No-hyphen format (TIS001GF) — normalized to match orders.product_code in dbt
        "sku":                {"data_type": "text",   "nullable": False},
        "shipped_qty":        {"data_type": "bigint", "nullable": False},
        "ship_date":          {"data_type": "text",   "nullable": False},
        # Sparsely populated — many nulls
        "delivery_confirmed": {"data_type": "bool",   "nullable": True},
        # Free-text: contains "partial - N units backordered", "duplicate - reship attempt"
        "notes":              {"data_type": "text",   "nullable": True},
    },
)
def fulfillment_resource() -> Iterator[dict]:
    with open(FULFILLMENT_FILE, newline="", encoding="utf-8") as f:
        for row in csv.DictReader(f):
            yield {
                "fulfillment_id":     row["fulfillment_id"] or None,
                "order_ref":          row["order_ref"] or None,
                "account_id":         row["account_id"] or None,
                "sku":                row["sku"] or None,
                "shipped_qty":        int(row["shipped_qty"]),
                "ship_date":          row["ship_date"] or None,
                "carrier":            row["carrier"] or None,
                "delivery_confirmed": _parse_bool(row.get("delivery_confirmed", "")),
                "notes":              row["notes"] or None,
            }
