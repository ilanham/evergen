import csv
import logging
import re
from pathlib import Path
from typing import Iterator

import dlt

FULFILLMENT_FILE = Path(__file__).parents[2] / "files" / "source2_fulfillment.csv"

_TRUTHY = {"true", "1", "yes"}
_FALSY = {"false", "0", "no"}
_VALID_CARRIERS = {"FedEx", "UPS"}
_FULFILLMENT_ID_RE = re.compile(r"^F-\d{4}$")

logger = logging.getLogger(__name__)


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
    schema_contract={"columns": "freeze"},
    columns={
        "fulfillment_id":     {"data_type": "text",   "nullable": False},
        # ~14% null: F-2005, F-2015, F-2099 — load nulls faithfully, resolve in dbt
        "order_ref":          {"data_type": "text",   "nullable": True},
        "account_id":         {"data_type": "text",   "nullable": True},
        # No-hyphen format (TIS001GF) — normalized to match orders.product_code in dbt
        "sku":                {"data_type": "text",   "nullable": False},
        "shipped_qty":        {"data_type": "bigint", "nullable": False},
        "ship_date":          {"data_type": "text",   "nullable": False},
        "carrier":            {"data_type": "text",   "nullable": True},
        # Sparsely populated — many nulls
        "delivery_confirmed": {"data_type": "bool",   "nullable": True},
        # Free-text: contains "partial - N units backordered", "duplicate - reship attempt"
        "notes":              {"data_type": "text",   "nullable": True},
    },
)
def fulfillment_resource() -> Iterator[dict]:
    seen_ids: set[str] = set()

    with open(FULFILLMENT_FILE, newline="", encoding="utf-8") as f:
        for row in csv.DictReader(f):
            fulfillment_id = row["fulfillment_id"].strip() or None
            if not fulfillment_id:
                raise ValueError(f"fulfillment_id is blank in row: {row}")
            if not _FULFILLMENT_ID_RE.match(fulfillment_id):
                raise ValueError(
                    f"fulfillment_id={fulfillment_id!r} does not match F-NNNN pattern"
                )

            if fulfillment_id in seen_ids:
                # F-2013 is a known duplicate (reship attempt) — dbt staging deduplicates via QUALIFY
                logger.warning(
                    "Duplicate fulfillment_id=%s — loading both rows; deduplication handled in dbt staging",
                    fulfillment_id,
                )
            seen_ids.add(fulfillment_id)

            sku = row["sku"].strip() or None
            if not sku:
                raise ValueError(f"fulfillment_id={fulfillment_id}: sku is required")

            shipped_qty = int(row["shipped_qty"])
            if shipped_qty <= 0:
                raise ValueError(
                    f"fulfillment_id={fulfillment_id}: shipped_qty must be positive, got {shipped_qty}"
                )

            carrier = row["carrier"].strip() or None
            if carrier and carrier not in _VALID_CARRIERS:
                raise ValueError(
                    f"fulfillment_id={fulfillment_id}: carrier={carrier!r} not in {_VALID_CARRIERS}"
                )

            yield {
                "fulfillment_id":     fulfillment_id,
                "order_ref":          row["order_ref"] or None,
                "account_id":         row["account_id"] or None,
                "sku":                sku,
                "shipped_qty":        shipped_qty,
                "ship_date":          row["ship_date"] or None,
                "carrier":            carrier,
                "delivery_confirmed": _parse_bool(row.get("delivery_confirmed", "")),
                "notes":              row["notes"] or None,
            }
