import csv
import logging
from datetime import datetime
from pathlib import Path
from typing import Iterator

import dlt

ORDERS_FILE = Path(__file__).parents[2] / "files" / "source1_orders.csv"

_VALID_REGIONS = {"SOUTH", "NORTHEAST", "MIDWEST", "WEST", "SOUTHEAST"}
_DATE_FORMATS = ("%m/%d/%Y", "%Y-%m-%d")

logger = logging.getLogger(__name__)


def _validate_date(value: str, field: str, order_id: int) -> None:
    for fmt in _DATE_FORMATS:
        try:
            datetime.strptime(value, fmt)
            return
        except ValueError:
            continue
    raise ValueError(
        f"order_id={order_id}: {field}={value!r} does not match MM/DD/YYYY or YYYY-MM-DD"
    )


@dlt.resource(
    name="orders",
    write_disposition="replace",
    schema_contract={"columns": "freeze"},
    columns={
        "order_id":            {"data_type": "bigint", "nullable": False},
        "customer_code":       {"data_type": "text",   "nullable": False},
        "customer_name":       {"data_type": "text",   "nullable": True},
        "product_code":        {"data_type": "text",   "nullable": False},
        "product_description": {"data_type": "text",   "nullable": True},
        "ordered_qty":         {"data_type": "bigint", "nullable": False},
        # Mixed date formats (MM/DD/YYYY and YYYY-MM-DD) — load as text, normalize in dbt
        "order_date":          {"data_type": "text",   "nullable": False},
        "requested_ship_date": {"data_type": "text",   "nullable": True},
        "region":              {"data_type": "text",   "nullable": True},
        "rep_id":              {"data_type": "text",   "nullable": True},
    },
)
def orders_resource() -> Iterator[dict]:
    with open(ORDERS_FILE, newline="", encoding="utf-8") as f:
        for row in csv.DictReader(f):
            order_id_raw = row["order_id"].strip()
            if not order_id_raw:
                raise ValueError(f"order_id is blank in row: {row}")
            order_id = int(order_id_raw)

            customer_code = row["customer_code"].strip() or None
            if not customer_code:
                raise ValueError(f"order_id={order_id}: customer_code is required")

            product_code = row["product_code"].strip() or None
            if not product_code:
                raise ValueError(f"order_id={order_id}: product_code is required")

            ordered_qty = int(row["ordered_qty"])
            if ordered_qty <= 0:
                raise ValueError(
                    f"order_id={order_id}: ordered_qty must be positive, got {ordered_qty}"
                )

            order_date = row["order_date"].strip() or None
            if order_date:
                _validate_date(order_date, "order_date", order_id)

            requested_ship_date = row["requested_ship_date"].strip() or None
            if requested_ship_date:
                _validate_date(requested_ship_date, "requested_ship_date", order_id)

            region = row["region"].strip() or None
            if region and region not in _VALID_REGIONS:
                raise ValueError(
                    f"order_id={order_id}: region={region!r} not in {_VALID_REGIONS}"
                )

            yield {
                "order_id":            order_id,
                "customer_code":       customer_code,
                "customer_name":       row["customer_name"] or None,
                "product_code":        product_code,
                "product_description": row["product_description"] or None,
                "ordered_qty":         ordered_qty,
                "order_date":          order_date,
                "requested_ship_date": requested_ship_date,
                "region":              region,
                "rep_id":              row["rep_id"] or None,
            }
