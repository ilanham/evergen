import csv
from pathlib import Path
from typing import Iterator

import dlt

ORDERS_FILE = Path(__file__).parents[2] / "files" / "source1_orders.csv"


@dlt.resource(
    name="orders",
    write_disposition="replace",
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
            yield {
                "order_id":            int(row["order_id"]),
                "customer_code":       row["customer_code"] or None,
                "customer_name":       row["customer_name"] or None,
                "product_code":        row["product_code"] or None,
                "product_description": row["product_description"] or None,
                "ordered_qty":         int(row["ordered_qty"]),
                "order_date":          row["order_date"] or None,
                "requested_ship_date": row["requested_ship_date"] or None,
                "region":              row["region"] or None,
                "rep_id":              row["rep_id"] or None,
            }
