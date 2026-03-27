"""
Phase 1: Extract & Load to Staging
===================================
Reads raw CSV files from data/raw/ and loads them into staging tables.
Truncates staging tables before each load (full refresh pattern).
Adds source_system and loaded_at metadata.
"""

import csv
import sys
from pathlib import Path
from datetime import datetime, timezone

BASE_DIR = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(BASE_DIR))

from backend.app.database import engine, SessionLocal, Base
from backend.app.models import StgOrder, StgCustomer, StgProduct, StgFinancial, StgEmployee

RAW_DIR = BASE_DIR / "data" / "raw"

STAGING_MAP = {
    "orders_raw.csv": {
        "model": StgOrder,
        "field_map": {
            "order_id": "source_order_id",
            "customer_name": "customer_name",
            "customer_email": "customer_email",
            "product_name": "product_name",
            "product_sku": "product_sku",
            "category": "category",
            "quantity": "quantity",
            "unit_price": "unit_price",
            "discount_pct": "discount_pct",
            "order_date": "order_date",
            "ship_date": "ship_date",
            "delivery_date": "delivery_date",
            "status": "status",
            "region": "region",
            "sales_rep": "sales_rep",
        },
        "source_system": "salesforce_export",
    },
    "customers_raw.csv": {
        "model": StgCustomer,
        "field_map": {
            "customer_id": "source_customer_id",
            "name": "name",
            "email": "email",
            "phone": "phone",
            "company": "company",
            "industry": "industry",
            "segment": "segment",
            "region": "region",
            "city": "city",
            "state": "state",
            "signup_date": "signup_date",
            "last_order_date": "last_order_date",
        },
        "source_system": "salesforce_export",
    },
    "products_raw.csv": {
        "model": StgProduct,
        "field_map": {
            "product_id": "source_product_id",
            "name": "name",
            "sku": "sku",
            "category": "category",
            "subcategory": "subcategory",
            "unit_cost": "unit_cost",
            "list_price": "list_price",
            "supplier": "supplier",
            "is_active": "is_active",
        },
        "source_system": "warehouse_export",
    },
    "financials_raw.csv": {
        "model": StgFinancial,
        "field_map": {
            "account_name": "account_name",
            "account_type": "account_type",
            "amount": "amount",
            "fiscal_year": "fiscal_year",
            "fiscal_quarter": "fiscal_quarter",
            "fiscal_month": "fiscal_month",
            "department": "department",
            "region": "region",
            "budget_amount": "budget_amount",
            "is_budget": "is_budget",
        },
        "source_system": "quickbooks_export",
    },
    "employees_raw.csv": {
        "model": StgEmployee,
        "field_map": {
            "employee_id": "employee_id",
            "name": "name",
            "department": "department",
            "title": "title",
            "hire_date": "hire_date",
            "termination_date": "termination_date",
            "salary": "salary",
            "region": "region",
            "manager": "manager",
            "status": "status",
        },
        "source_system": "adp_export",
    },
}


def extract_to_staging():
    """Load all raw CSVs into staging tables."""
    Base.metadata.create_all(bind=engine)
    db = SessionLocal()
    results = {}

    try:
        for filename, config in STAGING_MAP.items():
            filepath = RAW_DIR / filename
            if not filepath.exists():
                print(f"  WARNING: {filename} not found, skipping")
                results[filename] = 0
                continue

            model = config["model"]
            field_map = config["field_map"]
            source_system = config["source_system"]

            # truncate staging table
            db.query(model).delete()
            db.commit()

            # read CSV and load
            count = 0
            with open(filepath, "r") as f:
                reader = csv.DictReader(f)
                batch = []
                for row in reader:
                    record = {}
                    for csv_col, db_col in field_map.items():
                        val = row.get(csv_col, "")
                        if val == "" or val is None:
                            val = None
                        elif db_col in ("quantity", "fiscal_year", "fiscal_month"):
                            try:
                                val = int(val)
                            except (ValueError, TypeError):
                                val = None
                        elif db_col in ("unit_price", "discount_pct", "unit_cost",
                                        "list_price", "amount", "budget_amount", "salary"):
                            try:
                                val = float(val)
                            except (ValueError, TypeError):
                                val = None
                        record[db_col] = val

                    record["source_system"] = source_system
                    batch.append(model(**record))
                    count += 1

                    if len(batch) >= 500:
                        db.bulk_save_objects(batch)
                        db.commit()
                        batch = []

                if batch:
                    db.bulk_save_objects(batch)
                    db.commit()

            results[filename] = count
            print(f"  Loaded {count} rows from {filename}")

    finally:
        db.close()

    return results


if __name__ == "__main__":
    print("Phase 1: Extract & Load to Staging")
    print("-" * 40)
    results = extract_to_staging()
    total = sum(results.values())
    print(f"\nTotal: {total} records loaded to staging")
