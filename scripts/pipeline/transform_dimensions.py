"""
Phase 2: Transform & Load Dimensions
======================================
Cleans staging data and loads into conformed dimension tables.
Handles:
  - Date spine generation (dim_date)
  - Region name standardization
  - Boolean normalization
  - Deduplication
  - Surrogate key assignment
"""

import sys
from pathlib import Path
from datetime import date, timedelta, datetime
import calendar
import re

BASE_DIR = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(BASE_DIR))

from backend.app.database import engine, SessionLocal, Base
from backend.app.models import (
    StgCustomer, StgProduct, StgFinancial, StgEmployee,
    DimDate, DimCustomer, DimProduct, DimDepartment, DimRegion, DimEmployee
)

# ── Region Standardization Map ─────────────────────────────────────────

REGION_MAP = {
    "dallas-fort worth": "Dallas-Fort Worth",
    "dfw": "Dallas-Fort Worth",
    "dallas": "Dallas-Fort Worth",
    "dallas, tx": "Dallas-Fort Worth",
    "houston": "Houston",
    "houston, tx": "Houston",
    "austin": "Austin",
    "austin, tx": "Austin",
    "san antonio": "San Antonio",
    "san antonio, tx": "San Antonio",
}

REGION_DETAILS = {
    "Dallas-Fort Worth": {"city": "Dallas", "state": "TX", "timezone": "CST", "manager": "Sarah Johnson"},
    "Houston": {"city": "Houston", "state": "TX", "timezone": "CST", "manager": "David Martinez"},
    "Austin": {"city": "Austin", "state": "TX", "timezone": "CST", "manager": "Jennifer Lee"},
    "San Antonio": {"city": "San Antonio", "state": "TX", "timezone": "CST", "manager": "Michael Brown"},
}

DEPARTMENT_INFO = {
    "Sales": {"cost_center": "CC-100", "division": "Revenue", "headcount_budget": 35},
    "Operations": {"cost_center": "CC-200", "division": "Operations", "headcount_budget": 30},
    "Finance": {"cost_center": "CC-300", "division": "Corporate", "headcount_budget": 20},
    "Engineering": {"cost_center": "CC-400", "division": "Technology", "headcount_budget": 40},
    "Human Resources": {"cost_center": "CC-500", "division": "Corporate", "headcount_budget": 15},
    "Marketing": {"cost_center": "CC-600", "division": "Revenue", "headcount_budget": 20},
}


def standardize_region(raw_region):
    """Normalize messy region names to clean values."""
    if not raw_region:
        return "Dallas-Fort Worth"  # default
    key = raw_region.strip().lower()
    return REGION_MAP.get(key, "Dallas-Fort Worth")


def parse_date(date_str):
    """Parse dates from multiple formats into a date object."""
    if not date_str or date_str.strip() == "":
        return None

    date_str = date_str.strip()
    formats = ["%Y-%m-%d", "%m/%d/%Y", "%m-%d-%Y", "%d-%b-%Y", "%Y/%m/%d"]
    for fmt in formats:
        try:
            return datetime.strptime(date_str, fmt).date()
        except ValueError:
            continue
    return None


def normalize_boolean(val):
    """Convert messy boolean values to Python bool."""
    if val is None:
        return True
    truthy = {"yes", "y", "1", "true", "active"}
    return str(val).strip().lower() in truthy


def build_date_dimension(db):
    """Generate a complete date spine from 2024-01-01 to 2026-12-31."""
    print("  Building dim_date...")
    db.query(DimDate).delete()
    db.commit()

    start = date(2024, 1, 1)
    end = date(2026, 12, 31)
    current = start
    batch = []

    while current <= end:
        last_day = calendar.monthrange(current.year, current.month)[1]
        fiscal_year = current.year if current.month >= 7 else current.year
        fiscal_q = (current.month - 1) // 3 + 1

        dim = DimDate(
            date_key=int(current.strftime("%Y%m%d")),
            full_date=current,
            year=current.year,
            quarter=(current.month - 1) // 3 + 1,
            month=current.month,
            month_name=current.strftime("%B"),
            week=current.isocalendar()[1],
            day_of_week=current.weekday(),
            day_name=current.strftime("%A"),
            fiscal_year=fiscal_year,
            fiscal_quarter=f"FY{fiscal_year}-Q{fiscal_q}",
            is_weekend=current.weekday() >= 5,
            is_month_end=current.day == last_day,
            is_quarter_end=current.month in (3, 6, 9, 12) and current.day == last_day,
        )
        batch.append(dim)
        current += timedelta(days=1)

    db.bulk_save_objects(batch)
    db.commit()
    print(f"    -> {len(batch)} date records")
    return len(batch)


def build_region_dimension(db):
    """Build dim_region from reference data."""
    print("  Building dim_region...")
    db.query(DimRegion).delete()
    db.commit()

    for name, details in REGION_DETAILS.items():
        dim = DimRegion(
            name=name,
            city=details["city"],
            state=details["state"],
            timezone=details["timezone"],
            manager=details["manager"],
        )
        db.add(dim)
    db.commit()
    print(f"    -> {len(REGION_DETAILS)} regions")
    return len(REGION_DETAILS)


def build_department_dimension(db):
    """Build dim_department from reference data."""
    print("  Building dim_department...")
    db.query(DimDepartment).delete()
    db.commit()

    for name, info in DEPARTMENT_INFO.items():
        dim = DimDepartment(
            name=name,
            cost_center=info["cost_center"],
            division=info["division"],
            headcount_budget=info["headcount_budget"],
        )
        db.add(dim)
    db.commit()
    print(f"    -> {len(DEPARTMENT_INFO)} departments")
    return len(DEPARTMENT_INFO)


def get_region_key(db, region_name):
    """Look up region key by standardized name."""
    region = db.query(DimRegion).filter(DimRegion.name == region_name).first()
    return region.region_key if region else 1


def get_department_key(db, dept_name):
    """Look up department key by name."""
    dept = db.query(DimDepartment).filter(DimDepartment.name == dept_name).first()
    return dept.department_key if dept else 1


def build_customer_dimension(db):
    """Clean stg_customers into dim_customer, deduplicating by source_customer_id."""
    print("  Building dim_customer...")
    db.query(DimCustomer).delete()
    db.commit()

    staging = db.query(StgCustomer).all()
    seen = set()
    count = 0
    today = date.today()

    for stg in staging:
        if stg.source_customer_id in seen:
            continue
        seen.add(stg.source_customer_id)

        signup = parse_date(stg.signup_date)
        tenure = (today - signup).days if signup else 0

        dim = DimCustomer(
            source_customer_id=stg.source_customer_id,
            name=stg.name or "Unknown",
            email=stg.email,
            company=stg.company,
            industry=stg.industry,
            segment=stg.segment or "SMB",
            region=standardize_region(stg.region),
            city=stg.city,
            state=stg.state or "TX",
            signup_date=signup,
            customer_tenure_days=tenure,
            is_active=True,
        )
        db.add(dim)
        count += 1

    db.commit()
    print(f"    -> {count} customers (from {len(staging)} staging rows, {len(staging) - count} duplicates removed)")
    return count


def build_product_dimension(db):
    """Clean stg_products into dim_product, deduplicating by SKU."""
    print("  Building dim_product...")
    db.query(DimProduct).delete()
    db.commit()

    staging = db.query(StgProduct).all()
    seen_skus = set()
    count = 0

    for stg in staging:
        if stg.sku in seen_skus:
            continue
        seen_skus.add(stg.sku)

        cost = stg.unit_cost or 0
        price = stg.list_price or 0
        margin = round((price - cost) / price * 100, 2) if price > 0 else 0

        dim = DimProduct(
            source_product_id=stg.source_product_id,
            name=stg.name,
            sku=stg.sku,
            category=stg.category,
            subcategory=stg.subcategory,
            unit_cost=cost,
            list_price=price,
            margin_pct=margin,
            supplier=stg.supplier,
            is_active=normalize_boolean(stg.is_active),
        )
        db.add(dim)
        count += 1

    db.commit()
    print(f"    -> {count} products")
    return count


def build_employee_dimension(db):
    """Clean stg_employees into dim_employee with FK lookups."""
    print("  Building dim_employee...")
    db.query(DimEmployee).delete()
    db.commit()

    staging = db.query(StgEmployee).all()
    seen = set()
    count = 0

    for stg in staging:
        if stg.employee_id in seen:
            continue
        seen.add(stg.employee_id)

        region = standardize_region(stg.region)
        region_key = get_region_key(db, region)
        dept_key = get_department_key(db, stg.department)

        hire = parse_date(stg.hire_date)
        term = parse_date(stg.termination_date)
        is_active = stg.status != "Terminated" if stg.status else True

        dim = DimEmployee(
            employee_id=stg.employee_id,
            name=stg.name or "Unknown",
            department_key=dept_key,
            title=stg.title,
            hire_date=hire,
            termination_date=term,
            salary=stg.salary or 0,
            region_key=region_key,
            is_active=is_active,
        )
        db.add(dim)
        count += 1

    db.commit()
    print(f"    -> {count} employees")
    return count


def transform_dimensions():
    """Run all dimension transformations."""
    db = SessionLocal()
    results = {}
    try:
        results["dim_date"] = build_date_dimension(db)
        results["dim_region"] = build_region_dimension(db)
        results["dim_department"] = build_department_dimension(db)
        results["dim_customer"] = build_customer_dimension(db)
        results["dim_product"] = build_product_dimension(db)
        results["dim_employee"] = build_employee_dimension(db)
    finally:
        db.close()
    return results


if __name__ == "__main__":
    print("Phase 2: Transform & Load Dimensions")
    print("-" * 40)
    results = transform_dimensions()
    total = sum(results.values())
    print(f"\nTotal: {total} dimension records created")
