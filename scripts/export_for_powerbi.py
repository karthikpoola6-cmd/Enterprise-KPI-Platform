"""
Export Data for Power BI
========================
Exports dimension and fact tables as CSV files optimized for Power BI import.
Denormalizes key relationships for easier dashboard creation.
"""

import csv
import sys
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(BASE_DIR))

from backend.app.database import SessionLocal
from backend.app.models import (
    DimDate, DimCustomer, DimProduct, DimDepartment, DimRegion, DimEmployee,
    FactSales, FactFinancial, FactWorkforce, FactKPISnapshot,
    DataQualityLog,
)

EXPORT_DIR = BASE_DIR / "data" / "exports"
EXPORT_DIR.mkdir(parents=True, exist_ok=True)


def export_table(db, filename, query, headers, row_fn):
    """Generic export function."""
    filepath = EXPORT_DIR / filename
    rows = query.all()
    with open(filepath, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(headers)
        for row in rows:
            writer.writerow(row_fn(row))
    print(f"  {filename}: {len(rows)} rows")
    return len(rows)


def export_all():
    db = SessionLocal()

    try:
        # lookups for denormalization
        regions = {r.region_key: r.name for r in db.query(DimRegion).all()}
        depts = {d.department_key: d.name for d in db.query(DimDepartment).all()}
        customers = {c.customer_key: (c.name, c.segment, c.region) for c in db.query(DimCustomer).all()}
        products = {p.product_key: (p.name, p.category, p.sku) for p in db.query(DimProduct).all()}

        # 1. fact_sales_export.csv
        export_table(db, "fact_sales_export.csv",
            db.query(FactSales),
            ["order_id", "order_date_key", "customer_name", "customer_segment",
             "product_name", "product_category", "product_sku", "region",
             "quantity", "unit_price", "discount_pct", "gross_amount",
             "discount_amount", "net_amount", "cost_of_goods", "gross_profit",
             "fulfillment_days", "sla_met"],
            lambda s: [
                s.order_id, s.order_date_key,
                customers.get(s.customer_key, ("Unknown", "Unknown", "Unknown"))[0],
                customers.get(s.customer_key, ("Unknown", "Unknown", "Unknown"))[1],
                products.get(s.product_key, ("Unknown", "Unknown", "Unknown"))[0],
                products.get(s.product_key, ("Unknown", "Unknown", "Unknown"))[1],
                products.get(s.product_key, ("Unknown", "Unknown", "Unknown"))[2],
                regions.get(s.region_key, "Unknown"),
                s.quantity, s.unit_price, s.discount_pct, s.gross_amount,
                s.discount_amount, s.net_amount, s.cost_of_goods, s.gross_profit,
                s.fulfillment_days, s.sla_met,
            ])

        # 2. fact_financial_export.csv
        export_table(db, "fact_financial_export.csv",
            db.query(FactFinancial),
            ["date_key", "department", "region", "account_name", "account_type",
             "actual_amount", "budget_amount", "variance_amount", "variance_pct"],
            lambda f: [
                f.date_key, depts.get(f.department_key, "Unknown"),
                regions.get(f.region_key, "Unknown"),
                f.account_name, f.account_type,
                f.actual_amount, f.budget_amount, f.variance_amount, f.variance_pct,
            ])

        # 3. fact_workforce_export.csv
        export_table(db, "fact_workforce_export.csv",
            db.query(FactWorkforce),
            ["date_key", "department", "region", "active_headcount", "new_hires",
             "terminations", "total_salary_cost", "avg_salary", "turnover_rate"],
            lambda w: [
                w.date_key, depts.get(w.department_key, "Unknown"),
                regions.get(w.region_key, "Unknown"),
                w.active_headcount, w.new_hires, w.terminations,
                w.total_salary_cost, w.avg_salary, w.turnover_rate,
            ])

        # 4. dim_date.csv
        export_table(db, "dim_date.csv",
            db.query(DimDate),
            ["date_key", "full_date", "year", "quarter", "month", "month_name",
             "week", "day_name", "fiscal_year", "fiscal_quarter",
             "is_weekend", "is_month_end", "is_quarter_end"],
            lambda d: [
                d.date_key, d.full_date, d.year, d.quarter, d.month, d.month_name,
                d.week, d.day_name, d.fiscal_year, d.fiscal_quarter,
                d.is_weekend, d.is_month_end, d.is_quarter_end,
            ])

        # 5. dim_customer.csv
        export_table(db, "dim_customer.csv",
            db.query(DimCustomer),
            ["customer_key", "name", "company", "industry", "segment",
             "region", "city", "state", "signup_date", "tenure_days", "is_active"],
            lambda c: [
                c.customer_key, c.name, c.company, c.industry, c.segment,
                c.region, c.city, c.state, c.signup_date,
                c.customer_tenure_days, c.is_active,
            ])

        # 6. dim_product.csv
        export_table(db, "dim_product.csv",
            db.query(DimProduct),
            ["product_key", "name", "sku", "category", "subcategory",
             "unit_cost", "list_price", "margin_pct", "supplier", "is_active"],
            lambda p: [
                p.product_key, p.name, p.sku, p.category, p.subcategory,
                p.unit_cost, p.list_price, p.margin_pct, p.supplier, p.is_active,
            ])

        # 7. kpi_summary.csv
        export_table(db, "kpi_summary.csv",
            db.query(FactKPISnapshot),
            ["date_key", "kpi_name", "kpi_value", "kpi_unit",
             "dimension", "dimension_value", "period"],
            lambda k: [
                k.date_key, k.kpi_name, k.kpi_value, k.kpi_unit,
                k.dimension, k.dimension_value, k.period,
            ])

        # 8. data_quality_results.csv
        latest_run = db.query(DataQualityLog.run_id).order_by(
            DataQualityLog.run_at.desc()
        ).first()

        if latest_run:
            export_table(db, "data_quality_results.csv",
                db.query(DataQualityLog).filter(DataQualityLog.run_id == latest_run[0]),
                ["check_name", "table_name", "layer", "status",
                 "records_checked", "records_failed", "failure_rate", "details"],
                lambda q: [
                    q.check_name, q.table_name, q.layer, q.status,
                    q.records_checked, q.records_failed, q.failure_rate, q.details,
                ])

    finally:
        db.close()


if __name__ == "__main__":
    print("=" * 50)
    print("Exporting data for Power BI")
    print("=" * 50)
    export_all()
    print(f"\nExports saved to: {EXPORT_DIR}")
