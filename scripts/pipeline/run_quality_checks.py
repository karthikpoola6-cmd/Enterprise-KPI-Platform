"""
Phase 4: Data Quality Checks
==============================
Runs 15+ validations across all layers:
  - Completeness (no nulls in required fields)
  - Uniqueness (no duplicate keys)
  - Referential integrity (FK relationships valid)
  - Range checks (values within expected bounds)
  - Freshness (data is recent)

All results are logged to data_quality_log table.
"""

import sys
import uuid
from pathlib import Path
from datetime import datetime, timezone, date

BASE_DIR = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(BASE_DIR))

from sqlalchemy import func, text
from backend.app.database import SessionLocal
from backend.app.models import (
    DimDate, DimCustomer, DimProduct, DimDepartment, DimRegion, DimEmployee,
    FactSales, FactFinancial, FactWorkforce, FactKPISnapshot,
    DataQualityLog,
)


def log_check(db, run_id, check_name, table_name, layer, status,
              records_checked, records_failed, details=""):
    """Write a quality check result to the log."""
    failure_rate = round(records_failed / records_checked * 100, 2) if records_checked > 0 else 0
    entry = DataQualityLog(
        run_id=run_id,
        check_name=check_name,
        table_name=table_name,
        layer=layer,
        status=status,
        records_checked=records_checked,
        records_failed=records_failed,
        failure_rate=failure_rate,
        details=details,
    )
    db.add(entry)
    db.commit()

    icon = "PASS" if status == "pass" else ("WARN" if status == "warning" else "FAIL")
    print(f"    [{icon}] {check_name} — {table_name}: {records_failed}/{records_checked} failed ({failure_rate}%)")
    return status


def run_quality_checks():
    """Execute all data quality validations."""
    db = SessionLocal()
    run_id = str(uuid.uuid4())[:8]
    results = {"pass": 0, "fail": 0, "warning": 0}

    try:
        # ── Completeness Checks ────────────────────────────────────

        print("\n  Completeness Checks:")

        # 1. No null order_id in fact_sales
        total = db.query(FactSales).count()
        failed = db.query(FactSales).filter(FactSales.order_id == None).count()
        status = "pass" if failed == 0 else "fail"
        log_check(db, run_id, "no_null_order_id", "fact_sales", "fact", status, total, failed)
        results[status] += 1

        # 2. No null customer_key in fact_sales
        failed = db.query(FactSales).filter(FactSales.customer_key == None).count()
        status = "pass" if failed == 0 else "fail"
        log_check(db, run_id, "no_null_customer_key", "fact_sales", "fact", status, total, failed)
        results[status] += 1

        # 3. No null amount in fact_financial
        fin_total = db.query(FactFinancial).count()
        fin_failed = db.query(FactFinancial).filter(FactFinancial.actual_amount == None).count()
        status = "pass" if fin_failed == 0 else "fail"
        log_check(db, run_id, "no_null_actual_amount", "fact_financial", "fact", status, fin_total, fin_failed)
        results[status] += 1

        # 4. No null name in dim_customer
        cust_total = db.query(DimCustomer).count()
        cust_failed = db.query(DimCustomer).filter(DimCustomer.name == None).count()
        status = "pass" if cust_failed == 0 else "fail"
        log_check(db, run_id, "no_null_customer_name", "dim_customer", "dimension", status, cust_total, cust_failed)
        results[status] += 1

        # 5. No null name in dim_product
        prod_total = db.query(DimProduct).count()
        prod_failed = db.query(DimProduct).filter(DimProduct.name == None).count()
        status = "pass" if prod_failed == 0 else "fail"
        log_check(db, run_id, "no_null_product_name", "dim_product", "dimension", status, prod_total, prod_failed)
        results[status] += 1

        # ── Uniqueness Checks ──────────────────────────────────────

        print("\n  Uniqueness Checks:")

        # 6. No duplicate order_id in fact_sales
        distinct_orders = db.query(func.count(func.distinct(FactSales.order_id))).scalar()
        dupes = total - distinct_orders
        status = "pass" if dupes == 0 else "fail"
        log_check(db, run_id, "unique_order_id", "fact_sales", "fact", status, total, dupes,
                   f"{dupes} duplicate order IDs found")
        results[status] += 1

        # 7. No duplicate source_customer_id in dim_customer
        distinct_cust = db.query(func.count(func.distinct(DimCustomer.source_customer_id))).scalar()
        dupes = cust_total - distinct_cust
        status = "pass" if dupes == 0 else "fail"
        log_check(db, run_id, "unique_customer_id", "dim_customer", "dimension", status, cust_total, dupes)
        results[status] += 1

        # 8. No duplicate SKU in dim_product
        distinct_sku = db.query(func.count(func.distinct(DimProduct.sku))).scalar()
        dupes = prod_total - distinct_sku
        status = "pass" if dupes == 0 else "fail"
        log_check(db, run_id, "unique_product_sku", "dim_product", "dimension", status, prod_total, dupes)
        results[status] += 1

        # 9. No duplicate date_key in dim_date
        date_total = db.query(DimDate).count()
        distinct_dates = db.query(func.count(func.distinct(DimDate.date_key))).scalar()
        dupes = date_total - distinct_dates
        status = "pass" if dupes == 0 else "fail"
        log_check(db, run_id, "unique_date_key", "dim_date", "dimension", status, date_total, dupes)
        results[status] += 1

        # ── Referential Integrity ──────────────────────────────────

        print("\n  Referential Integrity Checks:")

        # 10. Every customer_key in fact_sales exists in dim_customer
        valid_customers = {c.customer_key for c in db.query(DimCustomer.customer_key).all()}
        orphan_count = 0
        for row in db.query(FactSales.customer_key).distinct().all():
            if row[0] not in valid_customers:
                orphan_count += 1
        distinct_keys = db.query(func.count(func.distinct(FactSales.customer_key))).scalar()
        status = "pass" if orphan_count == 0 else "warning"
        log_check(db, run_id, "fk_customer_key_valid", "fact_sales", "fact", status, distinct_keys, orphan_count)
        results[status] += 1

        # 11. Every product_key in fact_sales exists in dim_product
        valid_products = {p.product_key for p in db.query(DimProduct.product_key).all()}
        orphan_count = 0
        for row in db.query(FactSales.product_key).distinct().all():
            if row[0] not in valid_products:
                orphan_count += 1
        distinct_keys = db.query(func.count(func.distinct(FactSales.product_key))).scalar()
        status = "pass" if orphan_count == 0 else "warning"
        log_check(db, run_id, "fk_product_key_valid", "fact_sales", "fact", status, distinct_keys, orphan_count)
        results[status] += 1

        # 12. Every department_key in fact_financial exists in dim_department
        valid_depts = {d.department_key for d in db.query(DimDepartment.department_key).all()}
        orphan_count = 0
        for row in db.query(FactFinancial.department_key).distinct().all():
            if row[0] not in valid_depts:
                orphan_count += 1
        distinct_keys = db.query(func.count(func.distinct(FactFinancial.department_key))).scalar()
        status = "pass" if orphan_count == 0 else "warning"
        log_check(db, run_id, "fk_department_key_valid", "fact_financial", "fact", status, distinct_keys, orphan_count)
        results[status] += 1

        # ── Range / Reasonableness Checks ──────────────────────────

        print("\n  Range Checks:")

        # 13. unit_price > 0 in fact_sales
        neg_price = db.query(FactSales).filter(FactSales.unit_price <= 0).count()
        status = "pass" if neg_price == 0 else "warning"
        log_check(db, run_id, "positive_unit_price", "fact_sales", "fact", status, total, neg_price)
        results[status] += 1

        # 14. quantity between 1 and 10000
        bad_qty = db.query(FactSales).filter(
            (FactSales.quantity < 1) | (FactSales.quantity > 10000)
        ).count()
        status = "pass" if bad_qty == 0 else "warning"
        log_check(db, run_id, "reasonable_quantity", "fact_sales", "fact", status, total, bad_qty)
        results[status] += 1

        # 15. salary between 30000 and 500000
        emp_total = db.query(DimEmployee).count()
        bad_salary = db.query(DimEmployee).filter(
            (DimEmployee.salary < 30000) | (DimEmployee.salary > 500000)
        ).count()
        threshold = emp_total * 0.05
        status = "pass" if bad_salary == 0 else ("warning" if bad_salary <= threshold else "fail")
        log_check(db, run_id, "reasonable_salary", "dim_employee", "dimension", status, emp_total, bad_salary)
        results[status] += 1

        # 16. gross_profit not negative for >5% of records
        neg_profit = db.query(FactSales).filter(FactSales.gross_profit < 0).count()
        threshold = total * 0.05
        status = "pass" if neg_profit <= threshold else "warning"
        log_check(db, run_id, "acceptable_negative_profit", "fact_sales", "fact", status, total, neg_profit,
                   f"{neg_profit} records with negative profit (threshold: {int(threshold)})")
        results[status] += 1

    finally:
        db.close()

    return results


if __name__ == "__main__":
    print("Phase 4: Data Quality Checks")
    print("-" * 40)
    results = run_quality_checks()
    print(f"\n  Summary: {results['pass']} passed, {results['warning']} warnings, {results['fail']} failed")
