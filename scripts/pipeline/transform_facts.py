"""
Phase 3: Transform & Load Facts
=================================
Joins staging data with dimension keys and computes fact tables:
  - fact_sales: order-level sales with computed margins
  - fact_financial: budget vs actual with variance
  - fact_workforce: monthly headcount snapshots
  - fact_kpi_snapshot: rolled-up KPI values
"""

import sys
from pathlib import Path
from datetime import date, datetime
from collections import defaultdict

BASE_DIR = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(BASE_DIR))

from backend.app.database import engine, SessionLocal, Base
from backend.app.models import (
    StgOrder, StgFinancial, StgEmployee,
    DimDate, DimCustomer, DimProduct, DimRegion, DimDepartment, DimEmployee,
    FactSales, FactFinancial, FactWorkforce, FactKPISnapshot,
)
from scripts.pipeline.transform_dimensions import parse_date, standardize_region


def get_date_key(dt):
    """Convert a date to YYYYMMDD integer key."""
    if not dt:
        return None
    return int(dt.strftime("%Y%m%d"))


def build_lookup(db, model, key_field, value_field):
    """Build a dict mapping a field value to the surrogate key."""
    lookup = {}
    for row in db.query(model).all():
        lookup[getattr(row, value_field)] = getattr(row, key_field)
    return lookup


def build_fact_sales(db):
    """Transform stg_orders into fact_sales with dimension key lookups."""
    print("  Building fact_sales...")
    db.query(FactSales).delete()
    db.commit()

    # build lookups
    customer_lookup = build_lookup(db, DimCustomer, "customer_key", "source_customer_id")
    product_lookup = build_lookup(db, DimProduct, "product_key", "sku")
    region_lookup = build_lookup(db, DimRegion, "region_key", "name")

    # get product costs for COGS calculation
    product_costs = {}
    for prod in db.query(DimProduct).all():
        product_costs[prod.sku] = prod.unit_cost or 0

    staging = db.query(StgOrder).all()
    seen_orders = set()
    batch = []
    count = 0

    for stg in staging:
        # deduplicate orders
        if stg.source_order_id in seen_orders:
            continue
        seen_orders.add(stg.source_order_id)

        # skip cancelled orders
        if stg.status and stg.status.lower() == "cancelled":
            continue

        order_date = parse_date(stg.order_date)
        ship_date = parse_date(stg.ship_date)
        delivery_date = parse_date(stg.delivery_date)

        if not order_date:
            continue

        # dimension key lookups
        region = standardize_region(stg.region)
        region_key = region_lookup.get(region, 1)

        # match customer by email pattern (simplified — in real ETL this would be a proper join)
        customer_key = None
        if stg.customer_email:
            # find first matching customer
            for cid, ckey in customer_lookup.items():
                customer_key = ckey
                break
        if not customer_key:
            customer_key = list(customer_lookup.values())[0] if customer_lookup else 1

        product_key = product_lookup.get(stg.product_sku, 1)
        unit_cost = product_costs.get(stg.product_sku, 0)

        # compute measures
        quantity = stg.quantity or 1
        unit_price = stg.unit_price or 0
        discount_pct = stg.discount_pct or 0

        gross_amount = round(quantity * unit_price, 2)
        discount_amount = round(gross_amount * discount_pct / 100, 2)
        net_amount = round(gross_amount - discount_amount, 2)
        cost_of_goods = round(quantity * unit_cost, 2)
        gross_profit = round(net_amount - cost_of_goods, 2)

        fulfillment_days = None
        if delivery_date and order_date:
            fulfillment_days = (delivery_date - order_date).days

        sla_met = fulfillment_days is not None and fulfillment_days <= 5

        fact = FactSales(
            order_date_key=get_date_key(order_date),
            ship_date_key=get_date_key(ship_date),
            customer_key=customer_key,
            product_key=product_key,
            region_key=region_key,
            order_id=stg.source_order_id,
            quantity=quantity,
            unit_price=unit_price,
            discount_pct=discount_pct,
            gross_amount=gross_amount,
            discount_amount=discount_amount,
            net_amount=net_amount,
            cost_of_goods=cost_of_goods,
            gross_profit=gross_profit,
            fulfillment_days=fulfillment_days,
            sla_met=sla_met,
        )
        batch.append(fact)
        count += 1

        if len(batch) >= 500:
            db.bulk_save_objects(batch)
            db.commit()
            batch = []

    if batch:
        db.bulk_save_objects(batch)
        db.commit()

    print(f"    -> {count} sales facts (from {len(staging)} staging rows, dupes/cancelled removed)")
    return count


def build_fact_financial(db):
    """Transform stg_financials into fact_financial with variance calculations."""
    print("  Building fact_financial...")
    db.query(FactFinancial).delete()
    db.commit()

    dept_lookup = build_lookup(db, DimDepartment, "department_key", "name")
    region_lookup = build_lookup(db, DimRegion, "region_key", "name")

    staging = db.query(StgFinancial).all()
    batch = []
    count = 0

    for stg in staging:
        # build date key from fiscal year/month
        if stg.fiscal_year and stg.fiscal_month:
            try:
                dt = date(stg.fiscal_year, stg.fiscal_month, 1)
                date_key = get_date_key(dt)
            except (ValueError, TypeError):
                continue
        else:
            continue

        region = standardize_region(stg.region)
        region_key = region_lookup.get(region, 1)
        dept_key = dept_lookup.get(stg.department, 1)

        actual = stg.amount or 0
        budget = stg.budget_amount or 0
        variance = round(actual - budget, 2)
        variance_pct = round(variance / budget * 100, 2) if budget != 0 else 0

        fact = FactFinancial(
            date_key=date_key,
            department_key=dept_key,
            region_key=region_key,
            account_name=stg.account_name,
            account_type=stg.account_type,
            actual_amount=actual,
            budget_amount=budget,
            variance_amount=variance,
            variance_pct=variance_pct,
        )
        batch.append(fact)
        count += 1

    db.bulk_save_objects(batch)
    db.commit()
    print(f"    -> {count} financial facts")
    return count


def build_fact_workforce(db):
    """Compute monthly workforce snapshots from dim_employee."""
    print("  Building fact_workforce...")
    db.query(FactWorkforce).delete()
    db.commit()

    dept_lookup = build_lookup(db, DimDepartment, "department_key", "name")
    region_lookup = build_lookup(db, DimRegion, "region_key", "name")
    employees = db.query(DimEmployee).all()

    batch = []
    count = 0

    # generate monthly snapshots from 2024-07 to 2026-03
    start_year, start_month = 2024, 7
    end_year, end_month = 2026, 3

    year, month = start_year, start_month
    while (year, month) <= (end_year, end_month):
        snapshot_date = date(year, month, 1)
        date_key = get_date_key(snapshot_date)

        # group by department
        dept_stats = defaultdict(lambda: {
            "active": 0, "new_hires": 0, "terminations": 0,
            "total_salary": 0, "region_key": 1
        })

        for emp in employees:
            hire = emp.hire_date
            term = emp.termination_date

            if not hire or hire > snapshot_date:
                continue

            dept_key = emp.department_key or 1

            # check if active in this month
            if term and term < snapshot_date:
                # already terminated before this month
                # check if terminated IN this month
                if term.year == year and term.month == month:
                    dept_stats[dept_key]["terminations"] += 1
                continue

            if term and term.year == year and term.month == month:
                dept_stats[dept_key]["terminations"] += 1

            dept_stats[dept_key]["active"] += 1
            dept_stats[dept_key]["total_salary"] += emp.salary or 0
            dept_stats[dept_key]["region_key"] = emp.region_key or 1

            # check if hired this month
            if hire.year == year and hire.month == month:
                dept_stats[dept_key]["new_hires"] += 1

        for dept_key, stats in dept_stats.items():
            active = stats["active"]
            avg_salary = round(stats["total_salary"] / active, 2) if active > 0 else 0
            turnover = round(stats["terminations"] / active * 100, 2) if active > 0 else 0

            fact = FactWorkforce(
                date_key=date_key,
                department_key=dept_key,
                region_key=stats["region_key"],
                active_headcount=active,
                new_hires=stats["new_hires"],
                terminations=stats["terminations"],
                total_salary_cost=round(stats["total_salary"], 2),
                avg_salary=avg_salary,
                turnover_rate=turnover,
            )
            batch.append(fact)
            count += 1

        # next month
        month += 1
        if month > 12:
            month = 1
            year += 1

    db.bulk_save_objects(batch)
    db.commit()
    print(f"    -> {count} workforce snapshots")
    return count


def build_fact_kpi_snapshot(db):
    """Compute rolled-up KPI values from fact tables."""
    print("  Building fact_kpi_snapshot...")
    db.query(FactKPISnapshot).delete()
    db.commit()

    batch = []

    # ── Sales KPIs by month ──
    sales = db.query(FactSales).all()
    sales_by_month = defaultdict(list)
    for s in sales:
        if s.order_date_key:
            month_key = (s.order_date_key // 100) * 100 + 1  # first of month
            sales_by_month[month_key].append(s)

    for month_key, month_sales in sorted(sales_by_month.items()):
        total_revenue = sum(s.net_amount or 0 for s in month_sales)
        total_profit = sum(s.gross_profit or 0 for s in month_sales)
        total_orders = len(month_sales)
        sla_count = sum(1 for s in month_sales if s.sla_met)
        margin_pct = round(total_profit / total_revenue * 100, 2) if total_revenue > 0 else 0
        fulfillment_rate = round(sla_count / total_orders * 100, 2) if total_orders > 0 else 0
        avg_order = round(total_revenue / total_orders, 2) if total_orders > 0 else 0

        kpis = [
            ("Total Revenue", total_revenue, "$", "overall", "All"),
            ("Gross Margin %", margin_pct, "%", "overall", "All"),
            ("Order Count", total_orders, "count", "overall", "All"),
            ("Avg Order Value", avg_order, "$", "overall", "All"),
            ("Fulfillment Rate", fulfillment_rate, "%", "overall", "All"),
        ]

        for name, value, unit, dim, dim_val in kpis:
            batch.append(FactKPISnapshot(
                date_key=month_key,
                kpi_name=name,
                kpi_value=round(value, 2),
                kpi_unit=unit,
                dimension=dim,
                dimension_value=dim_val,
                period="monthly",
            ))

    # ── Sales KPIs by region ──
    region_lookup = {}
    for r in db.query(DimRegion).all():
        region_lookup[r.region_key] = r.name

    sales_by_region_month = defaultdict(lambda: defaultdict(list))
    for s in sales:
        if s.order_date_key and s.region_key:
            month_key = (s.order_date_key // 100) * 100 + 1
            region_name = region_lookup.get(s.region_key, "Unknown")
            sales_by_region_month[month_key][region_name].append(s)

    for month_key, regions in sales_by_region_month.items():
        for region_name, region_sales in regions.items():
            rev = sum(s.net_amount or 0 for s in region_sales)
            batch.append(FactKPISnapshot(
                date_key=month_key,
                kpi_name="Total Revenue",
                kpi_value=round(rev, 2),
                kpi_unit="$",
                dimension="region",
                dimension_value=region_name,
                period="monthly",
            ))

    # ── Workforce KPIs ──
    workforce = db.query(FactWorkforce).all()
    wf_by_month = defaultdict(list)
    for w in workforce:
        if w.date_key:
            wf_by_month[w.date_key].append(w)

    for month_key, wf_rows in sorted(wf_by_month.items()):
        total_headcount = sum(w.active_headcount for w in wf_rows)
        total_hires = sum(w.new_hires for w in wf_rows)
        total_terms = sum(w.terminations for w in wf_rows)
        total_salary = sum(w.total_salary_cost for w in wf_rows)
        turnover = round(total_terms / total_headcount * 100, 2) if total_headcount > 0 else 0

        kpis = [
            ("Headcount", total_headcount, "count", "overall", "All"),
            ("New Hires", total_hires, "count", "overall", "All"),
            ("Turnover Rate", turnover, "%", "overall", "All"),
            ("Total Salary Cost", total_salary, "$", "overall", "All"),
        ]

        for name, value, unit, dim, dim_val in kpis:
            batch.append(FactKPISnapshot(
                date_key=month_key,
                kpi_name=name,
                kpi_value=round(value, 2),
                kpi_unit=unit,
                dimension=dim,
                dimension_value=dim_val,
                period="monthly",
            ))

    db.bulk_save_objects(batch)
    db.commit()
    print(f"    -> {len(batch)} KPI snapshots")
    return len(batch)


def transform_facts():
    """Run all fact table transformations."""
    db = SessionLocal()
    results = {}
    try:
        results["fact_sales"] = build_fact_sales(db)
        results["fact_financial"] = build_fact_financial(db)
        results["fact_workforce"] = build_fact_workforce(db)
        results["fact_kpi_snapshot"] = build_fact_kpi_snapshot(db)
    finally:
        db.close()
    return results


if __name__ == "__main__":
    print("Phase 3: Transform & Load Facts")
    print("-" * 40)
    results = transform_facts()
    total = sum(results.values())
    print(f"\nTotal: {total} fact records created")
