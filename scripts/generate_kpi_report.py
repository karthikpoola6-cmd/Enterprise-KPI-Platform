"""
Generate Executive KPI Report
===============================
Produces a plain-text executive summary from the warehouse data.
Designed for leadership review — highlights key metrics, trends, and concerns.
"""

import sys
from pathlib import Path
from datetime import datetime
from collections import defaultdict

BASE_DIR = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(BASE_DIR))

from sqlalchemy import func
from backend.app.database import SessionLocal
from backend.app.models import (
    FactSales, FactFinancial, FactWorkforce, FactKPISnapshot,
    DimRegion, DimDepartment, DimProduct,
    DataQualityLog,
)


def generate_report():
    db = SessionLocal()
    lines = []

    def add(text=""): lines.append(text)

    try:
        add("=" * 60)
        add("RISE Inc. — Executive KPI Report")
        add(f"Generated: {datetime.now().strftime('%B %d, %Y at %I:%M %p')}")
        add("=" * 60)

        # ── Sales Summary ──
        add("\n1. SALES PERFORMANCE")
        add("-" * 40)
        sales = db.query(FactSales).all()
        total_revenue = sum(s.net_amount or 0 for s in sales)
        total_profit = sum(s.gross_profit or 0 for s in sales)
        total_orders = len(sales)
        sla_met = sum(1 for s in sales if s.sla_met)
        margin = (total_profit / total_revenue * 100) if total_revenue > 0 else 0
        fulfillment = (sla_met / total_orders * 100) if total_orders > 0 else 0
        avg_order = total_revenue / total_orders if total_orders > 0 else 0

        add(f"  Total Revenue:      ${total_revenue:,.0f}")
        add(f"  Gross Profit:       ${total_profit:,.0f}")
        add(f"  Gross Margin:       {margin:.1f}%")
        add(f"  Total Orders:       {total_orders:,}")
        add(f"  Avg Order Value:    ${avg_order:,.0f}")
        add(f"  Fulfillment Rate:   {fulfillment:.1f}%")

        # by region
        regions = {r.region_key: r.name for r in db.query(DimRegion).all()}
        region_rev = defaultdict(float)
        for s in sales:
            region_rev[regions.get(s.region_key, "Unknown")] += s.net_amount or 0

        add("\n  Revenue by Region:")
        for region, rev in sorted(region_rev.items(), key=lambda x: -x[1]):
            add(f"    {region:25s} ${rev:>12,.0f}")

        # by product
        products = {p.product_key: p.category for p in db.query(DimProduct).all()}
        cat_rev = defaultdict(float)
        for s in sales:
            cat_rev[products.get(s.product_key, "Unknown")] += s.net_amount or 0

        add("\n  Revenue by Product Category:")
        for cat, rev in sorted(cat_rev.items(), key=lambda x: -x[1]):
            add(f"    {cat:25s} ${rev:>12,.0f}")

        # ── Financial Summary ──
        add("\n2. FINANCIAL HEALTH")
        add("-" * 40)
        financials = db.query(FactFinancial).all()
        fin_rev = sum(f.actual_amount for f in financials if f.account_type == "revenue")
        fin_exp = sum(f.actual_amount for f in financials if f.account_type == "expense")
        fin_cogs = sum(f.actual_amount for f in financials if f.account_type == "cogs")
        net_income = fin_rev - fin_exp - fin_cogs
        budget_total = sum(f.budget_amount or 0 for f in financials)
        actual_total = sum(f.actual_amount or 0 for f in financials)
        budget_var = ((actual_total - budget_total) / budget_total * 100) if budget_total > 0 else 0

        add(f"  Total Revenue:      ${fin_rev:,.0f}")
        add(f"  Total Expenses:     ${fin_exp:,.0f}")
        add(f"  Total COGS:         ${fin_cogs:,.0f}")
        add(f"  Net Income:         ${net_income:,.0f}")
        add(f"  Budget Variance:    {budget_var:+.1f}%")

        # ── Workforce Summary ──
        add("\n3. WORKFORCE")
        add("-" * 40)
        latest_wf_date = db.query(func.max(FactWorkforce.date_key)).scalar()
        if latest_wf_date:
            workforce = db.query(FactWorkforce).filter(FactWorkforce.date_key == latest_wf_date).all()
            total_hc = sum(w.active_headcount for w in workforce)
            total_hires = sum(w.new_hires for w in workforce)
            total_terms = sum(w.terminations for w in workforce)
            total_salary = sum(w.total_salary_cost for w in workforce)
            turnover = (total_terms / total_hc * 100) if total_hc > 0 else 0
            rev_per_emp = total_revenue / total_hc if total_hc > 0 else 0

            add(f"  Active Headcount:   {total_hc}")
            add(f"  New Hires (Month):  {total_hires}")
            add(f"  Terminations:       {total_terms}")
            add(f"  Turnover Rate:      {turnover:.1f}%")
            add(f"  Total Salary Cost:  ${total_salary:,.0f}")
            add(f"  Revenue/Employee:   ${rev_per_emp:,.0f}")

        # ── Data Quality ──
        add("\n4. DATA QUALITY")
        add("-" * 40)
        latest_run = db.query(DataQualityLog.run_id).order_by(
            DataQualityLog.run_at.desc()
        ).first()

        if latest_run:
            checks = db.query(DataQualityLog).filter(DataQualityLog.run_id == latest_run[0]).all()
            passed = sum(1 for c in checks if c.status == "pass")
            warnings = sum(1 for c in checks if c.status == "warning")
            failed = sum(1 for c in checks if c.status == "fail")
            add(f"  Total Checks:       {len(checks)}")
            add(f"  Passed:             {passed}")
            add(f"  Warnings:           {warnings}")
            add(f"  Failed:             {failed}")

            if failed > 0 or warnings > 0:
                add("\n  Issues:")
                for c in checks:
                    if c.status in ("fail", "warning"):
                        add(f"    [{c.status.upper()}] {c.check_name} on {c.table_name}: "
                            f"{c.records_failed}/{c.records_checked} ({c.failure_rate}%)")

        # ── Recommendations ──
        add("\n5. RECOMMENDATIONS")
        add("-" * 40)

        if margin < 20:
            add("  - Gross margin below 20% — review pricing strategy and COGS")
        if fulfillment < 80:
            add("  - Fulfillment rate below 80% — investigate supply chain bottlenecks")
        if latest_wf_date and turnover > 15:
            add("  - Turnover rate above 15% — assess retention programs")
        if budget_var > 10:
            add("  - Budget variance over 10% — review spending controls")
        if budget_var < -10:
            add("  - Significant under-spending — evaluate resource allocation")
        if margin >= 20 and fulfillment >= 80:
            add("  - Core metrics healthy — continue monitoring trends")

        add(f"\n{'=' * 60}")
        add("End of Report")
        add(f"{'=' * 60}")

    finally:
        db.close()

    report = "\n".join(lines)

    output_path = BASE_DIR / "data" / "executive_kpi_report.txt"
    with open(output_path, "w") as f:
        f.write(report)

    print(report)
    print(f"\nReport saved to: {output_path}")
    return report


if __name__ == "__main__":
    generate_report()
