from fastapi import FastAPI, Depends, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from sqlalchemy.orm import Session
from sqlalchemy import func, desc, case
from collections import defaultdict
import subprocess
import sys
from pathlib import Path

from .database import engine, SessionLocal, Base, get_db
from .models import (
    DimDate, DimCustomer, DimProduct, DimDepartment, DimRegion, DimEmployee,
    FactSales, FactFinancial, FactWorkforce, FactKPISnapshot,
    DataQualityLog, PipelineRunLog,
)
from .schemas import (
    KPIValue, KPITrend, SalesSummary, RegionSales, ProductSales, SegmentSales,
    FinanceOverview, DepartmentVariance, WorkforceOverview, DepartmentWorkforce,
    QualityCheckResult, PipelineStatus,
)

Base.metadata.create_all(bind=engine)

app = FastAPI(title="Enterprise KPI Platform", version="1.0.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:3000", "http://localhost:3001"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


# ── KPI Endpoints ─────────────────────────────────────────────────────

@app.get("/api/kpis", response_model=list[KPIValue])
def get_kpis(db: Session = Depends(get_db)):
    """Get the latest KPI values (most recent month, overall dimension)."""
    latest_date = db.query(func.max(FactKPISnapshot.date_key)).filter(
        FactKPISnapshot.dimension == "overall"
    ).scalar()

    if not latest_date:
        return []

    rows = db.query(FactKPISnapshot).filter(
        FactKPISnapshot.date_key == latest_date,
        FactKPISnapshot.dimension == "overall",
    ).all()

    return rows


@app.get("/api/kpis/trends")
def get_kpi_trends(
    kpi_name: str = "Total Revenue",
    dimension: str = "overall",
    db: Session = Depends(get_db),
):
    """Get KPI values over time for trend charts."""
    query = db.query(FactKPISnapshot).filter(
        FactKPISnapshot.kpi_name == kpi_name,
        FactKPISnapshot.dimension == dimension,
    ).order_by(FactKPISnapshot.date_key).all()

    results = []
    for row in query:
        date_key = str(row.date_key)
        month = f"{date_key[:4]}-{date_key[4:6]}"
        results.append({
            "date_key": row.date_key,
            "month": month,
            "kpi_name": row.kpi_name,
            "kpi_value": row.kpi_value,
            "kpi_unit": row.kpi_unit,
            "dimension_value": row.dimension_value,
        })

    return results


# ── Sales Endpoints ────────────────────────────────────────────────────

@app.get("/api/sales/summary", response_model=SalesSummary)
def get_sales_summary(
    region: str = None,
    category: str = None,
    db: Session = Depends(get_db),
):
    """Get aggregated sales metrics with optional filters."""
    query = db.query(FactSales)

    if region:
        region_obj = db.query(DimRegion).filter(DimRegion.name == region).first()
        if region_obj:
            query = query.filter(FactSales.region_key == region_obj.region_key)

    if category:
        product_keys = [p.product_key for p in
                        db.query(DimProduct).filter(DimProduct.category == category).all()]
        query = query.filter(FactSales.product_key.in_(product_keys))

    sales = query.all()

    if not sales:
        return SalesSummary(
            total_revenue=0, total_orders=0, avg_order_value=0,
            total_discount=0, gross_profit=0, gross_margin_pct=0,
            fulfillment_rate=0,
        )

    total_revenue = sum(s.net_amount or 0 for s in sales)
    total_orders = len(sales)
    total_discount = sum(s.discount_amount or 0 for s in sales)
    gross_profit = sum(s.gross_profit or 0 for s in sales)
    sla_count = sum(1 for s in sales if s.sla_met)

    return SalesSummary(
        total_revenue=round(total_revenue, 2),
        total_orders=total_orders,
        avg_order_value=round(total_revenue / total_orders, 2) if total_orders > 0 else 0,
        total_discount=round(total_discount, 2),
        gross_profit=round(gross_profit, 2),
        gross_margin_pct=round(gross_profit / total_revenue * 100, 2) if total_revenue > 0 else 0,
        fulfillment_rate=round(sla_count / total_orders * 100, 2) if total_orders > 0 else 0,
    )


@app.get("/api/sales/by-region", response_model=list[RegionSales])
def get_sales_by_region(db: Session = Depends(get_db)):
    """Sales breakdown by region."""
    regions = {r.region_key: r.name for r in db.query(DimRegion).all()}
    sales = db.query(FactSales).all()

    region_data = defaultdict(lambda: {"revenue": 0, "orders": 0, "profit": 0})
    for s in sales:
        name = regions.get(s.region_key, "Unknown")
        region_data[name]["revenue"] += s.net_amount or 0
        region_data[name]["orders"] += 1
        region_data[name]["profit"] += s.gross_profit or 0

    results = []
    for name, data in sorted(region_data.items(), key=lambda x: -x[1]["revenue"]):
        margin = round(data["profit"] / data["revenue"] * 100, 2) if data["revenue"] > 0 else 0
        results.append(RegionSales(
            region=name,
            revenue=round(data["revenue"], 2),
            orders=data["orders"],
            gross_profit=round(data["profit"], 2),
            margin_pct=margin,
        ))
    return results


@app.get("/api/sales/by-product", response_model=list[ProductSales])
def get_sales_by_product(db: Session = Depends(get_db)):
    """Sales breakdown by product category."""
    products = {p.product_key: p.category for p in db.query(DimProduct).all()}
    sales = db.query(FactSales).all()

    cat_data = defaultdict(lambda: {"revenue": 0, "orders": 0, "profit": 0})
    for s in sales:
        category = products.get(s.product_key, "Unknown")
        cat_data[category]["revenue"] += s.net_amount or 0
        cat_data[category]["orders"] += 1
        cat_data[category]["profit"] += s.gross_profit or 0

    results = []
    for cat, data in sorted(cat_data.items(), key=lambda x: -x[1]["revenue"]):
        margin = round(data["profit"] / data["revenue"] * 100, 2) if data["revenue"] > 0 else 0
        results.append(ProductSales(
            category=cat,
            revenue=round(data["revenue"], 2),
            orders=data["orders"],
            gross_profit=round(data["profit"], 2),
            margin_pct=margin,
        ))
    return results


@app.get("/api/sales/by-customer-segment", response_model=list[SegmentSales])
def get_sales_by_segment(db: Session = Depends(get_db)):
    """Sales breakdown by customer segment."""
    customers = {c.customer_key: c.segment for c in db.query(DimCustomer).all()}
    sales = db.query(FactSales).all()

    seg_data = defaultdict(lambda: {"revenue": 0, "orders": 0})
    for s in sales:
        segment = customers.get(s.customer_key, "SMB")
        seg_data[segment]["revenue"] += s.net_amount or 0
        seg_data[segment]["orders"] += 1

    results = []
    for seg, data in sorted(seg_data.items(), key=lambda x: -x[1]["revenue"]):
        avg_order = round(data["revenue"] / data["orders"], 2) if data["orders"] > 0 else 0
        results.append(SegmentSales(
            segment=seg,
            revenue=round(data["revenue"], 2),
            orders=data["orders"],
            avg_order_value=avg_order,
        ))
    return results


# ── Finance Endpoints ──────────────────────────────────────────────────

@app.get("/api/finance/overview", response_model=FinanceOverview)
def get_finance_overview(db: Session = Depends(get_db)):
    """Financial summary: revenue, expenses, COGS, margins."""
    financials = db.query(FactFinancial).all()

    revenue = sum(f.actual_amount for f in financials if f.account_type == "revenue")
    expenses = sum(f.actual_amount for f in financials if f.account_type == "expense")
    cogs = sum(f.actual_amount for f in financials if f.account_type == "cogs")
    budget_total = sum(f.budget_amount or 0 for f in financials)
    actual_total = sum(f.actual_amount or 0 for f in financials)

    net_income = revenue - expenses - cogs
    net_margin = round(net_income / revenue * 100, 2) if revenue > 0 else 0
    budget_var = round((actual_total - budget_total) / budget_total * 100, 2) if budget_total > 0 else 0

    return FinanceOverview(
        total_revenue=round(revenue, 2),
        total_expenses=round(expenses, 2),
        total_cogs=round(cogs, 2),
        net_income=round(net_income, 2),
        net_margin_pct=net_margin,
        budget_variance_pct=budget_var,
    )


@app.get("/api/finance/variance", response_model=list[DepartmentVariance])
def get_finance_variance(
    account_type: str = None,
    db: Session = Depends(get_db),
):
    """Budget vs actual variance by department."""
    depts = {d.department_key: d.name for d in db.query(DimDepartment).all()}
    query = db.query(FactFinancial)
    if account_type:
        query = query.filter(FactFinancial.account_type == account_type)
    financials = query.all()

    dept_data = defaultdict(lambda: defaultdict(lambda: {"actual": 0, "budget": 0}))
    for f in financials:
        dept_name = depts.get(f.department_key, "Unknown")
        dept_data[dept_name][f.account_type]["actual"] += f.actual_amount or 0
        dept_data[dept_name][f.account_type]["budget"] += f.budget_amount or 0

    results = []
    for dept, acct_types in sorted(dept_data.items()):
        for acct_type, data in acct_types.items():
            variance = round(data["actual"] - data["budget"], 2)
            variance_pct = round(variance / data["budget"] * 100, 2) if data["budget"] != 0 else 0
            results.append(DepartmentVariance(
                department=dept,
                actual=round(data["actual"], 2),
                budget=round(data["budget"], 2),
                variance=variance,
                variance_pct=variance_pct,
                account_type=acct_type,
            ))
    return results


# ── Workforce Endpoints ────────────────────────────────────────────────

@app.get("/api/workforce/overview", response_model=WorkforceOverview)
def get_workforce_overview(db: Session = Depends(get_db)):
    """Workforce summary from latest month snapshot."""
    latest_date = db.query(func.max(FactWorkforce.date_key)).scalar()
    if not latest_date:
        return WorkforceOverview(
            total_headcount=0, new_hires=0, terminations=0,
            turnover_rate=0, total_salary_cost=0, avg_salary=0,
            revenue_per_employee=0,
        )

    workforce = db.query(FactWorkforce).filter(FactWorkforce.date_key == latest_date).all()

    total_hc = sum(w.active_headcount for w in workforce)
    total_hires = sum(w.new_hires for w in workforce)
    total_terms = sum(w.terminations for w in workforce)
    total_salary = sum(w.total_salary_cost for w in workforce)
    turnover = round(total_terms / total_hc * 100, 2) if total_hc > 0 else 0
    avg_salary = round(total_salary / total_hc, 2) if total_hc > 0 else 0

    # revenue per employee
    sales_summary = db.query(func.sum(FactSales.net_amount)).scalar() or 0
    rev_per_emp = round(sales_summary / total_hc, 2) if total_hc > 0 else 0

    return WorkforceOverview(
        total_headcount=total_hc,
        new_hires=total_hires,
        terminations=total_terms,
        turnover_rate=turnover,
        total_salary_cost=round(total_salary, 2),
        avg_salary=avg_salary,
        revenue_per_employee=rev_per_emp,
    )


@app.get("/api/workforce/by-department", response_model=list[DepartmentWorkforce])
def get_workforce_by_department(db: Session = Depends(get_db)):
    """Workforce breakdown by department (latest month)."""
    latest_date = db.query(func.max(FactWorkforce.date_key)).scalar()
    if not latest_date:
        return []

    depts = {d.department_key: d.name for d in db.query(DimDepartment).all()}
    workforce = db.query(FactWorkforce).filter(FactWorkforce.date_key == latest_date).all()

    results = []
    for w in workforce:
        dept_name = depts.get(w.department_key, "Unknown")
        results.append(DepartmentWorkforce(
            department=dept_name,
            headcount=w.active_headcount,
            new_hires=w.new_hires,
            terminations=w.terminations,
            turnover_rate=w.turnover_rate or 0,
            avg_salary=w.avg_salary or 0,
        ))
    return sorted(results, key=lambda x: -x.headcount)


# ── Quality Endpoints ──────────────────────────────────────────────────

@app.get("/api/quality/latest", response_model=list[QualityCheckResult])
def get_quality_latest(db: Session = Depends(get_db)):
    """Get the most recent quality check results."""
    latest_run = db.query(DataQualityLog.run_id).order_by(
        desc(DataQualityLog.run_at)
    ).first()

    if not latest_run:
        return []

    return db.query(DataQualityLog).filter(
        DataQualityLog.run_id == latest_run[0]
    ).order_by(DataQualityLog.id).all()


@app.get("/api/quality/history")
def get_quality_history(db: Session = Depends(get_db)):
    """Quality check summary over past runs."""
    runs = db.query(
        DataQualityLog.run_id,
        func.count().label("total_checks"),
        func.sum(case((DataQualityLog.status == "pass", 1), else_=0)).label("passed"),
        func.sum(case((DataQualityLog.status == "fail", 1), else_=0)).label("failed"),
        func.sum(case((DataQualityLog.status == "warning", 1), else_=0)).label("warnings"),
        func.max(DataQualityLog.run_at).label("run_at"),
    ).group_by(DataQualityLog.run_id).order_by(desc("run_at")).limit(20).all()

    return [
        {
            "run_id": r.run_id,
            "total_checks": r.total_checks,
            "passed": r.passed,
            "failed": r.failed,
            "warnings": r.warnings,
            "run_at": str(r.run_at) if r.run_at else None,
        }
        for r in runs
    ]


# ── Pipeline Endpoints ─────────────────────────────────────────────────

@app.get("/api/pipeline/status", response_model=list[PipelineStatus])
def get_pipeline_status(db: Session = Depends(get_db)):
    """Get the latest pipeline run status."""
    latest_run = db.query(PipelineRunLog.run_id).order_by(
        desc(PipelineRunLog.run_at)
    ).first()

    if not latest_run:
        return []

    return db.query(PipelineRunLog).filter(
        PipelineRunLog.run_id == latest_run[0]
    ).order_by(PipelineRunLog.id).all()


@app.post("/api/pipeline/run")
def trigger_pipeline(db: Session = Depends(get_db)):
    """Trigger a full pipeline run."""
    try:
        base_dir = Path(__file__).resolve().parent.parent.parent
        result = subprocess.run(
            [sys.executable, str(base_dir / "scripts" / "pipeline" / "run_pipeline.py")],
            capture_output=True, text=True, timeout=120,
            cwd=str(base_dir),
        )
        return {
            "status": "completed" if result.returncode == 0 else "failed",
            "output": result.stdout[-2000:] if result.stdout else "",
            "error": result.stderr[-500:] if result.stderr else "",
        }
    except subprocess.TimeoutExpired:
        return {"status": "failed", "error": "Pipeline timed out after 120 seconds"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
