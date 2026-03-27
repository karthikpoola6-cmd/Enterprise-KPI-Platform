from pydantic import BaseModel
from typing import Optional
from datetime import date, datetime


# ── KPI Schemas ────────────────────────────────────────────────────────

class KPIValue(BaseModel):
    kpi_name: str
    kpi_value: float
    kpi_unit: str
    dimension: str
    dimension_value: str

    class Config:
        from_attributes = True


class KPITrend(BaseModel):
    date_key: int
    month: str
    kpi_name: str
    kpi_value: float
    kpi_unit: str


# ── Sales Schemas ──────────────────────────────────────────────────────

class SalesSummary(BaseModel):
    total_revenue: float
    total_orders: int
    avg_order_value: float
    total_discount: float
    gross_profit: float
    gross_margin_pct: float
    fulfillment_rate: float


class RegionSales(BaseModel):
    region: str
    revenue: float
    orders: int
    gross_profit: float
    margin_pct: float


class ProductSales(BaseModel):
    category: str
    revenue: float
    orders: int
    gross_profit: float
    margin_pct: float


class SegmentSales(BaseModel):
    segment: str
    revenue: float
    orders: int
    avg_order_value: float


# ── Finance Schemas ────────────────────────────────────────────────────

class FinanceOverview(BaseModel):
    total_revenue: float
    total_expenses: float
    total_cogs: float
    net_income: float
    net_margin_pct: float
    budget_variance_pct: float


class DepartmentVariance(BaseModel):
    department: str
    actual: float
    budget: float
    variance: float
    variance_pct: float
    account_type: str


# ── Workforce Schemas ──────────────────────────────────────────────────

class WorkforceOverview(BaseModel):
    total_headcount: int
    new_hires: int
    terminations: int
    turnover_rate: float
    total_salary_cost: float
    avg_salary: float
    revenue_per_employee: float


class DepartmentWorkforce(BaseModel):
    department: str
    headcount: int
    new_hires: int
    terminations: int
    turnover_rate: float
    avg_salary: float


# ── Quality Schemas ────────────────────────────────────────────────────

class QualityCheckResult(BaseModel):
    check_name: str
    table_name: str
    layer: str
    status: str
    records_checked: int
    records_failed: int
    failure_rate: float
    details: Optional[str] = None
    run_at: Optional[datetime] = None

    class Config:
        from_attributes = True


class PipelineStatus(BaseModel):
    run_id: str
    step_name: str
    status: str
    records_processed: int
    duration_seconds: float
    run_at: Optional[datetime] = None

    class Config:
        from_attributes = True
