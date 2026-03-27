from sqlalchemy import Column, Integer, String, Float, Boolean, DateTime, Date, ForeignKey, Text
from sqlalchemy.sql import func
from .database import Base


# ── Staging Layer (raw ingested data from source systems) ──────────────


class StgOrder(Base):
    __tablename__ = "stg_orders"

    id = Column(Integer, primary_key=True)
    source_order_id = Column(String)
    customer_name = Column(String)
    customer_email = Column(String)
    product_name = Column(String)
    product_sku = Column(String)
    category = Column(String)
    quantity = Column(Integer)
    unit_price = Column(Float)
    discount_pct = Column(Float)
    order_date = Column(String)       # kept as string — raw data has inconsistent formats
    ship_date = Column(String)
    delivery_date = Column(String)
    status = Column(String)
    region = Column(String)
    sales_rep = Column(String)
    source_system = Column(String, default="salesforce_export")
    loaded_at = Column(DateTime, server_default=func.now())


class StgCustomer(Base):
    __tablename__ = "stg_customers"

    id = Column(Integer, primary_key=True)
    source_customer_id = Column(String)
    name = Column(String)
    email = Column(String)
    phone = Column(String)
    company = Column(String)
    industry = Column(String)
    segment = Column(String)
    region = Column(String)
    city = Column(String)
    state = Column(String)
    signup_date = Column(String)
    last_order_date = Column(String)
    source_system = Column(String, default="salesforce_export")
    loaded_at = Column(DateTime, server_default=func.now())


class StgProduct(Base):
    __tablename__ = "stg_products"

    id = Column(Integer, primary_key=True)
    source_product_id = Column(String)
    name = Column(String)
    sku = Column(String)
    category = Column(String)
    subcategory = Column(String)
    unit_cost = Column(Float)
    list_price = Column(Float)
    supplier = Column(String)
    is_active = Column(String)        # raw data has "Yes"/"No"/1/0 inconsistencies
    source_system = Column(String, default="warehouse_export")
    loaded_at = Column(DateTime, server_default=func.now())


class StgFinancial(Base):
    __tablename__ = "stg_financials"

    id = Column(Integer, primary_key=True)
    account_name = Column(String)
    account_type = Column(String)     # revenue, expense, cogs
    amount = Column(Float)
    fiscal_year = Column(Integer)
    fiscal_quarter = Column(String)
    fiscal_month = Column(Integer)
    department = Column(String)
    region = Column(String)
    budget_amount = Column(Float)
    is_budget = Column(String)        # raw data has "Y"/"N"/True/False inconsistencies
    source_system = Column(String, default="quickbooks_export")
    loaded_at = Column(DateTime, server_default=func.now())


class StgEmployee(Base):
    __tablename__ = "stg_employees"

    id = Column(Integer, primary_key=True)
    employee_id = Column(String)
    name = Column(String)
    department = Column(String)
    title = Column(String)
    hire_date = Column(String)
    termination_date = Column(String)
    salary = Column(Float)
    region = Column(String)
    manager = Column(String)
    status = Column(String)           # Active, Terminated, On Leave
    source_system = Column(String, default="adp_export")
    loaded_at = Column(DateTime, server_default=func.now())


# ── Dimension Layer (cleaned, conformed) ───────────────────────────────


class DimDate(Base):
    __tablename__ = "dim_date"

    date_key = Column(Integer, primary_key=True)   # YYYYMMDD format
    full_date = Column(Date, unique=True)
    year = Column(Integer)
    quarter = Column(Integer)
    month = Column(Integer)
    month_name = Column(String)
    week = Column(Integer)
    day_of_week = Column(Integer)
    day_name = Column(String)
    fiscal_year = Column(Integer)
    fiscal_quarter = Column(String)    # e.g., "FY2025-Q1"
    is_weekend = Column(Boolean)
    is_month_end = Column(Boolean)
    is_quarter_end = Column(Boolean)


class DimCustomer(Base):
    __tablename__ = "dim_customer"

    customer_key = Column(Integer, primary_key=True, autoincrement=True)
    source_customer_id = Column(String, unique=True)
    name = Column(String)
    email = Column(String)
    company = Column(String)
    industry = Column(String)
    segment = Column(String)           # Enterprise, Mid-Market, SMB
    region = Column(String)
    city = Column(String)
    state = Column(String)
    signup_date = Column(Date)
    customer_tenure_days = Column(Integer)
    is_active = Column(Boolean, default=True)


class DimProduct(Base):
    __tablename__ = "dim_product"

    product_key = Column(Integer, primary_key=True, autoincrement=True)
    source_product_id = Column(String, unique=True)
    name = Column(String)
    sku = Column(String, unique=True)
    category = Column(String)
    subcategory = Column(String)
    unit_cost = Column(Float)
    list_price = Column(Float)
    margin_pct = Column(Float)         # computed: (list_price - unit_cost) / list_price
    supplier = Column(String)
    is_active = Column(Boolean, default=True)


class DimDepartment(Base):
    __tablename__ = "dim_department"

    department_key = Column(Integer, primary_key=True, autoincrement=True)
    name = Column(String, unique=True)
    cost_center = Column(String)
    division = Column(String)
    headcount_budget = Column(Integer)


class DimRegion(Base):
    __tablename__ = "dim_region"

    region_key = Column(Integer, primary_key=True, autoincrement=True)
    name = Column(String, unique=True)
    city = Column(String)
    state = Column(String)
    timezone = Column(String)
    manager = Column(String)


class DimEmployee(Base):
    __tablename__ = "dim_employee"

    employee_key = Column(Integer, primary_key=True, autoincrement=True)
    employee_id = Column(String, unique=True)
    name = Column(String)
    department_key = Column(Integer, ForeignKey("dim_department.department_key"))
    title = Column(String)
    hire_date = Column(Date)
    termination_date = Column(Date, nullable=True)
    salary = Column(Float)
    region_key = Column(Integer, ForeignKey("dim_region.region_key"))
    is_active = Column(Boolean, default=True)


# ── Fact Layer (measures, aggregated) ──────────────────────────────────


class FactSales(Base):
    __tablename__ = "fact_sales"

    sale_key = Column(Integer, primary_key=True, autoincrement=True)
    order_date_key = Column(Integer, ForeignKey("dim_date.date_key"))
    ship_date_key = Column(Integer, ForeignKey("dim_date.date_key"))
    customer_key = Column(Integer, ForeignKey("dim_customer.customer_key"))
    product_key = Column(Integer, ForeignKey("dim_product.product_key"))
    region_key = Column(Integer, ForeignKey("dim_region.region_key"))
    order_id = Column(String)
    quantity = Column(Integer)
    unit_price = Column(Float)
    discount_pct = Column(Float)
    gross_amount = Column(Float)       # quantity * unit_price
    discount_amount = Column(Float)    # gross_amount * discount_pct / 100
    net_amount = Column(Float)         # gross_amount - discount_amount
    cost_of_goods = Column(Float)      # quantity * product.unit_cost
    gross_profit = Column(Float)       # net_amount - cost_of_goods
    fulfillment_days = Column(Integer) # delivery_date - order_date
    sla_met = Column(Boolean)          # fulfillment_days <= 5


class FactFinancial(Base):
    __tablename__ = "fact_financial"

    financial_key = Column(Integer, primary_key=True, autoincrement=True)
    date_key = Column(Integer, ForeignKey("dim_date.date_key"))
    department_key = Column(Integer, ForeignKey("dim_department.department_key"))
    region_key = Column(Integer, ForeignKey("dim_region.region_key"))
    account_name = Column(String)
    account_type = Column(String)      # revenue, expense, cogs
    actual_amount = Column(Float)
    budget_amount = Column(Float)
    variance_amount = Column(Float)    # actual - budget
    variance_pct = Column(Float)       # variance / budget * 100


class FactWorkforce(Base):
    __tablename__ = "fact_workforce"

    workforce_key = Column(Integer, primary_key=True, autoincrement=True)
    date_key = Column(Integer, ForeignKey("dim_date.date_key"))
    department_key = Column(Integer, ForeignKey("dim_department.department_key"))
    region_key = Column(Integer, ForeignKey("dim_region.region_key"))
    active_headcount = Column(Integer)
    new_hires = Column(Integer)
    terminations = Column(Integer)
    total_salary_cost = Column(Float)
    avg_salary = Column(Float)
    turnover_rate = Column(Float)      # terminations / active_headcount


class FactKPISnapshot(Base):
    __tablename__ = "fact_kpi_snapshot"

    snapshot_key = Column(Integer, primary_key=True, autoincrement=True)
    date_key = Column(Integer, ForeignKey("dim_date.date_key"))
    kpi_name = Column(String)
    kpi_value = Column(Float)
    kpi_unit = Column(String)          # %, $, count, days
    dimension = Column(String)         # overall, region, department, product_category
    dimension_value = Column(String)   # "Dallas-Fort Worth", "Sales", "Electronics", etc.
    period = Column(String)            # monthly, quarterly


# ── Logging / Monitoring ───────────────────────────────────────────────


class DataQualityLog(Base):
    __tablename__ = "data_quality_log"

    id = Column(Integer, primary_key=True)
    run_id = Column(String)
    check_name = Column(String)
    table_name = Column(String)
    layer = Column(String)             # staging, dimension, fact
    status = Column(String)            # pass, fail, warning
    records_checked = Column(Integer)
    records_failed = Column(Integer)
    failure_rate = Column(Float)
    details = Column(Text)
    run_at = Column(DateTime, server_default=func.now())


class PipelineRunLog(Base):
    __tablename__ = "pipeline_run_log"

    id = Column(Integer, primary_key=True)
    run_id = Column(String)
    step_name = Column(String)
    status = Column(String)            # started, completed, failed
    records_processed = Column(Integer, default=0)
    duration_seconds = Column(Float)
    error_message = Column(Text, nullable=True)
    run_at = Column(DateTime, server_default=func.now())
