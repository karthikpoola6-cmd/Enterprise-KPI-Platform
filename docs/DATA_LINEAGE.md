# Data Lineage — Source to Target Mapping

This document traces how every field flows from raw source data through staging, dimensions, and into fact tables. This is the "semantic layer" documentation that shows data governance.

---

## Sales Lineage

```
orders_raw.csv                stg_orders              fact_sales
─────────────                ──────────              ──────────
order_id          ──────>    source_order_id  ─────> order_id
customer_name     ──────>    customer_name
customer_email    ──────>    customer_email
product_name      ──────>    product_name
product_sku       ──────>    product_sku      ─┐
category          ──────>    category          │    dim_product
quantity          ──────>    quantity    ──────>│──> product_key ──> fact_sales.product_key
unit_price        ──────>    unit_price  ──────>    quantity
discount_pct      ──────>    discount_pct───────>   unit_price, discount_pct
order_date        ──────>    order_date  ──────>    order_date_key (YYYYMMDD)
ship_date         ──────>    ship_date   ──────>    ship_date_key (YYYYMMDD)
delivery_date     ──────>    delivery_date──────>   fulfillment_days (computed)
status            ──────>    status
region            ──────>    region      ─┐
                              (messy)    │    dim_region
                              │          └──> region_key ──> fact_sales.region_key
                              └── standardize_region()

COMPUTED FIELDS in fact_sales:
  gross_amount    = quantity × unit_price
  discount_amount = gross_amount × discount_pct / 100
  net_amount      = gross_amount - discount_amount
  cost_of_goods   = quantity × dim_product.unit_cost
  gross_profit    = net_amount - cost_of_goods
  fulfillment_days= delivery_date - order_date (in days)
  sla_met         = fulfillment_days <= 5
```

## Customer Lineage

```
customers_raw.csv             stg_customers           dim_customer
─────────────────            ──────────────          ────────────
customer_id       ──────>    source_customer_id ───> source_customer_id
name              ──────>    name              ───> name
email             ──────>    email             ───> email
company           ──────>    company           ───> company
industry          ──────>    industry          ───> industry
segment           ──────>    segment           ───> segment
region            ──────>    region (messy)    ───> region (standardized)
city              ──────>    city              ───> city
state             ──────>    state             ───> state
signup_date       ──────>    signup_date       ───> signup_date (parsed)

COMPUTED: customer_tenure_days = today - signup_date
DEDUP:    by source_customer_id (first occurrence kept)
```

## Financial Lineage

```
financials_raw.csv            stg_financials          fact_financial
──────────────────           ──────────────          ──────────────
account_name      ──────>    account_name     ────> account_name
account_type      ──────>    account_type     ────> account_type
amount            ──────>    amount           ────> actual_amount
fiscal_year       ──────>    fiscal_year      ─┐
fiscal_month      ──────>    fiscal_month     ─┴──> date_key (YYYYMM01)
department        ──────>    department       ────> department_key (via dim_department)
region            ──────>    region (messy)   ────> region_key (via dim_region)
budget_amount     ──────>    budget_amount    ────> budget_amount

COMPUTED:
  variance_amount = actual_amount - budget_amount
  variance_pct    = variance_amount / budget_amount × 100
```

## Workforce Lineage

```
employees_raw.csv             stg_employees           dim_employee
─────────────────            ─────────────           ────────────
employee_id       ──────>    employee_id      ────> employee_id
name              ──────>    name             ────> name
department        ──────>    department       ────> department_key (via dim_department)
title             ──────>    title            ────> title
hire_date         ──────>    hire_date        ────> hire_date (parsed)
termination_date  ──────>    termination_date ────> termination_date (parsed)
salary            ──────>    salary           ────> salary
region            ──────>    region (messy)   ────> region_key (via dim_region)
status            ──────>    status           ────> is_active (derived)

dim_employee ──> fact_workforce (monthly snapshots):
  active_headcount  = COUNT(employees active in month)
  new_hires         = COUNT(employees hired in month)
  terminations      = COUNT(employees terminated in month)
  total_salary_cost = SUM(salary) of active employees
  avg_salary        = total_salary_cost / active_headcount
  turnover_rate     = terminations / active_headcount × 100
```

## Transformation Rules

| Rule | Applied In | Description |
|------|-----------|-------------|
| Region Standardization | transform_dimensions | Maps messy region names to 4 clean values |
| Date Parsing | transform_dimensions | Handles 5 date formats (YYYY-MM-DD, MM/DD/YYYY, etc.) |
| Boolean Normalization | transform_dimensions | Maps "Yes"/"1"/"TRUE"/"Active" → True |
| Deduplication | transform_dimensions | By source ID for customers, SKU for products, employee ID |
| Margin Calculation | transform_dimensions | (list_price - unit_cost) / list_price × 100 |
| SLA Computation | transform_facts | fulfillment_days <= 5 → sla_met = True |
| Variance | transform_facts | actual - budget, (actual - budget) / budget × 100 |
