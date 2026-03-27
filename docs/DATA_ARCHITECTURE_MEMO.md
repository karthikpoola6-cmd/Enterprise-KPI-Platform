# Data Architecture Memo — RISE Inc. Enterprise KPI Platform

**Prepared by:** Karthik Poola, Data & Analytics Intern
**Date:** March 2026
**Client:** RISE Inc. Executive Team

---

## 1. Background

RISE Inc. is a mid-size professional services and distribution company (~500 employees) operating across the Dallas-Fort Worth metroplex with regional offices in Houston, Austin, and San Antonio. Over the past 3 years, the company grew through acquisition, leaving operational data fragmented across four siloed systems.

## 2. Current State Assessment

### Source Systems

| System | Data | Format | Refresh |
|--------|------|--------|---------|
| Salesforce | Orders, Customers, Pipeline | CSV export | Weekly |
| Warehouse Mgmt | Products, Inventory, Fulfillment | Spreadsheets | Daily |
| QuickBooks | Revenue, Expenses, Budgets | CSV export | Monthly |
| ADP | Headcount, Payroll, Turnover | CSV export | Bi-weekly |

### Pain Points

1. **Data Fragmentation** — No single source of truth for KPIs. Leadership relies on manually assembled spreadsheets to answer basic questions.
2. **Inconsistent Definitions** — Different teams calculate "revenue" and "margin" differently. No governed metric definitions.
3. **Manual Reporting** — The finance team spends 60+ hours per month building Excel reports that are outdated by the time they are reviewed.
4. **Data Quality Issues** — Duplicate records, inconsistent region names, mixed date formats, and missing values across source systems.
5. **No Historical Tracking** — KPI values are recalculated from scratch each period with no trend visibility.

## 3. Proposed Architecture

### Three-Layer Data Warehouse (Kimball Methodology)

```
Source Systems          Staging Layer         Dimension Layer        Fact Layer
┌──────────────┐      ┌──────────────┐      ┌──────────────┐      ┌──────────────┐
│  Salesforce   │─────>│  stg_orders  │      │  dim_date    │      │ fact_sales   │
│  (CRM)       │─────>│  stg_customers│─────>│  dim_customer│─────>│ fact_financial│
├──────────────┤      ├──────────────┤      │  dim_product │      │ fact_workforce│
│  Warehouse   │─────>│  stg_products │─────>│  dim_region  │─────>│ fact_kpi_    │
│  Management  │      ├──────────────┤      │  dim_department│     │   snapshot   │
├──────────────┤      │  stg_financials│────>│  dim_employee│      └──────────────┘
│  QuickBooks  │─────>├──────────────┤      └──────────────┘
│  (Finance)   │      │  stg_employees│
├──────────────┤      └──────────────┘
│  ADP         │─────>
│  (HR)        │
└──────────────┘
```

### Layer Descriptions

**Staging (stg_)** — Raw data loaded as-is from source CSVs. Minimal schema enforcement. Preserves original formats, including inconsistencies, for audit purposes. Full refresh each pipeline run.

**Dimensions (dim_)** — Cleaned, conformed, and deduplicated reference data. Surrogate keys assigned. Region names standardized. Dates normalized. Boolean values unified.

**Facts (fact_)** — Computed measures joined to dimension keys. Includes calculated fields: gross_amount, discount_amount, net_amount, cost_of_goods, gross_profit, fulfillment_days, SLA compliance, budget variance.

## 4. Data Quality Framework

16 automated checks organized by category:

- **Completeness** — No nulls in required fields (order_id, customer_key, amounts, names)
- **Uniqueness** — No duplicate keys (order_id, customer_id, SKU, date_key)
- **Referential Integrity** — All foreign keys resolve to valid dimension records
- **Range/Reasonableness** — Values within expected bounds (prices > 0, salaries in range)

All results logged to `data_quality_log` for historical tracking.

## 5. Expected Outcomes

- **Eliminate 60+ hours/month** of manual report preparation
- **Single source of truth** for all KPIs across business units
- **Real-time dashboards** accessible to leadership without manual data pulls
- **Governed metric definitions** documented and version-controlled
- **Data quality visibility** with automated monitoring and alerting
