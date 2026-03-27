# Enterprise KPI Data Platform

> **Note:** All data in this project is **simulated sample data** generated for demonstration purposes. No real client, financial, employee, or customer data is used. The data generator intentionally includes real-world quality issues (duplicates, nulls, inconsistent formats) to showcase ETL pipeline capabilities.

## Problem

RISE Inc. is a mid-size professional services and distribution company operating across Texas (Dallas-Fort Worth, Houston, Austin, San Antonio). After 3 years of growth through acquisition, operational data was fragmented across 4 siloed systems:

- **Salesforce** — orders, customers, pipeline
- **Warehouse Management** — products, inventory, fulfillment
- **QuickBooks** — revenue, expenses, budgets
- **ADP** — headcount, payroll, turnover

Leadership spent **60+ hours per month** manually assembling reports from spreadsheets. No single source of truth existed for key metrics like revenue, margins, or employee turnover.

## Solution

An enterprise analytics platform that:

1. **Ingests raw data** from 4 source systems (CSV exports with real-world quality issues)
2. **Transforms through a 3-layer warehouse** — Staging → Dimensions → Facts (Kimball methodology)
3. **Runs 16 automated data quality checks** — completeness, uniqueness, referential integrity, range validation
4. **Serves a React executive dashboard** — 5 pages covering sales, finance, workforce, and data quality
5. **Exports to Power BI** — 8 CSV files optimized for 5 Power BI dashboards
6. **Generates executive reports** — Plain-text KPI summaries for leadership review

## Tech Stack

| Layer | Technology |
|-------|-----------|
| Backend | Python, FastAPI, SQLAlchemy |
| Frontend | React, Recharts |
| Database | SQLite (3-layer warehouse) |
| ETL Pipeline | Python scripts (4-phase pipeline) |
| Analytics | Power BI, DAX |
| Data Export | CSV (8 export files) |

## Key Features

- **Dimensional Data Model** — 17 tables: 5 staging, 6 dimensions, 4 facts, 2 logging
- **ETL Pipeline** — Automated 4-phase pipeline with orchestrator and timing logs
- **Data Quality Monitoring** — 16 automated checks with historical tracking
- **Executive Dashboard** — Revenue trends, margin analysis, workforce insights
- **Power BI Integration** — 8 CSV exports with denormalized data for dashboards
- **Consulting Documentation** — Architecture memo, data lineage, KPI definitions, pipeline design

## Project Structure

```
enterprise-kpi-platform/
├── backend/
│   └── app/
│       ├── main.py              # FastAPI (14 endpoints)
│       ├── models.py            # 17 SQLAlchemy models
│       ├── schemas.py           # Pydantic response schemas
│       └── database.py          # SQLAlchemy setup
├── frontend/
│   └── src/
│       ├── App.js               # Sidebar nav (5 pages)
│       ├── api.js               # Axios API client
│       └── components/
│           ├── ExecutiveDashboard.js
│           ├── SalesAnalytics.js
│           ├── FinancialPerformance.js
│           ├── WorkforceInsights.js
│           ├── DataQualityMonitor.js
│           ├── KPICard.js
│           ├── TrendChart.js
│           ├── BarChart.js
│           └── DataTable.js
├── scripts/
│   ├── generate_raw_data.py     # Raw data generator (messy data)
│   ├── export_for_powerbi.py    # CSV exporter (8 files)
│   ├── generate_kpi_report.py   # Executive report generator
│   └── pipeline/
│       ├── run_pipeline.py      # Orchestrator
│       ├── extract_to_staging.py
│       ├── transform_dimensions.py
│       ├── transform_facts.py
│       └── run_quality_checks.py
├── data/
│   ├── raw/                     # Source CSVs (generated)
│   └── exports/                 # Power BI CSVs (exported)
├── docs/
│   ├── DATA_ARCHITECTURE_MEMO.md
│   ├── DATA_LINEAGE.md
│   ├── ETL_PIPELINE_DESIGN.md
│   └── KPI_DEFINITIONS.md
├── PROJECT_PLAN.md
└── README.md
```

## API Endpoints

| Method | Endpoint | Description |
|--------|----------|-------------|
| GET | `/api/kpis` | Current KPI values (latest month) |
| GET | `/api/kpis/trends` | KPI values over time |
| GET | `/api/sales/summary` | Sales summary with optional filters |
| GET | `/api/sales/by-region` | Revenue by region |
| GET | `/api/sales/by-product` | Revenue by product category |
| GET | `/api/sales/by-customer-segment` | Revenue by customer segment |
| GET | `/api/finance/overview` | Financial summary (revenue, expenses, margins) |
| GET | `/api/finance/variance` | Budget vs actual by department |
| GET | `/api/workforce/overview` | Workforce summary (headcount, turnover) |
| GET | `/api/workforce/by-department` | Department-level workforce metrics |
| GET | `/api/quality/latest` | Latest data quality check results |
| GET | `/api/quality/history` | Quality check history across runs |
| GET | `/api/pipeline/status` | Latest pipeline run status |
| POST | `/api/pipeline/run` | Trigger a full pipeline run |

## DAX Measures (Power BI)

```dax
Gross Margin % =
DIVIDE(
    SUM(fact_sales[gross_profit]),
    SUM(fact_sales[net_amount]),
    0
) * 100

Revenue Growth QoQ =
VAR CurrentQ = SUM(fact_sales[net_amount])
VAR PrevQ = CALCULATE(
    SUM(fact_sales[net_amount]),
    DATEADD(dim_date[full_date], -3, MONTH)
)
RETURN DIVIDE(CurrentQ - PrevQ, PrevQ, 0) * 100

Fulfillment Rate =
DIVIDE(
    COUNTROWS(FILTER(fact_sales, fact_sales[sla_met] = TRUE)),
    COUNTROWS(fact_sales),
    0
) * 100

Employee Turnover Rate =
DIVIDE(
    SUM(fact_workforce[terminations]),
    AVERAGE(fact_workforce[active_headcount]),
    0
) * 100

Budget Variance % =
DIVIDE(
    SUM(fact_financial[variance_amount]),
    SUM(fact_financial[budget_amount]),
    0
) * 100
```

## How to Run

### 1. Backend Setup
```bash
cd enterprise-kpi-platform
python3 -m venv venv
source venv/bin/activate
pip install -r backend/requirements.txt
```

### 2. Generate Data & Run Pipeline
```bash
python scripts/generate_raw_data.py
python scripts/pipeline/run_pipeline.py
```

### 3. Start API Server
```bash
uvicorn backend.app.main:app --reload
# API docs at http://localhost:8000/docs
```

### 4. Start Frontend
```bash
cd frontend
npm install
npm start
# Dashboard at http://localhost:3000
```

### 5. Power BI Export
```bash
python scripts/export_for_powerbi.py
# CSVs saved to data/exports/
```

### 6. Executive Report
```bash
python scripts/generate_kpi_report.py
# Report saved to data/executive_kpi_report.txt
```

## Sample Data

The data generator creates realistic enterprise data with intentional quality issues:

| Source | Records | Issues |
|--------|---------|--------|
| Orders | ~2,040 | ~2% duplicates, ~3% null values, 5 date formats |
| Customers | ~306 | ~2% duplicates, missing emails/phones |
| Products | 40 | Inconsistent boolean values (Yes/1/TRUE) |
| Financials | ~1,383 | Inconsistent region names, mixed budget flags |
| Employees | 150 | Mixed date formats, messy region names |

## Architecture

```
    ┌─────────────┐     ┌─────────────┐     ┌─────────────┐
    │  Source CSVs │────>│  Staging    │────>│ Dimensions  │
    │  (4 systems) │     │  (stg_*)    │     │ (dim_*)     │
    └─────────────┘     └─────────────┘     └──────┬──────┘
                                                    │
                         ┌──────────────────────────┘
                         │
                    ┌────┴────┐     ┌─────────────┐
                    │  Facts  │────>│  Dashboard   │
                    │ (fact_*)│     │  (React)     │
                    └────┬────┘     └─────────────┘
                         │
                    ┌────┴────┐     ┌─────────────┐
                    │ Quality │     │  Power BI   │
                    │ Checks  │     │ (CSV export) │
                    └─────────┘     └─────────────┘
```
