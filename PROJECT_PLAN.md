# Project 3: Enterprise KPI Data Platform — Strategic Plan

## Why This Project

RISE Inc.'s first two projects (Operations Command Center, Government Case Triage) proved Karthik can build full-stack operational systems. But a gap remained: **data engineering credibility**. Employers hiring for analytics, BI, and deployment roles want to see that a candidate can:

- Build proper ETL pipelines (not just CRUD apps)
- Design dimensional data models (star schema / Kimball methodology)
- Implement data quality checks and governance
- Produce executive-grade dashboards from transformed warehouse data

This project fills that gap.

## RISE Inc. Context

RISE Inc. grew through acquisition over 3 years, leaving operational data fragmented across 4 siloed systems:

- **Salesforce** — orders, customers, pipeline
- **Warehouse Management** — products, inventory, fulfillment
- **QuickBooks** — revenue, expenses, budgets
- **ADP** — headcount, payroll, turnover

Leadership spent **60+ hours per month** manually assembling reports from spreadsheets. The CFO could not answer basic questions like "What is our margin by product line?" or "Which region has the highest customer churn?"

**Karthik was asked to build a single source of truth** — an enterprise analytics platform that unifies all data, transforms it into a governed warehouse, and delivers executive dashboards.

## Core Features

1. **3-Layer Data Warehouse** — Staging → Dimensions → Facts (Kimball methodology)
2. **Automated ETL Pipeline** — 4-phase Python pipeline: Extract, Transform Dimensions, Transform Facts, Quality Checks
3. **17 Database Tables** — 5 staging, 6 dimensions, 4 facts, 2 logging
4. **Data Quality Monitoring** — 16 automated checks (completeness, uniqueness, referential integrity, range)
5. **React Executive Dashboard** — 5 pages: Executive, Sales, Finance, Workforce, Data Quality
6. **Power BI Dashboards** — 5 dashboards with DAX measures
7. **Executive KPI Report** — Auto-generated plain-text report for leadership
8. **Consulting Documentation** — Architecture memo, data lineage, ETL design, KPI definitions

## Power BI Dashboards

1. **Executive KPI Overview** — Revenue, margin, growth, fulfillment, headcount
2. **Sales Performance Deep Dive** — Revenue by region/product/segment, top customers
3. **Financial Health & Budget Variance** — Budget vs actual, variance by department, margins
4. **Workforce Analytics** — Headcount trends, turnover, salary costs, revenue per employee
5. **Data Quality Scorecard** — Pass/fail/warning breakdown, quality trends

## DAX Measures

```dax
Gross Margin % = DIVIDE(SUM(fact_sales[gross_profit]), SUM(fact_sales[net_amount]), 0) * 100

Revenue Growth QoQ =
VAR CurrentQ = SUM(fact_sales[net_amount])
VAR PrevQ = CALCULATE(SUM(fact_sales[net_amount]), DATEADD(dim_date[full_date], -3, MONTH))
RETURN DIVIDE(CurrentQ - PrevQ, PrevQ, 0) * 100

Fulfillment Rate = DIVIDE(COUNTROWS(FILTER(fact_sales, fact_sales[sla_met] = TRUE)), COUNTROWS(fact_sales), 0) * 100

Employee Turnover = DIVIDE(SUM(fact_workforce[terminations]), AVERAGE(fact_workforce[active_headcount]), 0) * 100

Budget Variance % = DIVIDE(SUM(fact_financial[variance_amount]), SUM(fact_financial[budget_amount]), 0) * 100
```

## GitHub Deliverables

A recruiter visiting this repo sees:

- [ ] **Working full-stack application** — React dashboard + FastAPI API
- [ ] **ETL pipeline** — 4-phase pipeline with orchestrator
- [ ] **Data warehouse** — Dimensional model with staging, dimensions, facts
- [ ] **Data quality checks** — 16 automated validations with logging
- [ ] **Power BI dashboards** — 5 dashboards with screenshots
- [ ] **Consulting documentation** — Architecture memo, lineage, pipeline design, KPI definitions
- [ ] **Executive report** — Auto-generated leadership summary
- [ ] **Clean README** — Problem, solution, architecture, how to run

## Resume Bullets

- Built an enterprise analytics platform integrating data from 4 source systems (CRM, warehouse, finance, HR) into a governed dimensional data warehouse with automated ETL pipelines
- Designed a 3-layer data architecture (staging → dimensions → facts) following Kimball methodology, with 16 automated data quality checks achieving 94%+ pass rate
- Developed 5 Power BI dashboards with custom DAX measures tracking revenue, margins, budget variance, workforce metrics, and data quality across business units
- Created a React executive dashboard with real-time KPI visualization, enabling leadership to monitor operational performance without manual report assembly
