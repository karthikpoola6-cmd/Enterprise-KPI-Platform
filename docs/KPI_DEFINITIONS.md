# KPI Definitions — Semantic Metrics Layer

This document defines every KPI tracked by the Enterprise KPI Platform. Each KPI has a governed definition, formula, data source, and business meaning.

---

## Sales KPIs

### Total Revenue
- **Formula:** `SUM(fact_sales.net_amount)`
- **Unit:** $
- **Grain:** Monthly, by region, by product category
- **Business Meaning:** Total net revenue after discounts. Primary top-line metric.

### Gross Margin %
- **Formula:** `SUM(fact_sales.gross_profit) / SUM(fact_sales.net_amount) × 100`
- **Unit:** %
- **Grain:** Monthly, by region, by product category
- **Business Meaning:** Percentage of revenue retained after cost of goods. Target: >40%.

### Order Count
- **Formula:** `COUNT(fact_sales.order_id)`
- **Unit:** count
- **Grain:** Monthly
- **Business Meaning:** Total number of completed orders. Volume indicator.

### Average Order Value
- **Formula:** `SUM(fact_sales.net_amount) / COUNT(fact_sales.order_id)`
- **Unit:** $
- **Grain:** Monthly, by segment
- **Business Meaning:** Revenue per order. Indicates pricing and deal size trends.

### Fulfillment Rate (SLA Compliance)
- **Formula:** `COUNT(fact_sales WHERE sla_met = true) / COUNT(fact_sales) × 100`
- **Unit:** %
- **Grain:** Monthly
- **Business Meaning:** Percentage of orders delivered within 5 business days. Target: >80%.

---

## Financial KPIs

### Net Income
- **Formula:** `Revenue - Expenses - COGS`
- **Source:** `SUM(fact_financial.actual_amount)` grouped by account_type
- **Unit:** $
- **Business Meaning:** Bottom-line profitability after all costs.

### Net Margin %
- **Formula:** `Net Income / Total Revenue × 100`
- **Unit:** %
- **Business Meaning:** Percentage of revenue that becomes profit. Target: >10%.

### Budget Variance %
- **Formula:** `(SUM(actual_amount) - SUM(budget_amount)) / SUM(budget_amount) × 100`
- **Unit:** %
- **Grain:** By department, by account type
- **Business Meaning:** How much actual spending deviates from plan. Acceptable range: ±5%.

---

## Workforce KPIs

### Headcount
- **Formula:** `SUM(fact_workforce.active_headcount)`
- **Unit:** count
- **Grain:** Monthly, by department
- **Business Meaning:** Total active employees at month end.

### Employee Turnover Rate
- **Formula:** `SUM(fact_workforce.terminations) / SUM(fact_workforce.active_headcount) × 100`
- **Unit:** %
- **Grain:** Monthly, by department
- **Business Meaning:** Rate of employee departures. Target: <10% annualized.

### Revenue per Employee
- **Formula:** `Total Revenue / Total Headcount`
- **Unit:** $
- **Grain:** Monthly
- **Business Meaning:** Productivity metric. Higher is better.

### New Hires
- **Formula:** `SUM(fact_workforce.new_hires)`
- **Unit:** count
- **Grain:** Monthly
- **Business Meaning:** Hiring velocity. Tracked against headcount budget.

---

## Data Quality KPIs

### Quality Score
- **Formula:** `Checks Passed / Total Checks × 100`
- **Unit:** %
- **Business Meaning:** Overall data governance health. Target: >90%.

### Data Freshness
- **Formula:** `MAX(loaded_at) in staging tables`
- **Unit:** timestamp
- **Business Meaning:** How recently source data was ingested. Target: within 24 hours.
