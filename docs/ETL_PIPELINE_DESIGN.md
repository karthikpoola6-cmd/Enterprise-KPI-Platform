# ETL Pipeline Design

## Pipeline Overview

```
┌─────────────────────────────────────────────────────────────────┐
│                    Pipeline Orchestrator                         │
│                     (run_pipeline.py)                            │
│                                                                  │
│  Phase 1          Phase 2            Phase 3          Phase 4    │
│  ┌──────────┐    ┌──────────────┐   ┌────────────┐  ┌────────┐ │
│  │ Extract  │───>│  Transform   │──>│ Transform  │─>│Quality │ │
│  │ to       │    │  Dimensions  │   │ Facts      │  │Checks  │ │
│  │ Staging  │    │              │   │            │  │        │ │
│  └──────────┘    └──────────────┘   └────────────┘  └────────┘ │
│       │                │                  │              │       │
│   Raw CSVs        Staging → Dims    Staging+Dims     Validates  │
│   → stg_*         → dim_*          → fact_*          all layers │
│                                                                  │
│  ┌──────────────────────────────────────────────────────────┐   │
│  │              pipeline_run_log (timing + status)          │   │
│  └──────────────────────────────────────────────────────────┘   │
└─────────────────────────────────────────────────────────────────┘
```

## Phase 1: Extract & Load to Staging

**Script:** `scripts/pipeline/extract_to_staging.py`

| Source File | Target Table | Records |
|-------------|-------------|---------|
| orders_raw.csv | stg_orders | ~2,040 |
| customers_raw.csv | stg_customers | ~306 |
| products_raw.csv | stg_products | ~40 |
| financials_raw.csv | stg_financials | ~1,383 |
| employees_raw.csv | stg_employees | ~150 |

**Pattern:** Full refresh (truncate + load). Adds `source_system` and `loaded_at` metadata.

## Phase 2: Transform Dimensions

**Script:** `scripts/pipeline/transform_dimensions.py`

**Transformations applied:**
- `dim_date` — Date spine generated (2024-2026), fiscal year/quarter computed
- `dim_customer` — Deduplicated by ID, region names standardized, tenure computed
- `dim_product` — Deduplicated by SKU, margin_pct computed, boolean normalized
- `dim_department` — Loaded from reference data with cost centers
- `dim_region` — Loaded from reference data with managers
- `dim_employee` — Deduplicated, FK lookups for department and region

**Region Standardization Map:**
```
"DFW", "Dallas", "dallas-fort worth" → "Dallas-Fort Worth"
"houston", "Houston, TX"              → "Houston"
"austin", "Austin, TX"                → "Austin"
"san antonio", "San Antonio, TX"      → "San Antonio"
```

## Phase 3: Transform Facts

**Script:** `scripts/pipeline/transform_facts.py`

- `fact_sales` — Joins orders to dimension keys. Computes: gross_amount, discount_amount, net_amount, cost_of_goods, gross_profit, fulfillment_days, sla_met
- `fact_financial` — Joins financials to department/region keys. Computes: variance_amount, variance_pct
- `fact_workforce` — Monthly snapshots from employee dimension. Computes: headcount, new_hires, terminations, turnover_rate
- `fact_kpi_snapshot` — Rolled-up KPI values by month for trend tracking

## Phase 4: Data Quality Checks

**Script:** `scripts/pipeline/run_quality_checks.py`

16 checks across 4 categories. All results logged to `data_quality_log`.

## Error Handling

- Each phase wrapped in try/except with status logging
- Pipeline stops on phase failure (fail-fast)
- Partial runs leave staging intact for debugging
- Run ID tracks all steps and quality checks for a single execution
