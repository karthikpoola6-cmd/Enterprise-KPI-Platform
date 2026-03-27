"""
Pipeline Orchestrator
======================
Runs all ETL phases in sequence:
  Phase 1: Extract raw CSVs → staging tables
  Phase 2: Transform staging → dimension tables
  Phase 3: Transform staging + dims → fact tables
  Phase 4: Run data quality checks

Logs timing and record counts for each phase.
"""

import sys
import uuid
import time
from pathlib import Path
from datetime import datetime, timezone

BASE_DIR = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(BASE_DIR))

from backend.app.database import engine, SessionLocal, Base
from backend.app.models import PipelineRunLog, Base as ModelBase
from scripts.pipeline.extract_to_staging import extract_to_staging
from scripts.pipeline.transform_dimensions import transform_dimensions
from scripts.pipeline.transform_facts import transform_facts
from scripts.pipeline.run_quality_checks import run_quality_checks


def log_step(db, run_id, step_name, status, records=0, duration=0, error=None):
    """Record a pipeline step in the run log."""
    entry = PipelineRunLog(
        run_id=run_id,
        step_name=step_name,
        status=status,
        records_processed=records,
        duration_seconds=round(duration, 2),
        error_message=error,
    )
    db.add(entry)
    db.commit()


def run_pipeline():
    """Execute the full ETL pipeline."""
    # ensure all tables exist
    ModelBase.metadata.create_all(bind=engine)

    run_id = str(uuid.uuid4())[:8]
    db = SessionLocal()
    pipeline_start = time.time()

    print("=" * 60)
    print(f"Enterprise KPI Platform — ETL Pipeline")
    print(f"Run ID: {run_id}")
    print(f"Started: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 60)

    steps = [
        ("extract_to_staging", extract_to_staging),
        ("transform_dimensions", transform_dimensions),
        ("transform_facts", transform_facts),
        ("run_quality_checks", run_quality_checks),
    ]

    overall_status = "completed"

    for step_name, step_func in steps:
        print(f"\n{'─' * 60}")
        start = time.time()
        log_step(db, run_id, step_name, "started")

        try:
            results = step_func()
            duration = time.time() - start

            if isinstance(results, dict):
                total_records = sum(v for v in results.values() if isinstance(v, int))
            else:
                total_records = 0

            log_step(db, run_id, step_name, "completed",
                     records=total_records, duration=duration)

        except Exception as e:
            duration = time.time() - start
            log_step(db, run_id, step_name, "failed",
                     duration=duration, error=str(e))
            print(f"\n  ERROR in {step_name}: {e}")
            overall_status = "failed"
            break

    total_duration = time.time() - pipeline_start

    print(f"\n{'=' * 60}")
    print(f"Pipeline {overall_status.upper()}")
    print(f"Total duration: {total_duration:.1f}s")
    print(f"Run ID: {run_id}")
    print(f"{'=' * 60}")

    db.close()
    return overall_status


if __name__ == "__main__":
    run_pipeline()
