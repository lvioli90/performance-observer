"""
reporting/export_csv.py
=======================
Writes the four required CSV output files from raw NDJSON data.

Output files
------------
- products.csv    : one row per product (end-to-end view)
- steps.csv       : one row per pod/step execution
- timeseries.csv  : one row per timeseries snapshot (workflow + product counts)
- step_kpis.csv   : one row per step_name with aggregated KPI columns
"""

from __future__ import annotations

import csv
import logging
from pathlib import Path
from typing import Any, Dict, List, Optional

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Column definitions
# ---------------------------------------------------------------------------

PRODUCTS_COLUMNS = [
    "run_id",
    "product_id",
    "object_key",
    "workflow_name",
    "workflow_created_at",
    "workflow_started_at",
    "workflow_finished_at",
    "stac_seen_at",
    "ingest_reference_time",
    "workflow_queue_sec",
    "workflow_run_sec",
    "stac_publish_sec",
    "end_to_end_sec",
    "final_status",
]

STEPS_COLUMNS = [
    "run_id",
    "product_id",
    "workflow_name",
    "step_name",
    "pod_name",
    "pod_phase",
    "pod_created_at",
    "pod_started_at",
    "pod_finished_at",
    "pod_pending_sec",
    "pod_running_sec",
    "retries",
    "restart_count",
    "oom_killed",
    "cpu_avg",
    "cpu_peak",
    "cpu_throttling",
    "mem_avg",
    "mem_peak",
]

TIMESERIES_COLUMNS = [
    "timestamp",
    "workflows_pending",
    "workflows_running",
    "workflows_succeeded_total",
    "workflows_failed_total",
    "completed_products_total",
    "throughput_last_5m",
    "avg_queue_time_last_5m",
    "p95_queue_time_last_5m",
]

STEP_KPIS_COLUMNS = [
    "step_name",
    "total_executions",
    "failed_count",
    "failure_rate",
    "retry_count",
    "retry_rate",
    "oom_count",
    "oom_rate",
    "pending_avg",
    "pending_p95",
    "duration_avg",
    "duration_p50",
    "duration_p95",
    "running_avg",
    "running_p50",
    "running_p95",
    "cpu_avg",
    "cpu_peak_avg",
    "cpu_peak_max",
    "mem_avg",
    "mem_peak_avg",
    "mem_peak_max",
    "cpu_throttling_avg",
    "throttling_incidence",
]


# ---------------------------------------------------------------------------
# Writer helpers
# ---------------------------------------------------------------------------

def _write_csv(path: Path, columns: List[str], rows: List[dict]) -> int:
    """
    Write rows to a CSV file with the given column order.
    Missing fields default to empty string.
    Returns row count written.
    """
    path.parent.mkdir(parents=True, exist_ok=True)
    count = 0
    with open(path, "w", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(
            fh,
            fieldnames=columns,
            extrasaction="ignore",   # drop extra fields silently
        )
        writer.writeheader()
        for row in rows:
            # Convert any non-string types that CSV can't handle
            cleaned = {k: _clean(row.get(k)) for k in columns}
            writer.writerow(cleaned)
            count += 1
    logger.info("Wrote %d rows to %s", count, path)
    return count


def _clean(val: Any) -> Any:
    """Coerce value to a CSV-safe type."""
    if val is None:
        return ""
    if isinstance(val, bool):
        return str(val).lower()
    if isinstance(val, (list, dict)):
        return str(val)
    return val


# ---------------------------------------------------------------------------
# Public functions
# ---------------------------------------------------------------------------

def write_products_csv(output_dir: Path, products: List[dict]) -> Path:
    """Write products.csv from a list of product record dicts."""
    path = output_dir / "products.csv"
    _write_csv(path, PRODUCTS_COLUMNS, products)
    return path


def write_steps_csv(output_dir: Path, pods: List[dict]) -> Path:
    """Write steps.csv from a list of pod record dicts."""
    path = output_dir / "steps.csv"
    _write_csv(path, STEPS_COLUMNS, pods)
    return path


def write_timeseries_csv(output_dir: Path, snapshots: List[dict]) -> Path:
    """Write timeseries.csv from a list of timeseries snapshot dicts."""
    path = output_dir / "timeseries.csv"
    _write_csv(path, TIMESERIES_COLUMNS, snapshots)
    return path


def write_step_kpis_csv(output_dir: Path, step_kpis: List[dict]) -> Path:
    """Write step_kpis.csv from the per-step KPI dicts produced by KPIEngine."""
    path = output_dir / "step_kpis.csv"
    _write_csv(path, STEP_KPIS_COLUMNS, step_kpis)
    return path


def write_all_csvs(
    output_dir: Path,
    products: List[dict],
    pods: List[dict],
    timeseries: List[dict],
    step_kpis: Optional[List[dict]] = None,
) -> Dict[str, Path]:
    """
    Convenience wrapper: write all four CSV files and return a dict of paths.
    """
    paths = {}
    paths["products"] = write_products_csv(output_dir, products)
    paths["steps"] = write_steps_csv(output_dir, pods)
    paths["timeseries"] = write_timeseries_csv(output_dir, timeseries)
    if step_kpis is not None:
        paths["step_kpis"] = write_step_kpis_csv(output_dir, step_kpis)
    return paths
