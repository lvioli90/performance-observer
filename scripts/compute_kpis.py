#!/usr/bin/env python3
"""
scripts/compute_kpis.py
=======================
Reads raw NDJSON observations from a completed (or in-progress) run and
computes all KPIs.  Writes:
  - products.csv
  - steps.csv
  - timeseries.csv
  - step_kpis.csv
  - run_summary.json
  - run_summary.md

Usage
-----
  python scripts/compute_kpis.py --run-dir results/baseline-run-001

  # Run against raw data in a custom directory
  python scripts/compute_kpis.py --run-dir results/baseline-run-001 --output-dir /tmp/kpis
"""

import argparse
import json
import logging
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import List, Optional

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from core.kpi import KPIEngine, compute_product_derived, compute_pod_derived, compute_workflow_derived
from core.persistence import read_ndjson, deduplicate_ndjson
from reporting.export_csv import write_all_csvs
from reporting.export_json import write_run_summary_json, write_markdown_summary

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)-8s %(name)s: %(message)s",
    stream=sys.stdout,
)
logger = logging.getLogger("compute_kpis")


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _parse_dt(val) -> Optional[datetime]:
    if val is None:
        return None
    if isinstance(val, datetime):
        return val
    if isinstance(val, str):
        try:
            return datetime.fromisoformat(val.replace("Z", "+00:00"))
        except ValueError:
            return None
    return None


def _detect_run_window(workflows: List[dict], products: List[dict]) -> tuple:
    """Estimate run start/end from workflow creation timestamps."""
    all_created = [
        _parse_dt(w.get("created_at")) for w in workflows if w.get("created_at")
    ]
    all_finished = [
        _parse_dt(w.get("finished_at")) for w in workflows if w.get("finished_at")
    ]
    started_at = min(all_created) if all_created else None
    finished_at = max(all_finished) if all_finished else datetime.now(timezone.utc)
    return started_at, finished_at


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> None:
    parser = argparse.ArgumentParser(description="Compute KPIs from observer raw data")
    parser.add_argument(
        "--run-dir", required=True,
        help="Path to the observer run output directory (contains raw/ subdirectory)"
    )
    parser.add_argument(
        "--output-dir", default=None,
        help="Where to write computed outputs. Defaults to --run-dir."
    )
    parser.add_argument("--verbose", "-v", action="store_true")
    args = parser.parse_args()

    if args.verbose:
        logging.getLogger().setLevel(logging.DEBUG)

    run_dir = Path(args.run_dir)
    raw_dir = run_dir / "raw"
    output_dir = Path(args.output_dir) if args.output_dir else run_dir

    if not raw_dir.exists():
        logger.error("Raw data directory not found: %s", raw_dir)
        sys.exit(1)

    logger.info("Loading raw data from %s", raw_dir)

    # ------------------------------------------------------------------
    # Load and de-duplicate NDJSON files
    # ------------------------------------------------------------------
    raw_workflows = read_ndjson(raw_dir / "workflows.ndjson")
    raw_pods = read_ndjson(raw_dir / "pods.ndjson")
    raw_products = read_ndjson(raw_dir / "products.ndjson")
    raw_timeseries = read_ndjson(raw_dir / "timeseries.ndjson")

    # De-duplicate: keep latest record per key
    workflows = deduplicate_ndjson(raw_workflows, "workflow_name")
    pods = deduplicate_ndjson(raw_pods, "pod_name")
    products = deduplicate_ndjson(raw_products, "product_id")
    # Timeseries is NOT de-duplicated (chronological; each row is a distinct snapshot)

    logger.info(
        "Loaded: %d workflows, %d pods, %d products, %d timeseries rows",
        len(workflows), len(pods), len(products), len(raw_timeseries),
    )

    # ------------------------------------------------------------------
    # Compute derived fields on each record (in-place on copies)
    # ------------------------------------------------------------------
    workflows = [compute_workflow_derived(dict(w)) for w in workflows]
    pods = [compute_pod_derived(dict(p)) for p in pods]
    products = [compute_product_derived(dict(p)) for p in products]

    # ------------------------------------------------------------------
    # Determine run window
    # ------------------------------------------------------------------
    started_at, finished_at = _detect_run_window(workflows, products)

    # Attempt to load run_id from checkpoint or first workflow record
    run_id = "unknown"
    if workflows:
        run_id = workflows[0].get("run_id", "unknown")
    if products:
        run_id = products[0].get("run_id", run_id)

    duration_sec = None
    if started_at and finished_at:
        duration_sec = (finished_at - started_at).total_seconds()

    logger.info("Run ID      : %s", run_id)
    logger.info("Run started : %s", started_at)
    logger.info("Run finished: %s", finished_at)
    logger.info("Duration    : %.0fs", duration_sec or 0)

    # ------------------------------------------------------------------
    # KPI computation
    # ------------------------------------------------------------------
    engine = KPIEngine(
        workflows=workflows,
        pods=pods,
        products=products,
        timeseries=raw_timeseries,
        run_started_at=started_at,
        run_finished_at=finished_at,
    )
    kpis = engine.compute_all()

    biz = kpis["business"]
    wf_kpis = kpis["workflows"]
    step_kpis = kpis["steps"]
    hints = kpis["bottleneck_hints"]

    logger.info("KPI results:")
    logger.info("  Products completed  : %d / %d", biz["total_products_completed"], biz["total_products_observed"])
    logger.info("  Success rate        : %.1f%%", (biz["success_rate"] or 0) * 100)
    logger.info("  Avg throughput      : %.2f products/min", biz["avg_throughput_per_min"] or 0)
    logger.info("  E2E p50             : %.1fs", biz["e2e_p50"] or 0)
    logger.info("  E2E p95             : %.1fs", biz["e2e_p95"] or 0)
    logger.info("  Workflow queue p95  : %.1fs", wf_kpis["queue_p95"] or 0)
    logger.info("  Max pending wf      : %d", wf_kpis["max_pending_workflows"])
    logger.info("Bottleneck hints:")
    for h in hints:
        logger.info("  -> %s", h)

    # ------------------------------------------------------------------
    # Build full summary dict
    # ------------------------------------------------------------------
    summary = {
        "meta": {
            "run_id": run_id,
            "started_at": started_at.isoformat() if started_at else None,
            "finished_at": finished_at.isoformat() if finished_at else None,
            "duration_sec": duration_sec,
            "computed_at": datetime.now(timezone.utc).isoformat(),
        },
        "business": biz,
        "workflows": wf_kpis,
        "steps": step_kpis,
        "bottleneck_hints": hints,
    }

    # ------------------------------------------------------------------
    # Write outputs
    # ------------------------------------------------------------------
    write_all_csvs(
        output_dir=output_dir,
        products=products,
        pods=pods,
        timeseries=raw_timeseries,
        step_kpis=step_kpis,
    )

    write_run_summary_json(output_dir, summary)
    write_markdown_summary(output_dir, summary)

    logger.info("KPI computation complete. Outputs written to %s", output_dir)
    logger.info("Next: python scripts/plot_results.py --run-dir %s", output_dir)


if __name__ == "__main__":
    main()
