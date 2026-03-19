#!/usr/bin/env python3
"""
scripts/plot_results.py
=======================
Reads computed data from a run directory and generates all diagnostic plots.

Requires compute_kpis.py to have been run first (needs products.csv,
timeseries.csv, step_kpis.csv).

Falls back to raw NDJSON if CSV files are not yet present.

Usage
-----
  python scripts/plot_results.py --run-dir results/baseline-run-001

  # Save plots to a different directory
  python scripts/plot_results.py --run-dir results/baseline-run-001 --plots-dir /tmp/plots
"""

import argparse
import csv
import json
import logging
import sys
from pathlib import Path
from typing import Any, Dict, List, Optional

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from core.persistence import read_ndjson, deduplicate_ndjson
from core.kpi import compute_product_derived, compute_pod_derived
from reporting.plots import generate_all_plots

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)-8s %(name)s: %(message)s",
    stream=sys.stdout,
)
logger = logging.getLogger("plot_results")


# ---------------------------------------------------------------------------
# CSV reader
# ---------------------------------------------------------------------------

def _read_csv(path: Path) -> List[dict]:
    """Read a CSV file into a list of dicts."""
    if not path.exists():
        logger.warning("CSV not found: %s", path)
        return []
    rows = []
    with open(path, "r", encoding="utf-8", newline="") as fh:
        reader = csv.DictReader(fh)
        for row in reader:
            rows.append(dict(row))
    return rows


def _read_json(path: Path) -> Optional[dict]:
    if not path.exists():
        return None
    with open(path, "r", encoding="utf-8") as fh:
        return json.load(fh)


# ---------------------------------------------------------------------------
# Data loading with fallback
# ---------------------------------------------------------------------------

def load_data(run_dir: Path) -> tuple:
    """
    Load products, timeseries, and step_kpis.
    Prefers computed CSV/JSON files; falls back to raw NDJSON.
    """
    raw_dir = run_dir / "raw"

    # --- Products ---
    products_csv = run_dir / "products.csv"
    if products_csv.exists():
        products = _read_csv(products_csv)
        logger.info("Loaded %d products from %s", len(products), products_csv)
    elif raw_dir.exists():
        raw = read_ndjson(raw_dir / "products.ndjson")
        products = [compute_product_derived(dict(r)) for r in deduplicate_ndjson(raw, "product_id")]
        logger.info("Loaded %d products from raw NDJSON (fallback)", len(products))
    else:
        products = []

    # --- Timeseries ---
    ts_csv = run_dir / "timeseries.csv"
    if ts_csv.exists():
        timeseries = _read_csv(ts_csv)
        logger.info("Loaded %d timeseries rows from %s", len(timeseries), ts_csv)
    elif raw_dir.exists():
        timeseries = read_ndjson(raw_dir / "timeseries.ndjson")
        logger.info("Loaded %d timeseries rows from raw NDJSON (fallback)", len(timeseries))
    else:
        timeseries = []

    # --- Step KPIs ---
    step_kpis_csv = run_dir / "step_kpis.csv"
    if step_kpis_csv.exists():
        step_kpis = _read_csv(step_kpis_csv)
        logger.info("Loaded %d step KPIs from %s", len(step_kpis), step_kpis_csv)
    else:
        # Compute step KPIs on-the-fly from raw pods
        if raw_dir.exists():
            raw_pods = read_ndjson(raw_dir / "pods.ndjson")
            pods = [compute_pod_derived(dict(p)) for p in deduplicate_ndjson(raw_pods, "pod_name")]
            from core.kpi import KPIEngine
            engine = KPIEngine(workflows=[], pods=pods, products=products, timeseries=timeseries)
            step_kpis = engine.step_kpis()
            logger.info("Computed %d step KPIs from raw pods (fallback)", len(step_kpis))
        else:
            step_kpis = []

    return products, timeseries, step_kpis


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> None:
    parser = argparse.ArgumentParser(description="Generate diagnostic plots from observer data")
    parser.add_argument(
        "--run-dir", required=True,
        help="Path to the observer run output directory"
    )
    parser.add_argument(
        "--plots-dir", default=None,
        help="Where to save plot images. Defaults to <run-dir>/plots."
    )
    parser.add_argument("--verbose", "-v", action="store_true")
    args = parser.parse_args()

    if args.verbose:
        logging.getLogger().setLevel(logging.DEBUG)

    run_dir = Path(args.run_dir)
    if not run_dir.exists():
        logger.error("Run directory not found: %s", run_dir)
        sys.exit(1)

    products, timeseries, step_kpis = load_data(run_dir)

    if not products and not timeseries:
        logger.error("No data found in %s. Run observe_run.py first.", run_dir)
        sys.exit(1)

    # Determine output directory for plots
    if args.plots_dir:
        output_dir = Path(args.plots_dir)
    else:
        output_dir = run_dir  # plots.py creates a plots/ subdirectory inside

    output_dir.mkdir(parents=True, exist_ok=True)

    logger.info("Generating plots...")
    results = generate_all_plots(
        output_dir=output_dir,
        timeseries=timeseries,
        products=products,
        step_kpis=step_kpis,
    )

    # Summary
    generated = [(k, v) for k, v in results.items() if v is not None]
    skipped = [(k, v) for k, v in results.items() if v is None]

    logger.info("=" * 50)
    logger.info("Plots generated: %d", len(generated))
    for name, path in generated:
        logger.info("  %-35s -> %s", name, path.name)
    if skipped:
        logger.warning("Plots skipped (no data): %s", [k for k, _ in skipped])
    logger.info("Plot directory: %s", output_dir / "plots")


if __name__ == "__main__":
    main()
