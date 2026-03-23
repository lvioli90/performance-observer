#!/usr/bin/env python3
"""
scripts/compare_runs.py
=======================
Generate a side-by-side comparison report for two observer runs (e.g. dev vs. UAT).

Prerequisites
-------------
Both run directories must contain the outputs of compute_kpis.py:
  step_kpis.csv, run_summary.json, products.csv

Usage
-----
  # Compare dev (run A) vs UAT (run B):
  python scripts/compare_runs.py \\
      --run-a results/dev-run-001 \\
      --run-b results/uat-run-001 \\
      --label-a dev \\
      --label-b uat

  # Custom output directory:
  python scripts/compare_runs.py \\
      --run-a results/dev-run-001 \\
      --run-b results/uat-run-001 \\
      --output-dir results/comparison-2026-03-23

Outputs (written to --output-dir or comparison_<label-a>_vs_<label-b>/)
-------
  comparison.md               Human-readable side-by-side report
  comparison_steps.csv        Per-step metrics with delta / ratio columns
  plots/
    step_duration_comparison.png
    step_cpu_comparison.png
    step_mem_comparison.png
    pipeline_breakdown_comparison.png
"""

import argparse
import logging
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from reporting.compare import (
    load_run,
    build_step_comparison,
    build_pipeline_comparison,
    build_business_comparison,
    write_comparison_steps_csv,
    write_comparison_md,
    generate_comparison_plots,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)-8s %(name)s: %(message)s",
    stream=sys.stdout,
)
logger = logging.getLogger("compare_runs")


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Compare two observer runs side-by-side (e.g. dev vs UAT)"
    )
    parser.add_argument(
        "--run-a", required=True, metavar="DIR",
        help="Run directory for the baseline (e.g. dev run)",
    )
    parser.add_argument(
        "--run-b", required=True, metavar="DIR",
        help="Run directory for the comparison target (e.g. UAT run)",
    )
    parser.add_argument(
        "--label-a", default="dev", metavar="LABEL",
        help="Short label for run A used in column headers (default: dev)",
    )
    parser.add_argument(
        "--label-b", default="uat", metavar="LABEL",
        help="Short label for run B used in column headers (default: uat)",
    )
    parser.add_argument(
        "--output-dir", default=None, metavar="DIR",
        help="Output directory for comparison reports. "
             "Defaults to comparison_<label-a>_vs_<label-b>/",
    )
    parser.add_argument(
        "--no-plots", action="store_true",
        help="Skip matplotlib plot generation",
    )
    parser.add_argument("--verbose", "-v", action="store_true")
    args = parser.parse_args()

    if args.verbose:
        logging.getLogger().setLevel(logging.DEBUG)

    run_a_dir = Path(args.run_a)
    run_b_dir = Path(args.run_b)
    label_a   = args.label_a
    label_b   = args.label_b
    output_dir = (
        Path(args.output_dir)
        if args.output_dir
        else Path(f"comparison_{label_a}_vs_{label_b}")
    )

    # ------------------------------------------------------------------
    # Validate inputs
    # ------------------------------------------------------------------
    ok = True
    for d, label in [(run_a_dir, label_a), (run_b_dir, label_b)]:
        if not d.exists():
            logger.error("Run directory not found: %s (%s)", d, label)
            ok = False
        elif not (d / "step_kpis.csv").exists():
            logger.warning(
                "step_kpis.csv missing in %s (%s) – run compute_kpis.py first", d, label
            )
    if not ok:
        sys.exit(1)

    # ------------------------------------------------------------------
    # Load data from both runs
    # ------------------------------------------------------------------
    logger.info("Loading %s: %s", label_a, run_a_dir)
    data_a = load_run(run_a_dir)
    logger.info("Loading %s: %s", label_b, run_b_dir)
    data_b = load_run(run_b_dir)

    logger.info(
        "%s – step-kpi rows: %d  product rows: %d",
        label_a, len(data_a["step_kpis"]), len(data_a["products"]),
    )
    logger.info(
        "%s – step-kpi rows: %d  product rows: %d",
        label_b, len(data_b["step_kpis"]), len(data_b["products"]),
    )

    # ------------------------------------------------------------------
    # Build comparison tables
    # ------------------------------------------------------------------
    step_comp     = build_step_comparison(data_a["step_kpis"], data_b["step_kpis"], label_a, label_b)
    pipeline_comp = build_pipeline_comparison(data_a["products"], data_b["products"], label_a, label_b)
    biz_comp      = build_business_comparison(data_a["summary"], data_b["summary"], label_a, label_b)
    hints_a       = data_a["summary"].get("bottleneck_hints", [])
    hints_b       = data_b["summary"].get("bottleneck_hints", [])

    logger.info("Steps matched: %d (total rows including run-B-only)", len(step_comp))

    # ------------------------------------------------------------------
    # Write outputs
    # ------------------------------------------------------------------
    output_dir.mkdir(parents=True, exist_ok=True)

    csv_path = write_comparison_steps_csv(output_dir, step_comp, label_a, label_b)
    md_path  = write_comparison_md(
        output_dir, label_a, label_b,
        biz_comp, pipeline_comp, step_comp,
        hints_a, hints_b,
    )

    if not args.no_plots:
        plots = generate_comparison_plots(output_dir, step_comp, pipeline_comp, label_a, label_b)
        if plots:
            logger.info("Generated %d plots:", len(plots))
            for name, path in plots.items():
                logger.info("  [%s] %s", name, path)

    logger.info("Done. Outputs written to %s/", output_dir)
    logger.info("  Report : %s", md_path)
    logger.info("  CSV    : %s", csv_path)


if __name__ == "__main__":
    main()
