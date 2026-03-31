"""
reporting/compare.py
====================
Side-by-side comparison of two observer runs (e.g. dev vs. UAT).

Inputs
------
Each run directory must contain the outputs of compute_kpis.py:
  - step_kpis.csv
  - run_summary.json
  - products.csv

Outputs
-------
  - comparison_steps.csv           : per-step metrics side by side with delta / ratio
  - comparison.md                  : human-readable Markdown report
  - plots/step_duration_comparison.png
  - plots/step_cpu_comparison.png
  - plots/step_mem_comparison.png
  - plots/pipeline_breakdown_comparison.png
"""

from __future__ import annotations

import csv
import json
import logging
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Metric definitions
# (csv_column, display_label, lower_is_better)
# ---------------------------------------------------------------------------

STEP_METRICS: List[Tuple[str, str, bool]] = [
    ("scheduling_avg",     "Scheduling avg (s)",   True),
    ("init_duration_avg",  "Init avg (s)",         True),
    ("init_duration_p50",  "Init p50 (s)",         True),
    ("init_duration_p95",  "Init p95 (s)",         True),
    ("volume_attach_p50",  "Vol attach p50 (s)",   True),
    ("pending_avg",        "Pending avg (s)",      True),
    ("duration_avg",       "Duration avg (s)",     True),
    ("duration_p50",       "Duration p50 (s)",     True),
    ("duration_p95",       "Duration p95 (s)",     True),
    ("running_avg",        "Running avg (s)",      True),
    ("cpu_avg",            "CPU avg (m)",          True),
    ("cpu_peak_avg",       "CPU peak avg (m)",     True),
    ("cpu_peak_max",       "CPU peak max (m)",     True),
    ("mem_avg",            "Mem avg (MiB)",        True),
    ("mem_peak_avg",       "Mem peak avg (MiB)",   True),
    ("mem_peak_max",       "Mem peak max (MiB)",   True),
    ("cpu_throttling_avg", "CPU throttling avg",   True),
]

# Pipeline-phase columns from products.csv
PIPELINE_METRICS: List[Tuple[str, str, bool]] = [
    ("dispatcher_queue_sec",   "Dispatcher queue (s)",     True),
    ("dispatcher_run_sec",     "Dispatcher run (s)",       True),
    ("pipeline_gap_sec",       "Pipeline gap (s)",         True),
    ("workflow_queue_sec",     "Omnipass queue (s)",       True),
    ("workflow_run_sec",       "Omnipass run (s)",         True),
    ("stac_publish_sec",       "STAC publish (s)",         True),
    ("gap_to_deletion_sec",    "Gap→deletion (s)",         True),
    ("deletion_queue_sec",     "Deletion queue (s)",       True),
    ("deletion_run_sec",       "Deletion run (s)",         True),
    ("end_to_end_sec",         "End-to-end (s)",           True),
]

# Keys from run_summary.json -> business section
BIZ_METRICS: List[Tuple[str, str, bool]] = [
    ("total_products_completed", "Products completed",      False),
    ("success_rate",             "Success rate",            False),
    ("e2e_avg",                  "E2E avg (s)",             True),
    ("e2e_p50",                  "E2E p50 (s)",             True),
    ("e2e_p95",                  "E2E p95 (s)",             True),
    ("stac_latency_avg",         "STAC latency avg (s)",    True),
    ("stac_latency_p95",         "STAC latency p95 (s)",    True),
    ("pipeline_gap_avg",         "Pipeline gap avg (s)",    True),
    ("pipeline_gap_p95",         "Pipeline gap p95 (s)",    True),
    ("gap_to_deletion_avg",      "Gap→deletion avg (s)",    True),
    ("deletion_queue_avg",       "Deletion queue avg (s)",  True),
    ("deletion_duration_avg",    "Deletion duration avg (s)", True),
    ("deletion_duration_p95",    "Deletion duration p95 (s)", True),
]


# ---------------------------------------------------------------------------
# Data loading
# ---------------------------------------------------------------------------

def _read_csv(path: Path) -> List[Dict[str, str]]:
    if not path.exists():
        logger.warning("File not found: %s", path)
        return []
    with open(path, newline="", encoding="utf-8") as fh:
        return list(csv.DictReader(fh))


def _read_json(path: Path) -> dict:
    if not path.exists():
        logger.warning("File not found: %s", path)
        return {}
    with open(path, encoding="utf-8") as fh:
        return json.load(fh)


def load_run(run_dir: Path) -> Dict[str, Any]:
    """Load all relevant outputs from a run directory produced by compute_kpis.py."""
    return {
        "step_kpis": _read_csv(run_dir / "step_kpis.csv"),
        "products":  _read_csv(run_dir / "products.csv"),
        "summary":   _read_json(run_dir / "run_summary.json"),
        "run_dir":   run_dir,
    }


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

def _to_float(val: Any) -> Optional[float]:
    try:
        return float(val)
    except (TypeError, ValueError):
        return None


def _delta_indicator(delta: Optional[float], base: Optional[float], lower_is_better: bool) -> str:
    """Return ▲ / ▼ / ≈ based on relative change (threshold: 5%)."""
    if delta is None or base is None or base == 0:
        return "≈"
    if abs(delta / base) < 0.05:
        return "≈"
    worse = (delta > 0) if lower_is_better else (delta < 0)
    return "▲" if worse else "▼"


def _fmt(val: Any, unit: str = "", decimals: int = 1) -> str:
    if val is None:
        return "N/A"
    if isinstance(val, float):
        return f"{val:.{decimals}f}{unit}"
    if isinstance(val, int):
        return f"{val}{unit}"
    return str(val)


# ---------------------------------------------------------------------------
# Comparison builders
# ---------------------------------------------------------------------------

def build_step_comparison(
    kpis_a: List[dict],
    kpis_b: List[dict],
    label_a: str,
    label_b: str,
) -> List[dict]:
    """
    Merge step_kpis rows from both runs by (workflow_type, step_name).
    Steps only in run B are appended at the end (new steps in UAT, etc.).
    """
    idx_b: Dict[Tuple[str, str], dict] = {
        (r.get("workflow_type", ""), r.get("step_name", "")): r
        for r in kpis_b
    }

    rows = []

    for row_a in kpis_a:
        key = (row_a.get("workflow_type", ""), row_a.get("step_name", ""))
        row_b = idx_b.get(key, {})

        merged: Dict[str, Any] = {
            "workflow_type": key[0],
            "step_name":     key[1],
        }

        # Count columns (integer, no ratio)
        for count_col in ("total_executions", "failed_count", "oom_count", "retry_count"):
            va = _to_float(row_a.get(count_col))
            vb = _to_float(row_b.get(count_col)) if row_b else None
            merged[f"{count_col}_{label_a}"] = int(va) if va is not None else None
            merged[f"{count_col}_{label_b}"] = int(vb) if vb is not None else None

        # Numeric metric columns
        for col, _label, lower_better in STEP_METRICS:
            va = _to_float(row_a.get(col))
            vb = _to_float(row_b.get(col)) if row_b else None
            delta = (vb - va) if (va is not None and vb is not None) else None
            ratio = (vb / va) if (va and vb is not None) else None
            merged[f"{col}_{label_a}"] = va
            merged[f"{col}_{label_b}"] = vb
            merged[f"{col}_delta"]      = round(delta, 3) if delta is not None else None
            merged[f"{col}_ratio"]      = round(ratio, 3) if ratio is not None else None
            merged[f"{col}_trend"]      = _delta_indicator(delta, va, lower_better)

        rows.append(merged)

    # Append steps present only in run B
    keys_a = {(r.get("workflow_type", ""), r.get("step_name", "")) for r in kpis_a}
    for row_b in kpis_b:
        key = (row_b.get("workflow_type", ""), row_b.get("step_name", ""))
        if key not in keys_a:
            merged = {"workflow_type": key[0], "step_name": key[1]}
            for count_col in ("total_executions", "failed_count", "oom_count", "retry_count"):
                merged[f"{count_col}_{label_a}"] = None
                merged[f"{count_col}_{label_b}"] = _to_float(row_b.get(count_col))
            for col, _label, lower_better in STEP_METRICS:
                merged[f"{col}_{label_a}"] = None
                merged[f"{col}_{label_b}"] = _to_float(row_b.get(col))
                merged[f"{col}_delta"]     = None
                merged[f"{col}_ratio"]     = None
                merged[f"{col}_trend"]     = "—"
            rows.append(merged)

    return rows


def build_pipeline_comparison(
    products_a: List[dict],
    products_b: List[dict],
    label_a: str,
    label_b: str,
) -> List[dict]:
    """
    Compare pipeline-phase timings.
    Single-product runs: shows the value directly.
    Multi-product runs: shows the average across products.
    """
    def _avg(records: List[dict], col: str) -> Optional[float]:
        vals = [_to_float(r.get(col)) for r in records]
        vals = [v for v in vals if v is not None]
        return sum(vals) / len(vals) if vals else None

    rows = []
    for col, label, lower_better in PIPELINE_METRICS:
        va = _avg(products_a, col)
        vb = _avg(products_b, col)
        delta = (vb - va) if (va is not None and vb is not None) else None
        rows.append({
            "phase":            label,
            f"value_{label_a}": round(va, 2) if va is not None else None,
            f"value_{label_b}": round(vb, 2) if vb is not None else None,
            "delta":            round(delta, 2) if delta is not None else None,
            "trend":            _delta_indicator(delta, va, lower_better),
        })
    return rows


def build_business_comparison(
    summary_a: dict,
    summary_b: dict,
    label_a: str,
    label_b: str,
) -> List[dict]:
    """Compare business KPIs from run_summary.json."""
    biz_a = summary_a.get("business", {})
    biz_b = summary_b.get("business", {})

    rows = []
    for col, label, lower_better in BIZ_METRICS:
        va = _to_float(biz_a.get(col))
        vb = _to_float(biz_b.get(col))
        delta = (vb - va) if (va is not None and vb is not None) else None
        rows.append({
            "metric":           label,
            f"value_{label_a}": va,
            f"value_{label_b}": vb,
            "delta":            round(delta, 3) if delta is not None else None,
            "trend":            _delta_indicator(delta, va, lower_better),
        })
    return rows


# ---------------------------------------------------------------------------
# CSV output
# ---------------------------------------------------------------------------

def write_comparison_steps_csv(
    output_dir: Path,
    step_comparison: List[dict],
    label_a: str,
    label_b: str,
) -> Path:
    """Write comparison_steps.csv with side-by-side step metrics + deltas."""
    output_dir.mkdir(parents=True, exist_ok=True)
    path = output_dir / "comparison_steps.csv"

    if not step_comparison:
        path.write_text("workflow_type,step_name\n", encoding="utf-8")
        return path

    fixed = ["workflow_type", "step_name"]
    count_cols = []
    for count_col in ("total_executions", "failed_count", "oom_count", "retry_count"):
        count_cols += [f"{count_col}_{label_a}", f"{count_col}_{label_b}"]
    metric_cols = []
    for col, _, _ in STEP_METRICS:
        metric_cols += [
            f"{col}_{label_a}", f"{col}_{label_b}",
            f"{col}_delta", f"{col}_ratio", f"{col}_trend",
        ]
    fieldnames = fixed + count_cols + metric_cols

    with open(path, "w", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(fh, fieldnames=fieldnames, extrasaction="ignore")
        writer.writeheader()
        for row in step_comparison:
            writer.writerow({k: ("" if row.get(k) is None else row[k]) for k in fieldnames})

    logger.info("Wrote %s (%d rows)", path, len(step_comparison))
    return path


# ---------------------------------------------------------------------------
# Markdown output
# ---------------------------------------------------------------------------

def write_comparison_md(
    output_dir: Path,
    label_a: str,
    label_b: str,
    biz_comp: List[dict],
    pipeline_comp: List[dict],
    step_comp: List[dict],
    hints_a: List[str],
    hints_b: List[str],
) -> Path:
    """Write comparison.md with a full side-by-side human-readable report."""
    output_dir.mkdir(parents=True, exist_ok=True)
    path = output_dir / "comparison.md"

    lines: List[str] = [
        f"# Performance Comparison: {label_a} vs {label_b}",
        f"",
        f"> **Legend:** ▲ = {label_b} is **worse**  ▼ = {label_b} is **better**  ≈ = no significant difference (< 5%)",
        f"",
    ]

    # ---- Business KPIs ----
    lines += [
        "## Business KPIs",
        "",
        f"| Metric | {label_a} | {label_b} | Delta | Trend |",
        "|--------|------:|------:|------:|:-----:|",
    ]
    for row in biz_comp:
        col_label = row["metric"]
        is_pct = "Success" in col_label
        va = row.get(f"value_{label_a}")
        vb = row.get(f"value_{label_b}")
        delta = row.get("delta")
        trend = row.get("trend", "≈")
        if is_pct:
            va_s = _fmt(va * 100 if va is not None else None, "%")
            vb_s = _fmt(vb * 100 if vb is not None else None, "%")
            d_s  = _fmt(delta * 100 if delta is not None else None, "%")
        else:
            va_s = _fmt(va)
            vb_s = _fmt(vb)
            d_s  = _fmt(delta, decimals=2)
        lines.append(f"| {col_label} | {va_s} | {vb_s} | {d_s} | {trend} |")
    lines.append("")

    # ---- Pipeline phases ----
    lines += [
        "## Pipeline Phase Breakdown",
        "",
        f"| Phase | {label_a} (s) | {label_b} (s) | Delta (s) | Trend |",
        "|-------|------:|------:|------:|:-----:|",
    ]
    for row in pipeline_comp:
        phase = row["phase"]
        va    = row.get(f"value_{label_a}")
        vb    = row.get(f"value_{label_b}")
        delta = row.get("delta")
        trend = row.get("trend", "≈")
        lines.append(f"| {phase} | {_fmt(va)} | {_fmt(vb)} | {_fmt(delta)} | {trend} |")
    lines.append("")

    # ---- Per-step: group by workflow_type ----
    lines += ["## Per-Step Comparison", ""]

    wf_types = sorted({r.get("workflow_type", "") for r in step_comp})
    for wft in wf_types:
        rows_wft = [r for r in step_comp if r.get("workflow_type", "") == wft]
        lines += [
            f"### {wft or 'all'}",
            "",
            f"| Step "
            f"| Dur {label_a} | Dur {label_b} | Δ Dur "
            f"| CPU pk {label_a} | CPU pk {label_b} | Δ CPU "
            f"| Mem pk {label_a} | Mem pk {label_b} | Δ Mem |",
            "|------|"
            "------:|------:|------:|"
            "------:|------:|------:|"
            "------:|------:|------:|",
        ]
        lines += [
            f"| Step "
            f"| Init {label_a} | Init {label_b} | Δ Init "
            f"| Dur {label_a} | Dur {label_b} | Δ Dur "
            f"| Run {label_a} | Run {label_b} | Δ Run |",
            "|------|"
            "------:|------:|------:|"
            "------:|------:|------:|"
            "------:|------:|------:|",
        ]
        for row in rows_wft:
            sn = row.get("step_name", "")

            i_a     = _fmt(row.get(f"init_duration_avg_{label_a}"), "s")
            i_b     = _fmt(row.get(f"init_duration_avg_{label_b}"), "s")
            i_delta = _fmt(row.get("init_duration_avg_delta"), "s")
            i_trend = row.get("init_duration_avg_trend", "≈")

            d_a     = _fmt(row.get(f"duration_avg_{label_a}"), "s")
            d_b     = _fmt(row.get(f"duration_avg_{label_b}"), "s")
            d_delta = _fmt(row.get("duration_avg_delta"), "s")
            d_trend = row.get("duration_avg_trend", "≈")

            r_a     = _fmt(row.get(f"running_avg_{label_a}"), "s")
            r_b     = _fmt(row.get(f"running_avg_{label_b}"), "s")
            r_delta = _fmt(row.get("running_avg_delta"), "s")
            r_trend = row.get("running_avg_trend", "≈")

            lines.append(
                f"| {sn} "
                f"| {i_a} | {i_b} | {i_delta} {i_trend} "
                f"| {d_a} | {d_b} | {d_delta} {d_trend} "
                f"| {r_a} | {r_b} | {r_delta} {r_trend} |"
            )
        lines.append("")

    # ---- Bottleneck hints ----
    lines += ["## Bottleneck Diagnosis", ""]
    lines.append(f"**{label_a}:**")
    lines += ([f"- {h}" for h in hints_a] if hints_a else ["- none"])
    lines.append("")
    lines.append(f"**{label_b}:**")
    lines += ([f"- {h}" for h in hints_b] if hints_b else ["- none"])
    lines.append("")

    content = "\n".join(lines)
    with open(path, "w", encoding="utf-8") as fh:
        fh.write(content)

    logger.info("Wrote %s", path)
    return path


# ---------------------------------------------------------------------------
# Plots
# ---------------------------------------------------------------------------

def generate_comparison_plots(
    output_dir: Path,
    step_comp: List[dict],
    pipeline_comp: List[dict],
    label_a: str,
    label_b: str,
) -> Dict[str, Path]:
    """Generate grouped bar charts comparing the two runs."""
    try:
        import matplotlib
        matplotlib.use("Agg")
        import matplotlib.pyplot as plt
        import numpy as np
    except ImportError:
        logger.warning("matplotlib not available – skipping comparison plots")
        return {}

    plots_dir = output_dir / "plots"
    plots_dir.mkdir(parents=True, exist_ok=True)
    generated: Dict[str, Path] = {}

    def _grouped_bar(
        title: str,
        step_names: List[str],
        vals_a: List[Optional[float]],
        vals_b: List[Optional[float]],
        ylabel: str,
        filename: str,
    ) -> Optional[Path]:
        if not step_names:
            return None
        x = np.arange(len(step_names))
        width = 0.35
        fig, ax = plt.subplots(figsize=(max(8, len(step_names) * 0.9), 5))
        bar_a = ax.bar(x - width / 2, [v or 0 for v in vals_a], width,
                       label=label_a, color="#4C72B0")
        bar_b = ax.bar(x + width / 2, [v or 0 for v in vals_b], width,
                       label=label_b, color="#DD8452")
        ax.set_title(title)
        ax.set_ylabel(ylabel)
        ax.set_xticks(x)
        ax.set_xticklabels(step_names, rotation=45, ha="right")
        ax.legend()
        ax.bar_label(bar_a, fmt="%.0f", padding=2, fontsize=7)
        ax.bar_label(bar_b, fmt="%.0f", padding=2, fontsize=7)
        fig.tight_layout()
        path = plots_dir / filename
        fig.savefig(path, dpi=120)
        plt.close(fig)
        logger.info("Wrote %s", path)
        return path

    steps = [r["step_name"] for r in step_comp]

    # 1. Step duration
    p = _grouped_bar(
        f"Step Duration avg: {label_a} vs {label_b}",
        steps,
        [r.get(f"duration_avg_{label_a}") for r in step_comp],
        [r.get(f"duration_avg_{label_b}") for r in step_comp],
        "Duration avg (s)",
        "step_duration_comparison.png",
    )
    if p:
        generated["step_duration"] = p

    # 1b. Init overhead comparison (key metric for storage optimisations)
    init_a = [r.get(f"init_duration_avg_{label_a}") for r in step_comp]
    init_b = [r.get(f"init_duration_avg_{label_b}") for r in step_comp]
    if any(v is not None for v in init_a + init_b):
        p = _grouped_bar(
            f"Init Overhead avg: {label_a} vs {label_b}",
            steps,
            init_a,
            init_b,
            "Init duration avg (s)",
            "step_init_comparison.png",
        )
        if p:
            generated["step_init"] = p

    # 2. CPU peak
    p = _grouped_bar(
        f"CPU Peak avg: {label_a} vs {label_b}",
        steps,
        [r.get(f"cpu_peak_avg_{label_a}") for r in step_comp],
        [r.get(f"cpu_peak_avg_{label_b}") for r in step_comp],
        "CPU peak avg (milli-cores)",
        "step_cpu_comparison.png",
    )
    if p:
        generated["step_cpu"] = p

    # 3. Memory peak
    p = _grouped_bar(
        f"Memory Peak avg: {label_a} vs {label_b}",
        steps,
        [r.get(f"mem_peak_avg_{label_a}") for r in step_comp],
        [r.get(f"mem_peak_avg_{label_b}") for r in step_comp],
        "Mem peak avg (MiB)",
        "step_mem_comparison.png",
    )
    if p:
        generated["step_mem"] = p

    # 4. Pipeline phase stacked bar (exclude end-to-end which is a sum)
    phase_data = [
        (r["phase"], r.get(f"value_{label_a}") or 0, r.get(f"value_{label_b}") or 0)
        for r in pipeline_comp
        if "End-to-end" not in r["phase"]
    ]
    if phase_data:
        try:
            ph_labels, ph_a, ph_b = zip(*phase_data)
            colors = plt.cm.Set2(np.linspace(0, 1, len(ph_labels)))  # type: ignore[attr-defined]
            fig, ax = plt.subplots(figsize=(6, 5))
            bot_a = bot_b = 0.0
            for i, (ph, va, vb) in enumerate(zip(ph_labels, ph_a, ph_b)):
                ax.bar(0, va, bottom=bot_a, color=colors[i])
                ax.bar(1, vb, bottom=bot_b, color=colors[i], label=ph)
                bot_a += va
                bot_b += vb
            ax.set_xticks([0, 1])
            ax.set_xticklabels([label_a, label_b])
            ax.set_title(f"Pipeline Phase Breakdown: {label_a} vs {label_b}")
            ax.set_ylabel("Time (s)")
            ax.legend(loc="upper right", fontsize=8)
            fig.tight_layout()
            path = plots_dir / "pipeline_breakdown_comparison.png"
            fig.savefig(path, dpi=120)
            plt.close(fig)
            logger.info("Wrote %s", path)
            generated["pipeline_breakdown"] = path
        except Exception as exc:  # noqa: BLE001
            logger.warning("Could not generate pipeline breakdown plot: %s", exc)

    return generated
