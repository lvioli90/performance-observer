"""
reporting/plots.py
==================
Generates all diagnostic plots using matplotlib only (no seaborn).

Plots generated
---------------
1.  throughput_over_time.png           - products/min completed over time
2.  workflows_pending_over_time.png    - pending workflow count over time
3.  workflows_running_over_time.png    - running workflow count over time
4.  e2e_latency_over_time.png          - end-to-end p50/p95 rolling latency
5.  queue_time_over_time.png           - workflow queue time p50/p95 over time
6.  step_duration_comparison.png       - per-step duration box plot / bar
7.  step_cpu_peak.png                  - per-step CPU peak (avg and max)
8.  step_mem_peak.png                  - per-step memory peak (avg and max)
9.  stac_publish_latency_histogram.png - distribution of stac_publish_sec
                                         (omnipass_finished → properties.updated)
10. pipeline_breakdown.png             - stacked bar per product showing time
                                         split: dispatcher_run + pipeline_gap +
                                         workflow_queue + workflow_run + stac_publish
11. step_init_overhead.png             - per-step overhead vs effective running time:
                                         scheduling | init (volume attach + image pull)
                                         | running — shown as absolute seconds (left)
                                         and as 100% normalised breakdown (right)

All plots are saved as PNG files in the output directory.
Matplotlib's Agg backend is used so no display is required.
"""

from __future__ import annotations

import logging
from collections import defaultdict
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import matplotlib
matplotlib.use("Agg")   # non-interactive backend, safe for headless runs
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
import numpy as np

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Style constants
# ---------------------------------------------------------------------------

_FIG_WIDTH = 12
_FIG_HEIGHT = 5
_DPI = 120
_COLOR_P50 = "#2196F3"   # blue
_COLOR_P95 = "#F44336"   # red
_COLOR_PENDING = "#FF9800"
_COLOR_RUNNING = "#4CAF50"
_COLOR_BAR = "#607D8B"
_COLOR_PEAK = "#E91E63"


# ---------------------------------------------------------------------------
# Parse helpers
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


def _safe_float(val) -> Optional[float]:
    try:
        f = float(val)
        return None if (f != f) else f  # NaN check
    except (TypeError, ValueError):
        return None


# ---------------------------------------------------------------------------
# Plot 1: Throughput over time
# ---------------------------------------------------------------------------

def plot_throughput_over_time(
    timeseries: List[dict], output_dir: Path
) -> Optional[Path]:
    """
    Plot throughput_last_5m over time from the timeseries data.
    """
    timestamps = []
    throughputs = []
    for row in timeseries:
        ts = _parse_dt(row.get("timestamp"))
        tp = _safe_float(row.get("throughput_last_5m"))
        if ts is not None and tp is not None:
            timestamps.append(ts)
            throughputs.append(tp)

    if not timestamps:
        logger.warning("No throughput data for plot")
        return None

    fig, ax = plt.subplots(figsize=(_FIG_WIDTH, _FIG_HEIGHT))
    ax.plot(timestamps, throughputs, color=_COLOR_RUNNING, linewidth=1.5, label="Throughput (last 5m)")
    ax.axhline(y=10.4, color=_COLOR_P95, linestyle="--", linewidth=1, label="Target (10.4/min)")
    ax.set_title("Product Throughput Over Time", fontsize=13, fontweight="bold")
    ax.set_xlabel("Time (UTC)")
    ax.set_ylabel("Products / minute")
    ax.legend()
    ax.xaxis.set_major_formatter(mdates.DateFormatter("%H:%M"))
    fig.autofmt_xdate()
    ax.grid(True, alpha=0.3)
    fig.tight_layout()

    path = output_dir / "throughput_over_time.png"
    fig.savefig(path, dpi=_DPI)
    plt.close(fig)
    logger.info("Saved %s", path)
    return path


# ---------------------------------------------------------------------------
# Plot 2: Workflows pending over time
# ---------------------------------------------------------------------------

def plot_workflows_pending_over_time(
    timeseries: List[dict], output_dir: Path
) -> Optional[Path]:
    timestamps = []
    pending = []
    for row in timeseries:
        ts = _parse_dt(row.get("timestamp"))
        v = _safe_float(row.get("workflows_pending"))
        if ts is not None and v is not None:
            timestamps.append(ts)
            pending.append(v)

    if not timestamps:
        logger.warning("No pending workflow data for plot")
        return None

    fig, ax = plt.subplots(figsize=(_FIG_WIDTH, _FIG_HEIGHT))
    ax.fill_between(timestamps, pending, alpha=0.3, color=_COLOR_PENDING)
    ax.plot(timestamps, pending, color=_COLOR_PENDING, linewidth=1.5, label="Pending workflows")
    ax.set_title("Pending Workflows Over Time", fontsize=13, fontweight="bold")
    ax.set_xlabel("Time (UTC)")
    ax.set_ylabel("Count")
    ax.legend()
    ax.xaxis.set_major_formatter(mdates.DateFormatter("%H:%M"))
    fig.autofmt_xdate()
    ax.grid(True, alpha=0.3)
    fig.tight_layout()

    path = output_dir / "workflows_pending_over_time.png"
    fig.savefig(path, dpi=_DPI)
    plt.close(fig)
    logger.info("Saved %s", path)
    return path


# ---------------------------------------------------------------------------
# Plot 3: Workflows running over time
# ---------------------------------------------------------------------------

def plot_workflows_running_over_time(
    timeseries: List[dict], output_dir: Path
) -> Optional[Path]:
    timestamps = []
    running = []
    for row in timeseries:
        ts = _parse_dt(row.get("timestamp"))
        v = _safe_float(row.get("workflows_running"))
        if ts is not None and v is not None:
            timestamps.append(ts)
            running.append(v)

    if not timestamps:
        logger.warning("No running workflow data for plot")
        return None

    fig, ax = plt.subplots(figsize=(_FIG_WIDTH, _FIG_HEIGHT))
    ax.fill_between(timestamps, running, alpha=0.3, color=_COLOR_RUNNING)
    ax.plot(timestamps, running, color=_COLOR_RUNNING, linewidth=1.5, label="Running workflows")
    ax.set_title("Running Workflows Over Time", fontsize=13, fontweight="bold")
    ax.set_xlabel("Time (UTC)")
    ax.set_ylabel("Count")
    ax.legend()
    ax.xaxis.set_major_formatter(mdates.DateFormatter("%H:%M"))
    fig.autofmt_xdate()
    ax.grid(True, alpha=0.3)
    fig.tight_layout()

    path = output_dir / "workflows_running_over_time.png"
    fig.savefig(path, dpi=_DPI)
    plt.close(fig)
    logger.info("Saved %s", path)
    return path


# ---------------------------------------------------------------------------
# Plot 4: End-to-end latency p50/p95 over time
# ---------------------------------------------------------------------------

def plot_e2e_latency_over_time(
    timeseries: List[dict],
    products: List[dict],
    output_dir: Path,
    window_minutes: int = 5,
    single_product: bool = False,
) -> Optional[Path]:
    """
    Compute rolling p50/p95 of end_to_end_sec binned into time windows,
    then plot.
    """
    if single_product:
        logger.info("Skipping e2e_latency_over_time: not meaningful for a single-product run")
        return None

    # Build (finished_at, end_to_end_sec) pairs
    data_points: List[Tuple[datetime, float]] = []
    for p in products:
        ts = _parse_dt(p.get("workflow_finished_at")) or _parse_dt(p.get("stac_seen_at"))
        val = _safe_float(p.get("end_to_end_sec"))
        if ts is not None and val is not None and val > 0:
            data_points.append((ts, val))

    if not data_points:
        logger.warning("No end-to-end latency data for plot")
        return None

    data_points.sort(key=lambda x: x[0])
    bin_ts, p50s, p95s = _rolling_percentiles(data_points, window_minutes)

    if not bin_ts:
        logger.warning("Insufficient data for rolling E2E latency plot")
        return None

    fig, ax = plt.subplots(figsize=(_FIG_WIDTH, _FIG_HEIGHT))
    ax.plot(bin_ts, p50s, color=_COLOR_P50, linewidth=1.5, label="E2E p50")
    ax.plot(bin_ts, p95s, color=_COLOR_P95, linewidth=1.5, linestyle="--", label="E2E p95")
    ax.set_title("End-to-End Latency Over Time (p50 / p95)", fontsize=13, fontweight="bold")
    ax.set_xlabel("Time (UTC)")
    ax.set_ylabel("Latency (seconds)")
    ax.legend()
    ax.xaxis.set_major_formatter(mdates.DateFormatter("%H:%M"))
    fig.autofmt_xdate()
    ax.grid(True, alpha=0.3)
    fig.tight_layout()

    path = output_dir / "e2e_latency_over_time.png"
    fig.savefig(path, dpi=_DPI)
    plt.close(fig)
    logger.info("Saved %s", path)
    return path


# ---------------------------------------------------------------------------
# Plot 5: Workflow queue time p50/p95 over time
# ---------------------------------------------------------------------------

def plot_queue_time_over_time(
    timeseries: List[dict],
    output_dir: Path,
) -> Optional[Path]:
    """
    Plot avg_queue_time_last_5m and p95_queue_time_last_5m from timeseries.
    """
    timestamps, avg_vals, p95_vals = [], [], []
    for row in timeseries:
        ts = _parse_dt(row.get("timestamp"))
        avg = _safe_float(row.get("avg_queue_time_last_5m"))
        p95 = _safe_float(row.get("p95_queue_time_last_5m"))
        if ts is not None:
            timestamps.append(ts)
            avg_vals.append(avg)
            p95_vals.append(p95)

    if not timestamps:
        logger.warning("No queue time timeseries data for plot")
        return None

    fig, ax = plt.subplots(figsize=(_FIG_WIDTH, _FIG_HEIGHT))
    ax.plot(timestamps, avg_vals, color=_COLOR_P50, linewidth=1.5, label="Queue time avg")
    ax.plot(timestamps, p95_vals, color=_COLOR_P95, linewidth=1.5, linestyle="--", label="Queue time p95")
    ax.set_title("Workflow Queue Time Over Time (avg / p95)", fontsize=13, fontweight="bold")
    ax.set_xlabel("Time (UTC)")
    ax.set_ylabel("Queue time (seconds)")
    ax.legend()
    ax.xaxis.set_major_formatter(mdates.DateFormatter("%H:%M"))
    fig.autofmt_xdate()
    ax.grid(True, alpha=0.3)
    fig.tight_layout()

    path = output_dir / "queue_time_over_time.png"
    fig.savefig(path, dpi=_DPI)
    plt.close(fig)
    logger.info("Saved %s", path)
    return path


# ---------------------------------------------------------------------------
# Plot 6: Per-step duration comparison
# ---------------------------------------------------------------------------

def plot_step_duration_comparison(
    step_kpis: List[dict], output_dir: Path, single_product: bool = False
) -> Optional[Path]:
    """
    Horizontal bar chart: per-step duration.
    Single-product run: one bar per step labelled "Observed".
    Multi-product run: two bars per step (p50 / p95).
    """
    if not step_kpis:
        logger.warning("No step KPI data for duration plot")
        return None

    steps = [s["step_name"] for s in step_kpis]
    y = np.arange(len(steps))

    fig, ax = plt.subplots(figsize=(_FIG_WIDTH, max(4, len(steps) * 0.6 + 1)))

    if single_product:
        vals = [_safe_float(s.get("duration_p50")) or 0 for s in step_kpis]
        ax.barh(y, vals, 0.55, label="Observed", color=_COLOR_P50, alpha=0.85)
        for i, v in enumerate(vals):
            if v >= 1:
                ax.text(v + 0.3, i, f"{v:.0f}s", va="center", fontsize=8)
        ax.set_title("Per-Step Duration — Single Run", fontsize=13, fontweight="bold")
    else:
        height = 0.35
        p50s = [_safe_float(s.get("duration_p50")) or 0 for s in step_kpis]
        p95s = [_safe_float(s.get("duration_p95")) or 0 for s in step_kpis]
        ax.barh(y + height / 2, p50s, height, label="p50", color=_COLOR_P50, alpha=0.85)
        ax.barh(y - height / 2, p95s, height, label="p95", color=_COLOR_P95, alpha=0.85)
        ax.set_title("Per-Step Duration Comparison (p50 / p95)", fontsize=13, fontweight="bold")

    ax.set_yticks(y)
    ax.set_yticklabels(steps)
    ax.set_xlabel("Duration (seconds)")
    ax.legend()
    ax.grid(True, axis="x", alpha=0.3)
    fig.tight_layout()

    path = output_dir / "step_duration_comparison.png"
    fig.savefig(path, dpi=_DPI)
    plt.close(fig)
    logger.info("Saved %s", path)
    return path


# ---------------------------------------------------------------------------
# Plot 7: CPU peak per step
# ---------------------------------------------------------------------------

def plot_step_cpu_peak(
    step_kpis: List[dict], output_dir: Path, single_product: bool = False
) -> Optional[Path]:
    """Bar chart: per-step CPU peak."""
    if not step_kpis:
        return None

    steps = [s["step_name"] for s in step_kpis]
    y = np.arange(len(steps))

    fig, ax = plt.subplots(figsize=(_FIG_WIDTH, max(4, len(steps) * 0.6 + 1)))

    if single_product:
        # avg_peak == max_peak for a single run; show one bar
        peaks = [_safe_float(s.get("cpu_peak_avg")) or 0 for s in step_kpis]
        ax.barh(y, peaks, 0.55, label="Observed CPU peak", color=_COLOR_BAR, alpha=0.85)
        ax.set_title("Per-Step CPU Peak — Single Run", fontsize=13, fontweight="bold")
    else:
        height = 0.35
        avg_peaks = [_safe_float(s.get("cpu_peak_avg")) or 0 for s in step_kpis]
        max_peaks = [_safe_float(s.get("cpu_peak_max")) or 0 for s in step_kpis]
        ax.barh(y + height / 2, avg_peaks, height, label="Avg peak CPU", color=_COLOR_BAR, alpha=0.85)
        ax.barh(y - height / 2, max_peaks, height, label="Max peak CPU", color=_COLOR_PEAK, alpha=0.85)
        ax.set_title("Per-Step CPU Peak", fontsize=13, fontweight="bold")

    ax.set_yticks(y)
    ax.set_yticklabels(steps)
    ax.set_xlabel("CPU (milli-cores)")
    ax.legend()
    ax.grid(True, axis="x", alpha=0.3)
    fig.tight_layout()

    path = output_dir / "step_cpu_peak.png"
    fig.savefig(path, dpi=_DPI)
    plt.close(fig)
    logger.info("Saved %s", path)
    return path


# ---------------------------------------------------------------------------
# Plot 8: Memory peak per step
# ---------------------------------------------------------------------------

def plot_step_mem_peak(
    step_kpis: List[dict], output_dir: Path, single_product: bool = False
) -> Optional[Path]:
    """Bar chart: per-step memory peak in MiB."""
    if not step_kpis:
        return None

    steps = [s["step_name"] for s in step_kpis]
    y = np.arange(len(steps))

    fig, ax = plt.subplots(figsize=(_FIG_WIDTH, max(4, len(steps) * 0.6 + 1)))

    if single_product:
        peaks = [_safe_float(s.get("mem_peak_avg")) or 0 for s in step_kpis]
        ax.barh(y, peaks, 0.55, label="Observed mem peak", color=_COLOR_BAR, alpha=0.85)
        ax.set_title("Per-Step Memory Peak — Single Run", fontsize=13, fontweight="bold")
    else:
        height = 0.35
        avg_peaks = [_safe_float(s.get("mem_peak_avg")) or 0 for s in step_kpis]
        max_peaks = [_safe_float(s.get("mem_peak_max")) or 0 for s in step_kpis]
        ax.barh(y + height / 2, avg_peaks, height, label="Avg peak mem", color=_COLOR_BAR, alpha=0.85)
        ax.barh(y - height / 2, max_peaks, height, label="Max peak mem", color=_COLOR_PEAK, alpha=0.85)
        ax.set_title("Per-Step Memory Peak", fontsize=13, fontweight="bold")

    ax.set_yticks(y)
    ax.set_yticklabels(steps)
    ax.set_xlabel("Memory (MiB)")
    ax.legend()
    ax.grid(True, axis="x", alpha=0.3)
    fig.tight_layout()

    path = output_dir / "step_mem_peak.png"
    fig.savefig(path, dpi=_DPI)
    plt.close(fig)
    logger.info("Saved %s", path)
    return path


# ---------------------------------------------------------------------------
# Plot 9: STAC publish latency histogram
# ---------------------------------------------------------------------------

def plot_stac_publish_latency_histogram(
    products: List[dict], output_dir: Path, single_product: bool = False
) -> Optional[Path]:
    """
    Histogram of stac_publish_sec (omnipass_finished → properties.updated).

    This is the central KPI for re-ingestion: how long after the ingestor
    completes does the STAC catalog reflect the updated item?

    Vertical lines mark p50, p95, p99.
    """
    if single_product:
        logger.info("Skipping stac_publish_latency_histogram: not meaningful for a single-product run")
        return None

    values = [
        _safe_float(p.get("stac_publish_sec"))
        for p in products
        if _safe_float(p.get("stac_publish_sec")) is not None
        and _safe_float(p.get("stac_publish_sec")) >= 0
    ]

    if not values:
        logger.warning("No stac_publish_sec data for histogram")
        return None

    arr = np.array(values)
    p50 = float(np.percentile(arr, 50))
    p95 = float(np.percentile(arr, 95))
    p99 = float(np.percentile(arr, 99))

    fig, ax = plt.subplots(figsize=(_FIG_WIDTH, _FIG_HEIGHT))

    n_bins = min(50, max(10, len(values) // 3))
    ax.hist(arr, bins=n_bins, color=_COLOR_BAR, alpha=0.75, edgecolor="white", linewidth=0.5)

    ax.axvline(p50, color=_COLOR_P50, linewidth=1.8, linestyle="-",
               label=f"p50 = {p50:.1f}s")
    ax.axvline(p95, color=_COLOR_P95, linewidth=1.8, linestyle="--",
               label=f"p95 = {p95:.1f}s")
    ax.axvline(p99, color="#9C27B0", linewidth=1.8, linestyle=":",
               label=f"p99 = {p99:.1f}s")

    ax.set_title(
        "STAC Publish Latency Distribution\n"
        "(omnipass_finished → properties.updated)",
        fontsize=13, fontweight="bold",
    )
    ax.set_xlabel("stac_publish_sec (seconds)")
    ax.set_ylabel("Number of products")
    ax.legend()
    ax.grid(True, axis="y", alpha=0.3)
    fig.tight_layout()

    path = output_dir / "stac_publish_latency_histogram.png"
    fig.savefig(path, dpi=_DPI)
    plt.close(fig)
    logger.info("Saved %s", path)
    return path


# ---------------------------------------------------------------------------
# Plot 10: Pipeline time breakdown (stacked bar per product)
# ---------------------------------------------------------------------------

def plot_pipeline_breakdown(
    products: List[dict], output_dir: Path, max_products: int = 60
) -> Optional[Path]:
    """
    Stacked horizontal bar chart showing how total time is split across the
    five pipeline phases for each product:

      dispatcher_run  → pipeline_gap (Kafka latency)
                      → workflow_queue (semaphore wait)
                      → workflow_run (calrissian)
                      → stac_publish (catalog update)

    Products are sorted by end_to_end_sec descending so the slowest appear
    at the top.  Capped at ``max_products`` for readability.
    """
    # Only include succeeded products with at least partial timing
    candidates = [
        p for p in products
        if p.get("final_status") == "succeeded"
        and any(
            _safe_float(p.get(f)) is not None
            for f in ("workflow_run_sec", "stac_publish_sec")
        )
    ]

    if not candidates:
        logger.warning("No succeeded products with timing data for pipeline breakdown")
        return None

    # Sort by end_to_end_sec desc; fall back to workflow_run_sec
    def _sort_key(p):
        v = _safe_float(p.get("end_to_end_sec")) or _safe_float(p.get("workflow_run_sec")) or 0
        return v

    candidates.sort(key=_sort_key, reverse=True)
    if len(candidates) > max_products:
        candidates = candidates[:max_products]

    # Labels: use last segment of product_id for readability
    labels = [str(p.get("product_id", "?"))[-24:] for p in candidates]

    phases = [
        ("dispatcher_run_sec",  "Dispatcher run",    "#42A5F5"),  # light blue
        ("pipeline_gap_sec",    "Kafka latency",      "#FFA726"),  # orange
        ("workflow_queue_sec",  "Semaphore wait",     "#EF5350"),  # red
        ("workflow_run_sec",    "Calrissian (argo-cwl)", "#66BB6A"),  # green
        ("stac_publish_sec",    "STAC publish",       "#AB47BC"),  # purple
    ]

    # Build matrix: rows=products, cols=phases
    data = []
    for field, _, _ in phases:
        col = [max(0.0, _safe_float(p.get(field)) or 0.0) for p in candidates]
        data.append(col)

    n = len(candidates)
    fig_height = max(5, n * 0.28 + 1.5)
    fig, ax = plt.subplots(figsize=(_FIG_WIDTH, fig_height))

    y = np.arange(n)
    lefts = np.zeros(n)

    for (field, label, color), col in zip(phases, data):
        col_arr = np.array(col)
        bars = ax.barh(y, col_arr, left=lefts, label=label, color=color, alpha=0.85)
        # Annotate non-trivial segments (>2s) with their value
        for i, (val, left) in enumerate(zip(col_arr, lefts)):
            if val >= 2:
                ax.text(
                    left + val / 2, i, f"{val:.0f}s",
                    ha="center", va="center", fontsize=6.5, color="white", fontweight="bold",
                )
        lefts = lefts + col_arr

    ax.set_yticks(y)
    ax.set_yticklabels(labels, fontsize=7)
    ax.set_xlabel("Elapsed time (seconds)")
    ax.set_title(
        "Pipeline Time Breakdown per Product\n"
        "(dispatcher → Kafka → semaphore wait → calrissian → STAC publish)",
        fontsize=12, fontweight="bold",
    )
    ax.legend(loc="lower right", fontsize=8)
    ax.grid(True, axis="x", alpha=0.25)
    fig.tight_layout()

    path = output_dir / "pipeline_breakdown.png"
    fig.savefig(path, dpi=_DPI)
    plt.close(fig)
    logger.info("Saved %s", path)
    return path


# ---------------------------------------------------------------------------
# Plot 11: Per-step init overhead vs effective running time
# ---------------------------------------------------------------------------

_COLOR_SCHEDULING = "#90A4AE"   # blue-grey  — K8s scheduling
_COLOR_INIT       = "#FF7043"   # deep orange — init containers (vol attach + pull)
_COLOR_VOL_ATTACH = "#B71C1C"   # dark red   — volume attach sub-component
_COLOR_RUNNING_EFF = "#66BB6A"  # green      — main container (effective work)
_OVERHEAD_THRESHOLD = 0.50      # draw a warning line at 50 % overhead


def plot_step_init_overhead(
    step_kpis: List[dict], output_dir: Path, single_product: bool = False
) -> Optional[Path]:
    """
    Dual-panel chart showing, for each Argo step, how total p50 wall-clock
    time is split between scheduling overhead, init-container overhead, and
    effective main-container execution.

    Left panel  — absolute seconds (stacked bar).
    Right panel — 100 % normalised breakdown; a vertical dashed line at 50 %
                  separates "mostly overhead" from "mostly work".

    Segments
    --------
    scheduling   (gray)        pod_scheduled_at − pod_created_at
    volume_attach (dark red)   SuccessfulAttachVolume − pod_scheduled_at
                               (only when K8s Events were collected)
    init_rest     (orange)     remaining init time after volume attach
                               (= init_duration_p50 − volume_attach_p50)
    running      (green)       main container execution

    When volume_attach_p50 is None the whole init bar is shown in orange
    without sub-splitting.

    Steps with no timing data at all are omitted.
    Steps are sorted by overhead fraction (scheduling + init) / total,
    highest overhead at the top.
    """
    if not step_kpis:
        logger.warning("No step KPI data for init overhead plot")
        return None

    # ------------------------------------------------------------------ #
    # Build per-step data                                                  #
    # ------------------------------------------------------------------ #
    rows = []
    for s in step_kpis:
        wtype = str(s.get("workflow_type") or "unknown").lower()
        sname = str(s.get("step_name") or "")

        # Show only omnipass ingestor steps.
        # When workflow_type classification is broken (all "unknown"), fall back
        # to excluding the obvious dispatcher-only steps by name so the chart
        # remains usable.
        _DISPATCHER_ONLY = {"send-message", "send-message-success", "send-message-failure"}
        if wtype not in ("omnipass", "unknown"):
            continue          # keep only omnipass (and unclassified as fallback)
        if wtype == "dispatcher":
            continue
        if not sname or sname == "unknown":
            continue          # skip the unresolved-step bucket
        if sname in _DISPATCHER_ONLY:
            continue

        label = sname          # clean label: step name only, no workflow_type prefix
        sched  = max(0.0, _safe_float(s.get("scheduling_p50"))      or 0.0)
        init   = max(0.0, _safe_float(s.get("init_duration_p50"))   or 0.0)
        run    = max(0.0, _safe_float(s.get("running_p50"))         or 0.0)
        vol    = _safe_float(s.get("volume_attach_p50"))

        total = sched + init + run
        if total < 0.5:          # skip steps with no timing at all
            continue

        if vol is not None:
            vol = max(0.0, min(vol, init))   # clamp to init window
            init_rest = max(0.0, init - vol)
        else:
            vol = None
            init_rest = init

        overhead = sched + init
        overhead_pct = overhead / total if total > 0 else 0.0

        rows.append({
            "label":        label,
            "sched":        sched,
            "vol":          vol,          # None when events not collected
            "init_rest":    init_rest,
            "run":          run,
            "total":        total,
            "overhead_pct": overhead_pct,
        })

    if not rows:
        logger.warning("No step data with sufficient timing for init overhead plot")
        return None

    # Sort: highest overhead fraction at top
    rows.sort(key=lambda r: r["overhead_pct"], reverse=True)

    labels       = [r["label"]       for r in rows]
    sched_vals   = np.array([r["sched"]        for r in rows])
    vol_vals     = np.array([r["vol"] if r["vol"] is not None else 0.0 for r in rows])
    init_vals    = np.array([r["init_rest"]    for r in rows])
    run_vals     = np.array([r["run"]          for r in rows])
    totals       = np.array([r["total"]        for r in rows])
    has_vol      = [r["vol"] is not None       for r in rows]

    # vol_attach data availability drives legend / subtitle wording
    any_vol_data = any(has_vol)
    init_label   = "Vol attach + init-ctr" if any_vol_data else "Init overhead"

    n = len(rows)
    fig_h = max(4.5, n * 0.70 + 2.5)
    fig, (ax_abs, ax_pct) = plt.subplots(
        1, 2,
        figsize=(16, fig_h),
        gridspec_kw={"width_ratios": [1, 1]},
    )
    y = np.arange(n)
    bar_h = 0.55

    # ------------------------------------------------------------------ #
    # Helper: add value label inside a bar segment                         #
    # ------------------------------------------------------------------ #
    def _label_abs(ax, lefts, vals, fmt="{:.0f}s", min_width=2.0, **txt_kw):
        for i, (l, v) in enumerate(zip(lefts, vals)):
            if v >= min_width:
                ax.text(
                    l + v / 2, i, fmt.format(v),
                    ha="center", va="center", fontsize=7,
                    color="white", fontweight="bold", **txt_kw
                )

    def _label_pct(ax, lefts, vals, totals, min_pct=0.05, **txt_kw):
        for i, (l, v, tot) in enumerate(zip(lefts, vals, totals)):
            pct = v / tot if tot > 0 else 0
            if pct >= min_pct:
                ax.text(
                    l + pct / 2, i, f"{pct:.0%}",
                    ha="center", va="center", fontsize=7,
                    color="white", fontweight="bold", **txt_kw
                )

    # ================================================================== #
    # Left panel — absolute seconds                                        #
    # ================================================================== #
    lefts = np.zeros(n)

    # scheduling
    ax_abs.barh(y, sched_vals, bar_h, left=lefts,
                label="Scheduling", color=_COLOR_SCHEDULING, alpha=0.9)
    _label_abs(ax_abs, lefts, sched_vals)
    lefts += sched_vals

    # volume attach (sub-segment of init, only when events available)
    vol_mask = np.array([1.0 if hv else 0.0 for hv in has_vol])
    vol_plot = vol_vals * vol_mask
    if vol_plot.sum() > 0:
        ax_abs.barh(y, vol_plot, bar_h, left=lefts,
                    label="Volume attach", color=_COLOR_VOL_ATTACH, alpha=0.9)
        _label_abs(ax_abs, lefts, vol_plot)
        lefts += vol_plot

    # remaining init (full init_duration when vol_attach not available)
    ax_abs.barh(y, init_vals, bar_h, left=lefts,
                label=init_label, color=_COLOR_INIT, alpha=0.9)
    _label_abs(ax_abs, lefts, init_vals)
    lefts += init_vals

    # running
    ax_abs.barh(y, run_vals, bar_h, left=lefts,
                label="Running (effective work)", color=_COLOR_RUNNING_EFF, alpha=0.9)
    _label_abs(ax_abs, lefts, run_vals)

    ax_abs.set_yticks(y)
    ax_abs.set_yticklabels(labels, fontsize=8)
    dur_label = "observed duration (seconds)" if single_product else "p50 duration (seconds)"
    ax_abs.set_xlabel(dur_label, fontsize=9)
    ax_abs.set_title(
        "Absolute duration (observed)" if single_product else "Absolute duration (p50)",
        fontsize=10, fontweight="bold",
    )
    ax_abs.grid(True, axis="x", alpha=0.25)
    ax_abs.legend(fontsize=7, loc="lower right")

    # ================================================================== #
    # Right panel — 100 % normalised                                       #
    # ================================================================== #
    lefts_pct = np.zeros(n)

    sched_pct   = np.where(totals > 0, sched_vals  / totals, 0.0)
    vol_pct     = np.where(totals > 0, vol_plot     / totals, 0.0)
    init_pct    = np.where(totals > 0, init_vals    / totals, 0.0)
    run_pct     = np.where(totals > 0, run_vals     / totals, 0.0)

    ax_pct.barh(y, sched_pct, bar_h, left=lefts_pct,
                color=_COLOR_SCHEDULING, alpha=0.9)
    _label_pct(ax_pct, lefts_pct, sched_pct, np.ones(n))
    lefts_pct += sched_pct

    if vol_pct.sum() > 0:
        ax_pct.barh(y, vol_pct, bar_h, left=lefts_pct,
                    color=_COLOR_VOL_ATTACH, alpha=0.9)
        _label_pct(ax_pct, lefts_pct, vol_pct, np.ones(n))
        lefts_pct += vol_pct

    ax_pct.barh(y, init_pct, bar_h, left=lefts_pct,
                color=_COLOR_INIT, alpha=0.9)
    _label_pct(ax_pct, lefts_pct, init_pct, np.ones(n))
    lefts_pct += init_pct

    ax_pct.barh(y, run_pct, bar_h, left=lefts_pct,
                color=_COLOR_RUNNING_EFF, alpha=0.9)
    _label_pct(ax_pct, lefts_pct, run_pct, np.ones(n))

    # 50 % overhead warning line
    ax_pct.axvline(
        _OVERHEAD_THRESHOLD, color="#E53935", linewidth=1.4, linestyle="--",
        label=f"{_OVERHEAD_THRESHOLD:.0%} overhead threshold",
    )

    # Annotate the overhead % on the right margin
    for i, r in enumerate(rows):
        pct = r["overhead_pct"]
        color = "#B71C1C" if pct > _OVERHEAD_THRESHOLD else "#2E7D32"
        ax_pct.text(
            1.02, i, f"{pct:.0%} overhead",
            va="center", ha="left", fontsize=7.5,
            color=color, fontweight="bold",
            transform=ax_pct.get_yaxis_transform(),
        )

    ax_pct.set_xlim(0, 1)
    ax_pct.set_xticks([0, 0.25, 0.5, 0.75, 1.0])
    ax_pct.set_xticklabels(["0%", "25%", "50%", "75%", "100%"], fontsize=8)
    ax_pct.set_yticks(y)
    ax_pct.set_yticklabels([])       # labels already on left panel
    ax_pct.set_xlabel("Fraction of total p50 duration", fontsize=9)
    ax_pct.set_title(
        "Overhead vs effective work (%) — single run" if single_product else
        "Overhead vs effective work (%)\nsorted by overhead fraction — highest at top",
        fontsize=10, fontweight="bold",
    )
    ax_pct.legend(fontsize=7, loc="lower right")
    ax_pct.grid(True, axis="x", alpha=0.25)

    if any_vol_data:
        subtitle = "scheduling (gray) | volume attach (dark red) | init-ctr (orange) | running (green)"
    else:
        subtitle = "scheduling (gray) | init overhead (orange) | running (green)  —  enable K8s Events for vol-attach breakdown"
    fig.suptitle(
        f"Per-Step Init Overhead Analysis\n{subtitle}",
        fontsize=11, fontweight="bold", y=1.01,
    )
    fig.tight_layout()

    path = output_dir / "step_init_overhead.png"
    fig.savefig(path, dpi=_DPI, bbox_inches="tight")
    plt.close(fig)
    logger.info("Saved %s", path)
    return path


# ---------------------------------------------------------------------------
# Generate all plots
# ---------------------------------------------------------------------------

def generate_all_plots(
    output_dir: Path,
    timeseries: List[dict],
    products: List[dict],
    step_kpis: List[dict],
) -> Dict[str, Optional[Path]]:
    """
    Run all plot generators and return a dict of name -> file path.

    Automatically detects single-product runs (≤ 1 completed product) and
    adapts the plots accordingly:
    - Percentile comparisons (p50/p95) are replaced with "Observed" single bars
    - Rolling-window and histogram plots are skipped (not meaningful with 1 data point)
    - Labels change from "p50 duration" to "observed duration"
    """
    plots_dir = output_dir / "plots"
    plots_dir.mkdir(parents=True, exist_ok=True)

    # Detect single-product run: ≤ 1 completed product OR all steps have ≤ 1 execution
    n_completed = sum(
        1 for p in products
        if str(p.get("final_status", "")).lower() in ("succeeded", "success", "completed", "1", "true")
        or _safe_float(p.get("workflow_run_sec")) is not None
    )
    # Fallback: use step execution counts when product records are thin
    max_step_executions = max(
        (int(s.get("total_executions") or 1) for s in step_kpis), default=1
    )
    single = (n_completed <= 1) and (max_step_executions <= 2)
    if single:
        logger.info(
            "Single-product run detected (%d completed products, max step executions=%d) "
            "— p95 metrics omitted from plots",
            n_completed, max_step_executions,
        )

    results = {}
    results["throughput_over_time"]          = plot_throughput_over_time(timeseries, plots_dir)
    results["workflows_pending_over_time"]   = plot_workflows_pending_over_time(timeseries, plots_dir)
    results["workflows_running_over_time"]   = plot_workflows_running_over_time(timeseries, plots_dir)
    results["e2e_latency_over_time"]         = plot_e2e_latency_over_time(timeseries, products, plots_dir, single_product=single)
    results["queue_time_over_time"]          = plot_queue_time_over_time(timeseries, plots_dir)
    results["step_duration_comparison"]      = plot_step_duration_comparison(step_kpis, plots_dir, single_product=single)
    results["step_cpu_peak"]                 = plot_step_cpu_peak(step_kpis, plots_dir, single_product=single)
    results["step_mem_peak"]                 = plot_step_mem_peak(step_kpis, plots_dir, single_product=single)
    results["stac_publish_latency_histogram"]= plot_stac_publish_latency_histogram(products, plots_dir, single_product=single)
    results["pipeline_breakdown"]            = plot_pipeline_breakdown(products, plots_dir)
    results["step_init_overhead"]            = plot_step_init_overhead(step_kpis, plots_dir, single_product=single)

    generated = sum(1 for v in results.values() if v is not None)
    logger.info("Generated %d/%d plots in %s", generated, len(results), plots_dir)
    return results


# ---------------------------------------------------------------------------
# Rolling percentile helper
# ---------------------------------------------------------------------------

def _rolling_percentiles(
    data_points: List[Tuple[datetime, float]],
    window_minutes: int = 5,
) -> Tuple[List[datetime], List[float], List[float]]:
    """
    Compute rolling p50 / p95 for a list of (timestamp, value) pairs.

    Bins are placed at each data point's timestamp; the window includes all
    points within ``window_minutes`` before that timestamp.
    """
    from datetime import timedelta

    window = timedelta(minutes=window_minutes)
    bin_ts, p50s, p95s = [], [], []

    for i, (ts, _) in enumerate(data_points):
        window_vals = [v for t, v in data_points if 0 <= (ts - t).total_seconds() <= window.total_seconds()]
        if len(window_vals) >= 2:
            bin_ts.append(ts)
            p50s.append(float(np.percentile(window_vals, 50)))
            p95s.append(float(np.percentile(window_vals, 95)))

    return bin_ts, p50s, p95s
