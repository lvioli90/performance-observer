"""
reporting/plots.py
==================
Generates all diagnostic plots using matplotlib only (no seaborn).

Plots generated
---------------
1.  throughput_over_time.png        - products/min completed over time
2.  workflows_pending_over_time.png - pending workflow count over time
3.  workflows_running_over_time.png - running workflow count over time
4.  e2e_latency_over_time.png       - end-to-end p50/p95 rolling latency
5.  queue_time_over_time.png        - workflow queue time p50/p95 over time
6.  step_duration_comparison.png    - per-step duration box plot / bar
7.  step_cpu_peak.png               - per-step CPU peak (avg and max)
8.  step_mem_peak.png               - per-step memory peak (avg and max)

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
) -> Optional[Path]:
    """
    Compute rolling p50/p95 of end_to_end_sec binned into time windows,
    then plot.
    """
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
    step_kpis: List[dict], output_dir: Path
) -> Optional[Path]:
    """
    Horizontal bar chart: per-step p50 and p95 duration.
    """
    if not step_kpis:
        logger.warning("No step KPI data for duration plot")
        return None

    steps = [s["step_name"] for s in step_kpis]
    p50s = [_safe_float(s.get("duration_p50")) or 0 for s in step_kpis]
    p95s = [_safe_float(s.get("duration_p95")) or 0 for s in step_kpis]

    y = np.arange(len(steps))
    height = 0.35

    fig, ax = plt.subplots(figsize=(_FIG_WIDTH, max(4, len(steps) * 0.6 + 1)))
    ax.barh(y + height / 2, p50s, height, label="p50", color=_COLOR_P50, alpha=0.85)
    ax.barh(y - height / 2, p95s, height, label="p95", color=_COLOR_P95, alpha=0.85)
    ax.set_yticks(y)
    ax.set_yticklabels(steps)
    ax.set_xlabel("Duration (seconds)")
    ax.set_title("Per-Step Duration Comparison (p50 / p95)", fontsize=13, fontweight="bold")
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

def plot_step_cpu_peak(step_kpis: List[dict], output_dir: Path) -> Optional[Path]:
    """Bar chart: per-step CPU peak (avg of peaks and absolute max)."""
    if not step_kpis:
        return None

    steps = [s["step_name"] for s in step_kpis]
    avg_peaks = [_safe_float(s.get("cpu_peak_avg")) or 0 for s in step_kpis]
    max_peaks = [_safe_float(s.get("cpu_peak_max")) or 0 for s in step_kpis]

    y = np.arange(len(steps))
    height = 0.35

    fig, ax = plt.subplots(figsize=(_FIG_WIDTH, max(4, len(steps) * 0.6 + 1)))
    ax.barh(y + height / 2, avg_peaks, height, label="Avg peak CPU", color=_COLOR_BAR, alpha=0.85)
    ax.barh(y - height / 2, max_peaks, height, label="Max peak CPU", color=_COLOR_PEAK, alpha=0.85)
    ax.set_yticks(y)
    ax.set_yticklabels(steps)
    ax.set_xlabel("CPU (milli-cores)")
    ax.set_title("Per-Step CPU Peak", fontsize=13, fontweight="bold")
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

def plot_step_mem_peak(step_kpis: List[dict], output_dir: Path) -> Optional[Path]:
    """Bar chart: per-step memory peak (avg of peaks and absolute max) in MiB."""
    if not step_kpis:
        return None

    steps = [s["step_name"] for s in step_kpis]
    avg_peaks = [_safe_float(s.get("mem_peak_avg")) or 0 for s in step_kpis]
    max_peaks = [_safe_float(s.get("mem_peak_max")) or 0 for s in step_kpis]

    y = np.arange(len(steps))
    height = 0.35

    fig, ax = plt.subplots(figsize=(_FIG_WIDTH, max(4, len(steps) * 0.6 + 1)))
    ax.barh(y + height / 2, avg_peaks, height, label="Avg peak mem", color=_COLOR_BAR, alpha=0.85)
    ax.barh(y - height / 2, max_peaks, height, label="Max peak mem", color=_COLOR_PEAK, alpha=0.85)
    ax.set_yticks(y)
    ax.set_yticklabels(steps)
    ax.set_xlabel("Memory (MiB)")
    ax.set_title("Per-Step Memory Peak", fontsize=13, fontweight="bold")
    ax.legend()
    ax.grid(True, axis="x", alpha=0.3)
    fig.tight_layout()

    path = output_dir / "step_mem_peak.png"
    fig.savefig(path, dpi=_DPI)
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
    Run all eight plot generators and return a dict of name -> file path.
    """
    plots_dir = output_dir / "plots"
    plots_dir.mkdir(parents=True, exist_ok=True)

    results = {}
    results["throughput_over_time"] = plot_throughput_over_time(timeseries, plots_dir)
    results["workflows_pending_over_time"] = plot_workflows_pending_over_time(timeseries, plots_dir)
    results["workflows_running_over_time"] = plot_workflows_running_over_time(timeseries, plots_dir)
    results["e2e_latency_over_time"] = plot_e2e_latency_over_time(timeseries, products, plots_dir)
    results["queue_time_over_time"] = plot_queue_time_over_time(timeseries, plots_dir)
    results["step_duration_comparison"] = plot_step_duration_comparison(step_kpis, plots_dir)
    results["step_cpu_peak"] = plot_step_cpu_peak(step_kpis, plots_dir)
    results["step_mem_peak"] = plot_step_mem_peak(step_kpis, plots_dir)

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
