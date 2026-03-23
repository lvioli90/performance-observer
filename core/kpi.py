"""
core/kpi.py
===========
KPI computation engine.

Takes the de-duplicated raw records loaded from NDJSON files and computes:
  - Business KPIs (throughput, success rate, end-to-end latency)
  - Workflow KPIs (queue time, concurrency, duration)
  - Pod/Step KPIs (per-step duration, pending, CPU, memory, failures)
  - Bottleneck diagnosis hints

All inputs are plain Python dicts (as loaded from NDJSON) so this module does
NOT depend on any network client.  It can be run stand-alone via compute_kpis.py.
"""

from __future__ import annotations

import logging
import math
from collections import defaultdict
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

import numpy as np

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _parse_dt(val) -> Optional[datetime]:
    """Parse an ISO datetime string (or passthrough datetime) to datetime."""
    if val is None:
        return None
    if isinstance(val, datetime):
        return val
    if isinstance(val, str):
        try:
            return datetime.fromisoformat(val)
        except ValueError:
            return None
    return None


def _delta_sec(start, end) -> Optional[float]:
    """Return (end - start).total_seconds() or None if either is missing."""
    s = _parse_dt(start)
    e = _parse_dt(end)
    if s is None or e is None:
        return None
    return (e - s).total_seconds()


def _percentile(values: List[float], p: float) -> Optional[float]:
    """Compute percentile p (0-100) from a list of floats. Returns None if empty."""
    if not values:
        return None
    return float(np.percentile(values, p))


def _safe_mean(values: List[float]) -> Optional[float]:
    if not values:
        return None
    return float(np.mean(values))


# ---------------------------------------------------------------------------
# Derived metrics computation for single records
# ---------------------------------------------------------------------------

def compute_product_derived(p: dict) -> dict:
    """
    Compute derived seconds fields for a product record dict (in-place).

    Two-workflow pipeline:
      dispatcher_queue_sec  = dispatcher started_at - created_at
      dispatcher_run_sec    = dispatcher finished_at - started_at
      workflow_queue_sec    = omnipass started_at - created_at   (the meaningful queue)
      workflow_run_sec      = omnipass finished_at - started_at  (the meaningful run)
      pipeline_gap_sec      = omnipass created_at - dispatcher finished_at
                              (Kafka trigger latency: time from dispatcher done to omnipass created)
      stac_publish_sec      = stac_seen_at - omnipass finished_at
      end_to_end_sec        = stac_seen_at - ingest_reference_time (= dispatcher created_at)
    """
    # Dispatcher timing
    p["dispatcher_queue_sec"] = _delta_sec(
        p.get("dispatcher_created_at"), p.get("dispatcher_started_at")
    )
    p["dispatcher_run_sec"] = _delta_sec(
        p.get("dispatcher_started_at"), p.get("dispatcher_finished_at")
    )
    # Pipeline gap (Kafka trigger latency): dispatcher_finished → omnipass_created
    p["pipeline_gap_sec"] = _delta_sec(
        p.get("dispatcher_finished_at"), p.get("workflow_created_at")
    )
    # Omnipass (heavy ingestor) timing
    p["workflow_queue_sec"] = _delta_sec(
        p.get("workflow_created_at"), p.get("workflow_started_at")
    )
    p["workflow_run_sec"] = _delta_sec(
        p.get("workflow_started_at"), p.get("workflow_finished_at")
    )
    # STAC publication latency (after omnipass finishes)
    p["stac_publish_sec"] = _delta_sec(
        p.get("workflow_finished_at"), p.get("stac_seen_at")
    )
    # Full end-to-end from earliest observable event
    p["end_to_end_sec"] = _delta_sec(
        p.get("ingest_reference_time"), p.get("stac_seen_at")
    )
    return p


def compute_pod_derived(pod: dict) -> dict:
    """Compute derived seconds fields for a pod record dict (in-place)."""
    pod["pod_pending_sec"] = _delta_sec(pod.get("pod_created_at"), pod.get("pod_started_at"))
    pod["pod_running_sec"] = _delta_sec(pod.get("pod_started_at"), pod.get("pod_finished_at"))
    return pod


def compute_workflow_derived(wf: dict) -> dict:
    """Compute derived seconds fields for a workflow record dict (in-place)."""
    wf["queue_sec"] = _delta_sec(wf.get("created_at"), wf.get("started_at"))
    wf["run_sec"] = _delta_sec(wf.get("started_at"), wf.get("finished_at"))
    return wf


# ---------------------------------------------------------------------------
# Main KPI computation
# ---------------------------------------------------------------------------

class KPIEngine:
    """
    Computes all KPIs from raw de-duplicated record lists.

    Parameters
    ----------
    workflows : list of dict  (de-duplicated, latest state per workflow_name)
    pods      : list of dict  (de-duplicated, latest state per pod_name)
    products  : list of dict  (de-duplicated, latest state per product_id)
    timeseries: list of dict  (ordered chronologically)
    run_started_at : datetime or None
    run_finished_at: datetime or None
    """

    def __init__(
        self,
        workflows: List[dict],
        pods: List[dict],
        products: List[dict],
        timeseries: List[dict],
        run_started_at: Optional[datetime] = None,
        run_finished_at: Optional[datetime] = None,
    ):
        # Compute derived fields in-place on copies so originals are not mutated
        self.workflows = [compute_workflow_derived(dict(w)) for w in workflows]
        self.pods = [compute_pod_derived(dict(p)) for p in pods]
        self.products = [compute_product_derived(dict(p)) for p in products]
        self.timeseries = timeseries
        self.run_started_at = run_started_at
        self.run_finished_at = run_finished_at or datetime.now(timezone.utc)

    # ------------------------------------------------------------------
    # Business KPIs
    # ------------------------------------------------------------------

    def business_kpis(self) -> dict:
        total_observed = len(self.products)
        completed = [p for p in self.products if p.get("final_status") == "succeeded"]
        failed = [p for p in self.products if p.get("final_status") == "failed"]
        total_completed = len(completed)
        total_failed = len(failed)

        success_rate = (total_completed / total_observed) if total_observed > 0 else None

        # Throughput: completed products per minute over total run window
        run_duration_min: Optional[float] = None
        if self.run_started_at:
            run_duration_min = (
                self.run_finished_at - self.run_started_at
            ).total_seconds() / 60.0

        avg_throughput = (
            (total_completed / run_duration_min) if run_duration_min and run_duration_min > 0 else None
        )

        # Rolling throughput (from timeseries)
        peak_throughput = self._peak_rolling_throughput()

        # End-to-end latency (only for succeeded products with stac_seen_at)
        e2e_values = [
            p["end_to_end_sec"]
            for p in completed
            if p.get("end_to_end_sec") is not None
        ]

        stac_latency_values = [
            p["stac_publish_sec"]
            for p in completed
            if p.get("stac_publish_sec") is not None
        ]

        return {
            "total_products_observed": total_observed,
            "total_products_completed": total_completed,
            "total_products_failed": total_failed,
            "success_rate": success_rate,
            "avg_throughput_per_min": avg_throughput,
            "peak_throughput_per_min": peak_throughput,
            "e2e_avg": _safe_mean(e2e_values),
            "e2e_p50": _percentile(e2e_values, 50),
            "e2e_p95": _percentile(e2e_values, 95),
            "e2e_p99": _percentile(e2e_values, 99),
            "stac_latency_avg": _safe_mean(stac_latency_values),
            "stac_latency_p95": _percentile(stac_latency_values, 95),
        }

    def _peak_rolling_throughput(self) -> Optional[float]:
        """Extract peak throughput_last_5m from timeseries."""
        values = [
            row.get("throughput_last_5m")
            for row in self.timeseries
            if row.get("throughput_last_5m") is not None
        ]
        return max(values) if values else None

    # ------------------------------------------------------------------
    # Workflow KPIs
    # ------------------------------------------------------------------

    def workflow_kpis(self) -> dict:
        """
        Compute workflow-level KPIs split by workflow type:
          - dispatcher: fast dispatch step (should be seconds)
          - omnipass:   heavy ingestor (minutes; the main performance signal)
          - pipeline_gap: Kafka trigger latency between dispatcher done and omnipass created
        """
        finished_wf = [
            w for w in self.workflows
            if w.get("phase") in ("Succeeded", "Failed", "Error")
        ]

        # Split by workflow type (using template_name)
        dispatcher_wf = [
            w for w in finished_wf
            if "dispatcher" in (w.get("template_name") or w.get("workflow_name", "")).lower()
        ]
        omnipass_wf = [
            w for w in finished_wf
            if "omnipass" in (w.get("template_name") or w.get("workflow_name", "")).lower()
        ]

        def _queue(wfs): return [w["queue_sec"] for w in wfs if w.get("queue_sec") is not None]
        def _run(wfs):   return [w["run_sec"] for w in wfs if w.get("run_sec") is not None]

        # Pipeline gap (Kafka trigger latency): from ProductRecord derived field
        gap_values = [
            p.get("pipeline_gap_sec")
            for p in self.products
            if p.get("pipeline_gap_sec") is not None
        ]

        # Max pending / running from timeseries
        max_pending = max(
            (row.get("workflows_pending", 0) for row in self.timeseries), default=0
        )
        max_running = max(
            (row.get("workflows_running", 0) for row in self.timeseries), default=0
        )
        running_vals = [row.get("workflows_running", 0) for row in self.timeseries]
        avg_concurrency = _safe_mean([float(v) for v in running_vals])

        # Combined (all workflows) for backward-compat
        all_queue = _queue(finished_wf)
        all_run = _run(finished_wf)

        return {
            # Combined (all workflow types)
            "queue_avg": _safe_mean(all_queue),
            "queue_p50": _percentile(all_queue, 50),
            "queue_p95": _percentile(all_queue, 95),
            "queue_p99": _percentile(all_queue, 99),
            "duration_avg": _safe_mean(all_run),
            "duration_p50": _percentile(all_run, 50),
            "duration_p95": _percentile(all_run, 95),
            "duration_p99": _percentile(all_run, 99),
            # Dispatcher-specific (fast steps)
            "dispatcher_queue_avg": _safe_mean(_queue(dispatcher_wf)),
            "dispatcher_queue_p95": _percentile(_queue(dispatcher_wf), 95),
            "dispatcher_duration_avg": _safe_mean(_run(dispatcher_wf)),
            "dispatcher_duration_p95": _percentile(_run(dispatcher_wf), 95),
            # Omnipass-specific (heavy ingestor)
            "omnipass_queue_avg": _safe_mean(_queue(omnipass_wf)),
            "omnipass_queue_p95": _percentile(_queue(omnipass_wf), 95),
            "omnipass_duration_avg": _safe_mean(_run(omnipass_wf)),
            "omnipass_duration_p95": _percentile(_run(omnipass_wf), 95),
            # Kafka trigger latency (dispatcher_finished → omnipass_created)
            "pipeline_gap_avg": _safe_mean(gap_values),
            "pipeline_gap_p95": _percentile(gap_values, 95),
            # Concurrency
            "max_pending_workflows": max_pending,
            "max_running_workflows": max_running,
            "avg_concurrency": avg_concurrency,
        }

    # ------------------------------------------------------------------
    # Pod / Step KPIs
    # ------------------------------------------------------------------

    def step_kpis(self) -> List[dict]:
        """
        Compute per-step KPIs.

        Returns a list of dicts, one per unique step_name observed.
        """
        by_step: Dict[str, List[dict]] = defaultdict(list)
        for pod in self.pods:
            step = pod.get("step_name") or "unknown"
            by_step[step].append(pod)

        results = []
        for step_name, step_pods in sorted(by_step.items()):
            total = len(step_pods)
            failed_pods = [p for p in step_pods if p.get("pod_phase") == "Failed"]
            retry_pods = [p for p in step_pods if (p.get("retries") or 0) > 0]
            oom_pods = [p for p in step_pods if p.get("oom_killed")]

            pending_vals = [p["pod_pending_sec"] for p in step_pods if p.get("pod_pending_sec") is not None]
            running_vals = [p["pod_running_sec"] for p in step_pods if p.get("pod_running_sec") is not None]
            # Wall-clock = pending + running: matches Argo UI node duration and
            # represents the step's true contribution to pipeline latency.
            wall_vals = [
                (p.get("pod_pending_sec") or 0) + (p.get("pod_running_sec") or 0)
                for p in step_pods
                if p.get("pod_pending_sec") is not None or p.get("pod_running_sec") is not None
            ]
            cpu_avg_vals = [p["cpu_avg"] for p in step_pods if p.get("cpu_avg") is not None]
            cpu_peak_vals = [p["cpu_peak"] for p in step_pods if p.get("cpu_peak") is not None]
            mem_avg_vals = [p["mem_avg"] for p in step_pods if p.get("mem_avg") is not None]
            mem_peak_vals = [p["mem_peak"] for p in step_pods if p.get("mem_peak") is not None]
            throttle_vals = [p["cpu_throttling"] for p in step_pods if p.get("cpu_throttling") is not None]

            results.append({
                "step_name": step_name,
                "total_executions": total,
                "failed_count": len(failed_pods),
                "failure_rate": len(failed_pods) / total if total > 0 else None,
                "retry_count": len(retry_pods),
                "retry_rate": len(retry_pods) / total if total > 0 else None,
                "oom_count": len(oom_pods),
                "oom_rate": len(oom_pods) / total if total > 0 else None,
                # Pending (scheduling delay, PVC mount, image pull)
                "pending_avg": _safe_mean(pending_vals),
                "pending_p95": _percentile(pending_vals, 95),
                # Running (container execution time only)
                "duration_avg": _safe_mean(running_vals),
                "duration_p50": _percentile(running_vals, 50),
                "duration_p95": _percentile(running_vals, 95),
                # Wall-clock = pending + running (matches Argo UI, use for bottleneck analysis)
                "wall_clock_avg": _safe_mean(wall_vals),
                "wall_clock_p50": _percentile(wall_vals, 50),
                "wall_clock_p95": _percentile(wall_vals, 95),
                # CPU (milli-cores)
                "cpu_avg": _safe_mean(cpu_avg_vals),
                "cpu_peak_avg": _safe_mean(cpu_peak_vals),
                "cpu_peak_max": max(cpu_peak_vals) if cpu_peak_vals else None,
                # Memory (MiB)
                "mem_avg": _safe_mean(mem_avg_vals),
                "mem_peak_avg": _safe_mean(mem_peak_vals),
                "mem_peak_max": max(mem_peak_vals) if mem_peak_vals else None,
                # Throttling ratio (avg across pods that have data)
                "cpu_throttling_avg": _safe_mean(throttle_vals),
                "throttling_incidence": (
                    sum(1 for v in throttle_vals if v > 0.1) / len(throttle_vals)
                    if throttle_vals else None
                ),
            })

        return results

    # ------------------------------------------------------------------
    # Bottleneck diagnosis
    # ------------------------------------------------------------------

    def bottleneck_hints(
        self,
        biz: dict,
        wf: dict,
        steps: List[dict],
    ) -> List[str]:
        """
        Produce human-readable diagnostic hints based on the computed KPIs.
        These correspond to CASES A–E from the project spec.
        """
        hints = []

        # CASE A: orchestration bottleneck
        if wf.get("max_pending_workflows", 0) > 5:
            hints.append(
                "CASE A (orchestration bottleneck): max_pending_workflows="
                f"{wf['max_pending_workflows']}. "
                "Argo controller may not be keeping up with workflow submissions. "
                "Consider increasing Argo controller parallelism or workflow concurrency limits."
            )
        if wf.get("queue_p95") and wf["queue_p95"] > 60:
            hints.append(
                f"CASE A: Workflow queue p95={wf['queue_p95']:.0f}s is high. "
                "Scheduling backlog detected."
            )

        # CASE B: pod execution bottleneck
        for step in steps:
            if step.get("wall_clock_p95") and step["wall_clock_p95"] > 120:
                hints.append(
                    f"CASE B (pod execution bottleneck): step '{step['step_name']}' "
                    f"wall_clock p95={step['wall_clock_p95']:.0f}s. "
                    "Check pod logic or external dependency latency."
                )

        # CASE C: CPU-bound
        for step in steps:
            if (
                step.get("cpu_peak_max") is not None
                and step.get("wall_clock_p95") is not None
                and step["cpu_peak_max"] > 800  # >800 milli-cores
                and step["wall_clock_p95"] > 60
            ):
                hints.append(
                    f"CASE C (CPU-bound): step '{step['step_name']}' "
                    f"cpu_peak_max={step['cpu_peak_max']:.0f}m, "
                    f"wall_clock_p95={step['wall_clock_p95']:.0f}s. "
                    "Consider increasing CPU limits or optimizing code."
                )
            if step.get("cpu_throttling_avg") and step["cpu_throttling_avg"] > 0.1:
                hints.append(
                    f"CASE C (CPU throttling): step '{step['step_name']}' "
                    f"throttling_avg={step['cpu_throttling_avg']:.1%}. "
                    "CPU limits may be too restrictive."
                )

        # CASE D: memory pressure
        for step in steps:
            if step.get("oom_count", 0) > 0:
                hints.append(
                    f"CASE D (memory pressure/OOM): step '{step['step_name']}' "
                    f"oom_count={step['oom_count']}. "
                    "Increase memory limits or investigate memory leaks."
                )

        # CASE E: external dependency bottleneck (low CPU, high wall-clock time)
        for step in steps:
            if (
                step.get("wall_clock_p95") is not None
                and step.get("cpu_peak_max") is not None
                and step["wall_clock_p95"] > 60
                and step["cpu_peak_max"] < 200  # <200 milli-cores
            ):
                hints.append(
                    f"CASE E (external dependency wait): step '{step['step_name']}' "
                    f"wall_clock_p95={step['wall_clock_p95']:.0f}s but "
                    f"cpu_peak_max={step['cpu_peak_max']:.0f}m (low). "
                    "Likely waiting on MinIO, STAC API, or another external service."
                )

        # Throughput target check (15,000/day = 625/hour = ~10.4/min)
        if biz.get("avg_throughput_per_min") is not None:
            target = 10.4
            actual = biz["avg_throughput_per_min"]
            if actual < target * 0.8:
                hints.append(
                    f"THROUGHPUT WARNING: avg throughput {actual:.1f}/min is below "
                    f"80% of target ({target:.1f}/min). "
                    "Platform may not sustain 15,000 products/day."
                )

        if not hints:
            hints.append(
                "No significant bottleneck patterns detected from available data."
            )

        return hints

    # ------------------------------------------------------------------
    # Top-level compute
    # ------------------------------------------------------------------

    def compute_all(self) -> dict:
        """Run all KPI computations and return a combined results dict."""
        biz = self.business_kpis()
        wf = self.workflow_kpis()
        steps = self.step_kpis()
        hints = self.bottleneck_hints(biz, wf, steps)

        return {
            "business": biz,
            "workflows": wf,
            "steps": steps,
            "bottleneck_hints": hints,
        }
