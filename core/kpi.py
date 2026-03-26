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
    pod["scheduling_sec"] = _delta_sec(pod.get("pod_created_at"), pod.get("pod_scheduled_at"))
    pod["init_duration_sec"] = _delta_sec(pod.get("pod_scheduled_at"), pod.get("init_done_at"))
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
    dispatcher_template : str
        Substring used to identify dispatcher workflows (default "ingestion-dispatcher").
    omnipass_template : str
        Substring used to identify omnipass workflows (default "ingestor-omnipass").
    stac_ref_steps : list of str
        Step names (in priority order) whose pod finish time is used as the
        reference for stac_latency.  The omnipass exit-handler runs AFTER STAC
        is published, so workflow_finished_at is unsuitable; the last main-workflow
        step (typically "post-results") is the correct reference.
    """

    def __init__(
        self,
        workflows: List[dict],
        pods: List[dict],
        products: List[dict],
        timeseries: List[dict],
        run_started_at: Optional[datetime] = None,
        run_finished_at: Optional[datetime] = None,
        dispatcher_template: str = "ingestion-dispatcher",
        omnipass_template: str = "ingestor-omnipass",
        stac_ref_steps: Optional[List[str]] = None,
    ):
        # Compute derived fields in-place on copies so originals are not mutated
        self.workflows = [compute_workflow_derived(dict(w)) for w in workflows]
        self.pods = [compute_pod_derived(dict(p)) for p in pods]
        self.products = [compute_product_derived(dict(p)) for p in products]
        self.timeseries = timeseries
        self.run_started_at = run_started_at
        self.run_finished_at = run_finished_at or datetime.now(timezone.utc)
        self.dispatcher_template = dispatcher_template.lower()
        self.omnipass_template = omnipass_template.lower()
        # Steps whose finish time marks "STAC has been posted" in the omnipass workflow.
        # The exit-handler steps (message-passthrough, send-message-success) run AFTER
        # STAC is published, so they must not be used as the reference.
        self.stac_ref_steps: List[str] = stac_ref_steps or [
            "post-results",
            "stage-out",
            "calrissian-argo-wf-runner",
        ]
        # Index: workflow_name → {step_name → latest pod_finished_at (datetime)}
        self._pod_finish_index: Dict[str, Dict[str, datetime]] = self._build_pod_finish_index()

    # ------------------------------------------------------------------
    # Pod finish index
    # ------------------------------------------------------------------

    def _build_pod_finish_index(self) -> Dict[str, Dict[str, datetime]]:
        """
        Build workflow_name → {step_name → latest pod_finished_at} mapping.

        Used by business_kpis() to resolve the correct stac_latency reference
        point (last main-workflow step finish) without relying on
        workflow_finished_at which, in pod-based mode, reflects exit-handler
        pod times rather than the end of the CWL computation.
        """
        index: Dict[str, Dict[str, datetime]] = defaultdict(dict)
        for pod in self.pods:
            wf = pod.get("workflow_name")
            step = pod.get("step_name")
            finished_str = pod.get("pod_finished_at")
            if not (wf and step and finished_str):
                continue
            finished = _parse_dt(finished_str)
            if finished is None:
                continue
            existing = index[wf].get(step)
            if existing is None or finished > existing:
                index[wf][step] = finished
        return dict(index)

    def _stac_ref_time(self, product: dict) -> Optional[datetime]:
        """
        Return the best available finish time of the last STAC-posting step
        for *product*.  Falls back to workflow_finished_at when no matching
        pod is found.
        """
        wf_name = product.get("workflow_name")
        wf_steps = self._pod_finish_index.get(wf_name or "") or {}
        for step in self.stac_ref_steps:
            t = wf_steps.get(step)
            if t is not None:
                return t
        # Fallback: workflow_finished_at from the product record itself
        return _parse_dt(product.get("workflow_finished_at"))

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

        # stac_latency = stac_seen_at - last_main_step_finish_time.
        # The omnipass exit-handler runs AFTER STAC is published (negative latency
        # if we used workflow_finished_at).  Use the pod finish time of the last
        # STAC-posting step (post-results / stage-out) as the reference instead.
        stac_latency_values = []
        for p in completed:
            stac_seen = _parse_dt(p.get("stac_seen_at"))
            if stac_seen is None:
                continue
            ref = self._stac_ref_time(p)
            if ref is None:
                continue
            val = (stac_seen - ref).total_seconds()
            stac_latency_values.append(val)

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
        Compute per-step KPIs, split by workflow type (dispatcher / omnipass).

        Returns a list of dicts, one per unique (workflow_type, step_name) pair,
        sorted by workflow_type then step_name.
        """
        # Group pods by (workflow_type, step_name)
        by_key: Dict[Tuple[str, str], List[dict]] = defaultdict(list)
        for pod in self.pods:
            step = pod.get("step_name") or "unknown"
            # Prefer the workflow_type already resolved during collection (set by
            # poll_pods); fall back to keyword-matching on workflow_name only when
            # the stored value is absent or still "unknown".
            wf_type = pod.get("workflow_type") or "unknown"
            if wf_type == "unknown":
                wf_name = (pod.get("workflow_name") or "").lower()
                if self.dispatcher_template and self.dispatcher_template in wf_name:
                    wf_type = "dispatcher"
                elif self.omnipass_template and self.omnipass_template in wf_name:
                    wf_type = "omnipass"
            by_key[(wf_type, step)].append(pod)

        results = []
        for (wf_type, step_name), step_pods in sorted(by_key.items()):
            total = len(step_pods)
            failed_pods = [p for p in step_pods if p.get("pod_phase") == "Failed"]
            retry_pods = [p for p in step_pods if (p.get("retries") or 0) > 0]
            oom_pods = [p for p in step_pods if p.get("oom_killed")]

            pending_vals = [p["pod_pending_sec"] for p in step_pods if p.get("pod_pending_sec") is not None]
            scheduling_vals = [p["scheduling_sec"] for p in step_pods if p.get("scheduling_sec") is not None]
            init_vals = [p["init_duration_sec"] for p in step_pods if p.get("init_duration_sec") is not None]
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
            # Event-based storage metrics (present only when K8s Events were fetched)
            vol_attach_vals = [p["volume_attach_sec"] for p in step_pods if p.get("volume_attach_sec") is not None]
            image_pull_vals = [p["image_pull_sec"] for p in step_pods if p.get("image_pull_sec") is not None]
            failed_attach_total = sum(p.get("failed_attach_count") or 0 for p in step_pods)

            results.append({
                "workflow_type": wf_type,
                "step_name": step_name,
                "total_executions": total,
                "failed_count": len(failed_pods),
                "failure_rate": len(failed_pods) / total if total > 0 else None,
                "retry_count": len(retry_pods),
                "retry_rate": len(retry_pods) / total if total > 0 else None,
                "oom_count": len(oom_pods),
                "oom_rate": len(oom_pods) / total if total > 0 else None,
                # Pending (total pre-execution: scheduling + init)
                "pending_avg": _safe_mean(pending_vals),
                "pending_p95": _percentile(pending_vals, 95),
                # Scheduling (K8s scheduling overhead: creation → kubelet acknowledged)
                "scheduling_avg": _safe_mean(scheduling_vals),
                "scheduling_p95": _percentile(scheduling_vals, 95),
                # Init container duration (kubelet acknowledged → last init done)
                "init_duration_avg": _safe_mean(init_vals),
                "init_duration_p50": _percentile(init_vals, 50),
                "init_duration_p95": _percentile(init_vals, 95),
                # Duration = full pod lifecycle (pending + running) — matches Argo UI
                "duration_avg": _safe_mean(wall_vals),
                "duration_p50": _percentile(wall_vals, 50),
                "duration_p95": _percentile(wall_vals, 95),
                # Running = main container execution only
                "running_avg": _safe_mean(running_vals),
                "running_p50": _percentile(running_vals, 50),
                "running_p95": _percentile(running_vals, 95),
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
                # Event-based storage metrics (None when K8s Events unavailable)
                "volume_attach_p50": _percentile(vol_attach_vals, 50),
                "volume_attach_p95": _percentile(vol_attach_vals, 95),
                "image_pull_p95": _percentile(image_pull_vals, 95),
                "failed_attach_total": failed_attach_total,
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
            if step.get("duration_p95") and step["duration_p95"] > 120:
                hints.append(
                    f"CASE B (pod execution bottleneck): step '{step['step_name']}' "
                    f"duration p95={step['duration_p95']:.0f}s. "
                    "Check pod logic or external dependency latency."
                )

        # CASE C: CPU-bound
        for step in steps:
            if (
                step.get("cpu_peak_max") is not None
                and step.get("duration_p95") is not None
                and step["cpu_peak_max"] > 800  # >800 milli-cores
                and step["duration_p95"] > 60
            ):
                hints.append(
                    f"CASE C (CPU-bound): step '{step['step_name']}' "
                    f"cpu_peak_max={step['cpu_peak_max']:.0f}m, "
                    f"duration_p95={step['duration_p95']:.0f}s. "
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
                step.get("duration_p95") is not None
                and step.get("cpu_peak_max") is not None
                and step["duration_p95"] > 60
                and step["cpu_peak_max"] < 200  # <200 milli-cores
            ):
                hints.append(
                    f"CASE E (external dependency wait): step '{step['step_name']}' "
                    f"duration_p95={step['duration_p95']:.0f}s but "
                    f"cpu_peak_max={step['cpu_peak_max']:.0f}m (low). "
                    "Likely waiting on MinIO, STAC API, or another external service."
                )

        # CASE F: slow init containers.
        # init_duration_sec = pod_scheduled_at → last init container done.
        # This window encompasses: PVC/volume attachment (Longhorn CSI) +
        # image pull + init container execution.  Healthy pods init in <5s.
        # Threshold: p95 > 15s is abnormal.
        #
        # Sub-classification via p50:
        #   p50 > 8s  → structural / every-pod delay → volume attachment most likely
        #   p50 ≤ 8s  → occasional spikes           → cold image pull most likely
        for step in steps:
            init_p95 = step.get("init_duration_p95")
            if init_p95 is None or init_p95 <= 15:
                continue
            init_p50 = step.get("init_duration_p50") or 0.0
            p50_str = f", p50={init_p50:.0f}s" if init_p50 else ""

            # Evidence-based breakdown when K8s Events were collected
            vol_p95 = step.get("volume_attach_p95")
            pull_p95 = step.get("image_pull_p95")
            fail_total = step.get("failed_attach_total") or 0

            if vol_p95 is not None or pull_p95 is not None:
                # Measured values available: report them and derive dominant factor
                measured_parts = []
                if vol_p95 is not None:
                    measured_parts.append(f"volume_attach_p95={vol_p95:.1f}s")
                if pull_p95 is not None:
                    measured_parts.append(f"image_pull_p95={pull_p95:.3f}s")
                measured_str = ", ".join(measured_parts)
                fail_str = (
                    f" ({fail_total} FailedAttachVolume event(s) recorded)"
                    if fail_total > 0 else " (volume attached cleanly on first attempt)"
                )
                if vol_p95 is not None and pull_p95 is not None and init_p95 > 0:
                    vol_pct = int(vol_p95 / init_p95 * 100)
                    pull_pct = int(pull_p95 / init_p95 * 100)
                    dominant = (
                        f"volume attach ({vol_pct}%)" if vol_pct >= pull_pct
                        else f"image pull ({pull_pct}%)"
                    )
                    breakdown = (
                        f"Dominant factor: {dominant} of init time. "
                        f"volume_attach={vol_pct}% image_pull={pull_pct}% "
                        f"other={max(0, 100 - vol_pct - pull_pct)}%."
                    )
                else:
                    breakdown = ""
                cause = (
                    f"Measured from K8s Events: {measured_str}{fail_str}. "
                    f"{breakdown} "
                    "For Longhorn RWX volumes the attach window covers CSI "
                    "NodeStageVolume + NodePublishVolume and, for per-workflow PVCs, "
                    "the share-manager pod (NFS server) startup."
                )
                fix = (
                    "Pre-provision the working-dir PVC outside the workflow so the "
                    "Longhorn share-manager is already running when the workflow starts "
                    "(pass it via Calrissian --persistent-volume-claim). "
                    "Also verify Longhorn replica locality: a local replica eliminates "
                    "the remote NFS round-trip."
                )
            elif init_p50 > 8:
                # No event data — fall back to statistical heuristic (structural delay)
                cause = (
                    "Primary suspect: Longhorn volume/share-manager not ready at pod start "
                    "(heuristic: p50 > 8s implies every-pod delay, not a cold-start spike). "
                    "init_duration_sec spans pod_scheduled_at → last init container done, "
                    "including CSI NodeStageVolume + NodePublishVolume. "
                    "For per-workflow PVCs the share-manager must start from scratch on "
                    "each workflow run (typically 20-35s). "
                    "Run with K8s Events enabled to get precise attach/pull breakdown."
                )
                fix = (
                    "Pre-provision the working-dir PVC outside the workflow and pass it "
                    "to Calrissian as a pre-existing claim. "
                    "If per-workflow PVCs are required, add a lightweight warmup step "
                    "before the first CWL step to force early volume attachment. "
                    "Check Longhorn replica placement: co-locating the replica on the "
                    "same node as the worker pods eliminates remote NFS overhead."
                )
            else:
                # Occasional spikes → cold-start image pull
                cause = (
                    "Primary suspect: image pull on cold node (argoexec or step image "
                    "not yet cached). Secondary causes: slow secret/configmap injection "
                    "or PVC attachment delay."
                )
                fix = (
                    "Pre-pull argoexec and step images via a DaemonSet image-prepuller. "
                    "Cache credentials or split heavyweight init logic."
                )
            hints.append(
                f"CASE F (slow init container): step '{step['step_name']}' "
                f"init_duration_p95={init_p95:.0f}s{p50_str}. "
                f"{cause} {fix}"
            )

        # CASE G: init overhead dominates wall-clock time.
        # When init_duration_p95 > 50% of duration_p95, the pod spends more time
        # in startup overhead than in actual work.  This indicates a structural
        # platform issue that will cap throughput regardless of workload
        # parallelism or code optimisation.
        overhead_steps = []
        for step in steps:
            init_p95 = step.get("init_duration_p95")
            dur_p95 = step.get("duration_p95")
            if (
                init_p95 is not None
                and dur_p95 is not None
                and dur_p95 > 0
                and init_p95 / dur_p95 > 0.5
            ):
                pct = int(init_p95 / dur_p95 * 100)
                overhead_steps.append((step["step_name"], init_p95, dur_p95, pct))
        if overhead_steps:
            step_summary = "; ".join(
                f"'{n}' {pct}% ({i:.0f}s init / {d:.0f}s total)"
                for n, i, d, pct in overhead_steps
            )
            hints.append(
                f"CASE G (init overhead dominates): {len(overhead_steps)} step(s) spend "
                f">50% of wall-clock time in init: {step_summary}. "
                "Startup overhead (volume attach + image pull + argoexec copy) "
                "is consuming the majority of each pod's lifetime. "
                "Resolving CASE F (PVC attachment / image caching) is the "
                "highest-leverage action to improve end-to-end throughput."
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
