"""
core/models.py
==============
Dataclasses for every entity the observer suite tracks.

Design notes
------------
- All timestamps are stored as UTC ISO-8601 strings when crossing I/O boundaries
  (CSV, JSON) and as datetime objects internally.
- Optional fields use Python's Optional[] type hint; None means "not yet known".
- Derived metrics (queue_sec, etc.) are computed by the KPI engine, NOT here.
  They are stored here so the full record can travel as a single object.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from typing import Optional


# ---------------------------------------------------------------------------
# Workflow record (Argo level)
# ---------------------------------------------------------------------------

@dataclass
class WorkflowRecord:
    """One Argo Workflow observed during the test run."""

    run_id: str
    workflow_name: str
    namespace: str

    # Phase reported by Argo: Pending | Running | Succeeded | Failed | Error
    phase: str = "Unknown"

    # Template / WorkflowTemplate name, if available
    template_name: Optional[str] = None

    # Labels and parameters extracted from the workflow spec (for correlation)
    labels: dict = field(default_factory=dict)
    parameters: dict = field(default_factory=dict)

    # Timestamps (UTC datetime objects; None = not yet observed)
    created_at: Optional[datetime] = None
    started_at: Optional[datetime] = None
    finished_at: Optional[datetime] = None

    # Derived durations (seconds); computed by kpi.py after all data is collected
    queue_sec: Optional[float] = None       # started_at - created_at
    run_sec: Optional[float] = None         # finished_at - started_at

    # Product correlation (populated by correlator.py)
    product_id: Optional[str] = None
    object_key: Optional[str] = None


# ---------------------------------------------------------------------------
# Pod / Step record (Kubernetes pod level)
# ---------------------------------------------------------------------------

@dataclass
class PodRecord:
    """One pod execution within an Argo workflow step."""

    run_id: str
    workflow_name: str
    pod_name: str

    # Argo step name (extracted from pod label workflows.argoproj.io/workflow-step-name)
    step_name: Optional[str] = None

    # Kubernetes pod phase: Pending | Running | Succeeded | Failed | Unknown
    pod_phase: str = "Unknown"

    # Timestamps
    pod_created_at: Optional[datetime] = None
    pod_started_at: Optional[datetime] = None   # containerStatuses[0].state.running.startedAt
    pod_finished_at: Optional[datetime] = None  # containerStatuses[0].state.terminated.finishedAt

    # Derived durations (seconds)
    pod_pending_sec: Optional[float] = None     # started_at - created_at
    pod_running_sec: Optional[float] = None     # finished_at - started_at

    # Reliability counters
    retries: int = 0
    restart_count: int = 0
    oom_killed: bool = False

    # Resource metrics (populated by k8s.py metrics collection)
    # CPU in milli-cores (m), memory in MiB
    cpu_avg: Optional[float] = None
    cpu_peak: Optional[float] = None
    cpu_throttling: Optional[float] = None  # throttled_periods / total_periods ratio 0..1
    mem_avg: Optional[float] = None
    mem_peak: Optional[float] = None

    # Product correlation
    product_id: Optional[str] = None


# ---------------------------------------------------------------------------
# Product / end-to-end record
# ---------------------------------------------------------------------------

@dataclass
class ProductRecord:
    """
    Correlated end-to-end view for a single ingested product.

    ingest_reference_time approximation strategy
    --------------------------------------------
    Because the observer does not control injection, it cannot observe the
    exact moment a product was written to MinIO.  The approximation used here
    is workflow_created_at (the moment Argo created the workflow), which is the
    earliest observable timestamp and is usually within a few seconds of the
    MinIO event trigger.  This can be overridden by setting the
    ingest_reference_time field directly when a better source is available
    (e.g. a workflow parameter that carries the object creation timestamp).
    """

    run_id: str
    product_id: str

    object_key: Optional[str] = None
    workflow_name: Optional[str] = None

    # Timestamps
    workflow_created_at: Optional[datetime] = None
    workflow_started_at: Optional[datetime] = None
    workflow_finished_at: Optional[datetime] = None
    stac_seen_at: Optional[datetime] = None

    # ingest_reference_time: approximated as workflow_created_at by default.
    # Overridden when a workflow parameter carries a more precise MinIO event time.
    ingest_reference_time: Optional[datetime] = None

    # Derived KPIs (seconds)
    workflow_queue_sec: Optional[float] = None   # workflow_started_at - workflow_created_at
    workflow_run_sec: Optional[float] = None     # workflow_finished_at - workflow_started_at
    stac_publish_sec: Optional[float] = None     # stac_seen_at - workflow_finished_at
    end_to_end_sec: Optional[float] = None       # stac_seen_at - ingest_reference_time

    # Final disposition
    final_status: str = "in_progress"  # succeeded | failed | timeout | in_progress


# ---------------------------------------------------------------------------
# STAC item visibility record
# ---------------------------------------------------------------------------

@dataclass
class StacRecord:
    """Tracks when a STAC item was first observed for a given product."""

    run_id: str
    product_id: str
    stac_item_id: str
    collection_id: str

    first_seen_at: Optional[datetime] = None
    verification_status: str = "not_found"  # not_found | found | verified

    # Snapshot of relevant STAC item fields at first observation
    stac_datetime: Optional[str] = None
    stac_bbox: Optional[list] = None


# ---------------------------------------------------------------------------
# Timeseries snapshot (one row per polling tick)
# ---------------------------------------------------------------------------

@dataclass
class TimeseriesSnapshot:
    """
    Point-in-time system-wide counters captured on each workflow polling cycle.
    These form the timeseries.csv output.
    """

    timestamp: datetime

    # Workflow counts at this instant
    workflows_pending: int = 0
    workflows_running: int = 0
    workflows_succeeded_total: int = 0
    workflows_failed_total: int = 0

    # Product-level counters
    completed_products_total: int = 0

    # Rolling throughput and latency over last 5 minutes
    throughput_last_5m: Optional[float] = None
    avg_queue_time_last_5m: Optional[float] = None
    p95_queue_time_last_5m: Optional[float] = None


# ---------------------------------------------------------------------------
# Run-level summary (written to run_summary.json)
# ---------------------------------------------------------------------------

@dataclass
class RunSummary:
    """Top-level summary produced by compute_kpis.py."""

    run_id: str
    started_at: Optional[datetime] = None
    finished_at: Optional[datetime] = None
    duration_sec: Optional[float] = None
    config_snapshot: dict = field(default_factory=dict)

    # Business KPIs
    total_products_observed: int = 0
    total_products_completed: int = 0
    total_products_failed: int = 0
    success_rate: Optional[float] = None          # 0..1
    avg_throughput_per_min: Optional[float] = None
    peak_throughput_per_min: Optional[float] = None

    # End-to-end latency (seconds)
    e2e_avg: Optional[float] = None
    e2e_p50: Optional[float] = None
    e2e_p95: Optional[float] = None
    e2e_p99: Optional[float] = None

    # STAC publication latency (seconds)
    stac_latency_avg: Optional[float] = None
    stac_latency_p95: Optional[float] = None

    # Workflow KPIs
    wf_queue_avg: Optional[float] = None
    wf_queue_p50: Optional[float] = None
    wf_queue_p95: Optional[float] = None
    wf_queue_p99: Optional[float] = None
    wf_duration_avg: Optional[float] = None
    wf_duration_p50: Optional[float] = None
    wf_duration_p95: Optional[float] = None
    wf_duration_p99: Optional[float] = None
    max_pending_workflows: int = 0
    max_running_workflows: int = 0
    avg_concurrency: Optional[float] = None

    # Per-step KPIs (list of dicts, one per step_name)
    step_kpis: list = field(default_factory=list)

    # Bottleneck diagnosis hints (populated by kpi.py)
    bottleneck_hints: list = field(default_factory=list)
