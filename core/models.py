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

    # Kubernetes UID (UUID format, e.g. "9acd3eb9-9b3d-4b00-a64f-1ac37b556f4a").
    # IMPORTANT: Argo artifact paths use {{workflow.uid}}, NOT workflow.name.
    # MinIO artifact key = "{uid}/kafka-message.json"
    uid: Optional[str] = None

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

    # Node where the pod was scheduled
    node_name: Optional[str] = None

    # Timestamps
    pod_created_at: Optional[datetime] = None
    pod_scheduled_at: Optional[datetime] = None  # status.start_time: kubelet acknowledged (≈ POD_START in save_pods.sh)
    init_done_at: Optional[datetime] = None      # last init container terminated.finished_at (≈ INIT_DONE)
    pod_started_at: Optional[datetime] = None    # containerStatuses[0].state.running.startedAt (≈ MAIN_START)
    pod_finished_at: Optional[datetime] = None   # containerStatuses[0].state.terminated.finishedAt (≈ MAIN_END)

    # Derived durations (seconds)
    pod_pending_sec: Optional[float] = None     # pod_started_at - pod_created_at (total pre-execution: scheduling + init)
    scheduling_sec: Optional[float] = None      # pod_scheduled_at - pod_created_at (K8s scheduling overhead)
    init_duration_sec: Optional[float] = None   # init_done_at - pod_scheduled_at (init container execution)
    pod_running_sec: Optional[float] = None     # pod_finished_at - pod_started_at (main container execution)

    # Reliability counters
    retries: int = 0
    restart_count: int = 0
    oom_killed: bool = False

    # Event-based storage metrics (populated from K8s Events API when available).
    # volume_attach_sec: time from first FailedAttachVolume → SuccessfulAttachVolume;
    #   falls back to pod_scheduled_at → SuccessfulAttachVolume for clean attaches.
    # image_pull_sec: total image pull duration, summed from Pulled event messages.
    # failed_attach_count: repetitions of FailedAttachVolume (0 = volume attached cleanly).
    volume_attach_sec: Optional[float] = None
    image_pull_sec: Optional[float] = None
    failed_attach_count: int = 0

    # Resource metrics (populated by k8s.py metrics collection)
    # CPU in milli-cores (m), memory in MiB
    cpu_avg: Optional[float] = None
    cpu_peak: Optional[float] = None
    cpu_throttling: Optional[float] = None  # throttled_periods / total_periods ratio 0..1
    mem_avg: Optional[float] = None
    mem_peak: Optional[float] = None

    # Workflow type: "dispatcher" | "omnipass" | "unknown"
    # Set by the collector from workflow_name matching against corr_*_template
    # config keys.  Used by KPIEngine to separate dispatcher KPIs from omnipass.
    workflow_type: str = "unknown"

    # Product correlation
    product_id: Optional[str] = None


# ---------------------------------------------------------------------------
# Product / end-to-end record
# ---------------------------------------------------------------------------

@dataclass
class ProductRecord:
    """
    Correlated end-to-end view for a single ingested product.

    Three-workflow pipeline
    -----------------------
    Each product goes through three sequential Argo workflows:
      1. ingestion-dispatcher  (fast: dispatches the job, ~20s)
      2. ingestor-omnipass     (heavy: runs calrissian CWL, minutes)
      3. deletion              (cleanup: removes product from drop bucket, triggered
                                after omnipass succeeds)

    All three workflows carry the same S3 object key:
      dispatcher:  s3-key parameter = "path/to/product.zip"
      omnipass:    reference parameter = "s3://drop-bucket/path/to/product.zip"
      deletion:    url parameter      = "s3://drop-bucket/path/to/product.zip"

    ingest_reference_time
    ---------------------
    Set to dispatcher_created_at (earliest observable event, closest to the
    actual MinIO drop). Falls back to omnipass created_at if dispatcher is
    not observed.

    end_to_end_sec
    --------------
    T0 = ingest_reference_time (dispatcher_created_at or drop-bucket event)
    T_final = deletion_finished_at when deletion is observed, else stac_seen_at.
    The deletion workflow completion is the definitive end of the ingestion cycle
    because it confirms the drop-bucket cleanup is done.
    """

    run_id: str
    product_id: str

    object_key: Optional[str] = None   # = s3://bucket/key (full S3 URL)

    # --- Dispatcher workflow fields ---
    dispatcher_workflow_name: Optional[str] = None
    dispatcher_created_at: Optional[datetime] = None
    dispatcher_started_at: Optional[datetime] = None
    dispatcher_finished_at: Optional[datetime] = None
    dispatcher_status: Optional[str] = None   # Succeeded | Failed | ...

    # --- Omnipass ingestor workflow fields ---
    # "workflow_*" fields refer to the omnipass workflow (the heavy one)
    workflow_name: Optional[str] = None
    workflow_created_at: Optional[datetime] = None
    workflow_started_at: Optional[datetime] = None
    workflow_finished_at: Optional[datetime] = None

    # --- STAC visibility ---
    stac_seen_at: Optional[datetime] = None

    # --- Deletion workflow fields ---
    deletion_workflow_name: Optional[str] = None
    deletion_created_at: Optional[datetime] = None
    deletion_started_at: Optional[datetime] = None
    deletion_finished_at: Optional[datetime] = None
    deletion_status: Optional[str] = None   # Succeeded | Failed | Running | ...

    # ingest_reference_time: dispatcher_created_at when available, else omnipass created_at.
    ingest_reference_time: Optional[datetime] = None

    # Derived KPIs (seconds) — computed by kpi.py
    # Omnipass-level (the meaningful performance signal):
    workflow_queue_sec: Optional[float] = None   # omnipass started_at - created_at
    workflow_run_sec: Optional[float] = None     # omnipass finished_at - started_at
    # Dispatcher-level (should be fast; high value = orchestration lag):
    dispatcher_queue_sec: Optional[float] = None
    dispatcher_run_sec: Optional[float] = None
    # Pipeline gap (Kafka trigger latency: dispatcher finish → omnipass created):
    pipeline_gap_sec: Optional[float] = None
    # Gap between omnipass finish and deletion triggered:
    gap_to_deletion_sec: Optional[float] = None  # deletion_created_at - omnipass finished_at
    # Deletion workflow timing:
    deletion_queue_sec: Optional[float] = None   # deletion started_at - created_at
    deletion_run_sec: Optional[float] = None     # deletion finished_at - started_at
    # STAC publication latency (after omnipass finishes, before deletion):
    stac_publish_sec: Optional[float] = None     # stac_seen_at - omnipass finished_at
    # Full end-to-end: T0 → deletion_finished_at (falls back to stac_seen_at):
    end_to_end_sec: Optional[float] = None

    # Final disposition (reflects the last completed workflow phase)
    final_status: str = "in_progress"  # succeeded | failed | timeout | in_progress


# ---------------------------------------------------------------------------
# STAC item visibility record
# ---------------------------------------------------------------------------

@dataclass
class StacRecord:
    """
    Tracks when a STAC item was first observed for a given product.

    Source: either from the MinIO kafka-message.json artifact (preferred)
    or from direct STAC API polling (fallback).
    """

    run_id: str
    product_id: str
    stac_item_id: str
    collection_id: str

    first_seen_at: Optional[datetime] = None
    verification_status: str = "not_found"  # not_found | found | verified
    # Discovery method: "artifact" (from kafka-message.json) | "poll" (from STAC API)
    discovery_method: str = "poll"

    # Full STAC URL (from artifact: {stac_public_endpoint}/collections/{coll}/items/{id})
    stac_url: Optional[str] = None
    # Snapshot of relevant STAC item fields at first observation
    stac_datetime: Optional[str] = None
    stac_bbox: Optional[list] = None
    # properties.updated from the STAC item — the catalog's own publication timestamp.
    # For re-ingested products this changes when the item is re-published; we compare it
    # against workflow_finished_at to detect a successful re-ingestion cycle.
    stac_updated: Optional[str] = None


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
