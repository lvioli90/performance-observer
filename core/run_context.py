"""
core/run_context.py
===================
Loads and validates configuration; provides a single shared context object
that all collectors and reporters use throughout a test run.

The RunContext is created once at startup and passed by reference to every
component.  It also holds the in-memory stores for observations so that
multiple threads can share state safely via a threading.Lock.
"""

from __future__ import annotations

import logging
import os
import re
import threading
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional

import yaml

from core.models import (
    PodRecord,
    ProductRecord,
    RunSummary,
    StacRecord,
    TimeseriesSnapshot,
    WorkflowRecord,
)

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Config loading
# ---------------------------------------------------------------------------

def load_config(path: str) -> Dict[str, Any]:
    """Load YAML config file and return as a plain dict."""
    with open(path, "r") as fh:
        cfg = yaml.safe_load(fh)
    if cfg is None:
        cfg = {}
    return cfg


def _get(cfg: dict, *keys, default=None):
    """Safe nested dict getter."""
    node = cfg
    for k in keys:
        if not isinstance(node, dict):
            return default
        node = node.get(k, default)
        if node is None:
            return default
    return node


# ---------------------------------------------------------------------------
# RunContext
# ---------------------------------------------------------------------------

class RunContext:
    """
    Central context object for one observer run.

    Attributes
    ----------
    cfg : dict
        Raw YAML config dict.
    run_id : str
        Unique identifier for this run.
    output_dir : Path
        Directory where all outputs for this run are stored.
    started_at : datetime
        UTC wall-clock time when the observer started.

    Thread-safety
    -------------
    All mutable stores (workflows, pods, products, stac_records, timeseries)
    are protected by ``self._lock``.  Callers must acquire the lock before
    modifying or iterating these collections.
    """

    def __init__(self, cfg: dict):
        self.cfg = cfg
        self._lock = threading.Lock()

        # --- Identity ---
        self.run_id: str = cfg.get("run_id") or f"run-{uuid.uuid4().hex[:8]}"
        self.started_at: datetime = datetime.now(timezone.utc)

        # --- Output directory ---
        base_dir = Path(_get(cfg, "output", "base_dir", default="./results"))
        self.output_dir: Path = base_dir / self.run_id
        self.output_dir.mkdir(parents=True, exist_ok=True)

        # --- Polling intervals (seconds) ---
        p = cfg.get("polling", {})
        self.poll_workflow_sec: int = int(p.get("workflow_status_sec", 15))
        self.poll_pod_sec: int = int(p.get("pod_status_sec", 20))
        self.poll_metrics_sec: int = int(p.get("pod_metrics_sec", 30))
        self.poll_stac_sec: int = int(p.get("stac_sec", 45))
        self.poll_timeseries_flush_sec: int = int(p.get("timeseries_flush_sec", 30))

        # --- Timing ---
        t = cfg.get("timing", {})
        self.max_duration_sec: int = int(t.get("max_duration_sec", 0))
        self.grace_period_sec: int = int(t.get("grace_period_sec", 300))

        # --- Argo config ---
        a = cfg.get("argo", {})
        self.argo_namespace: str = a.get("namespace", "datalake")
        self.argo_template_filter: str = a.get("workflow_template_filter", "")
        self.argo_label_selector: str = a.get("workflow_label_selector", "")
        self.argo_server_url: str = a.get("server_url", "")
        self.argo_token: str = a.get("token", "")
        self.argo_verify_tls: bool = bool(a.get("verify_tls", True))

        # --- Kubernetes config ---
        k = cfg.get("kubernetes", {})
        self.pod_namespaces: List[str] = k.get("pod_namespaces", [self.argo_namespace])
        self.kubeconfig: Optional[str] = k.get("kubeconfig") or None
        self.k8s_context: Optional[str] = k.get("context") or None

        # --- STAC config ---
        s = cfg.get("stac", {})
        self.stac_endpoint: str = s.get("endpoint", "")
        self.stac_token: str = s.get("token", "")
        self.stac_collection: str = s.get("collection_id", "")
        self.stac_verify_tls: bool = bool(s.get("verify_tls", True))

        # --- Correlation config ---
        c = cfg.get("correlation", {})
        self.corr_object_key_label: str = c.get("object_key_label", "")
        self.corr_product_id_regex: str = c.get(
            "product_id_from_object_key",
            r"(?P<product_id>[^/]+)(?:\.zip|\.tar\.gz|\.tgz|)$",
        )
        self.corr_stac_id_template: str = c.get(
            "stac_item_id_from_product_id", "{product_id}"
        )
        self.corr_wf_name_contains_product: bool = bool(
            c.get("workflow_name_contains_product", False)
        )
        self.corr_wf_name_regex: str = c.get(
            "product_id_from_workflow_name_regex",
            r"ingestion-(?P<product_id>.+)-[a-z0-9]{5}$",
        )
        self.corr_time_window_sec: int = int(c.get("time_window_fallback_sec", 60))

        # Pre-compile correlation regexes (may raise if invalid)
        self._product_id_re: Optional[re.Pattern] = None
        if self.corr_product_id_regex:
            self._product_id_re = re.compile(self.corr_product_id_regex)

        self._wf_name_re: Optional[re.Pattern] = None
        if self.corr_wf_name_regex:
            self._wf_name_re = re.compile(self.corr_wf_name_regex)

        # --- Step tracking ---
        self.tracked_steps: List[str] = cfg.get("tracked_steps", [])

        # --- Flush / checkpoint config ---
        out = cfg.get("output", {})
        self.flush_every_n: int = int(out.get("flush_every_n_records", 50))
        self.checkpoint_interval_sec: int = int(out.get("checkpoint_interval_sec", 300))

        # -------------------------------------------------------------------
        # In-memory observation stores (ALL protected by self._lock)
        # -------------------------------------------------------------------
        # workflow_name -> WorkflowRecord
        self.workflows: Dict[str, WorkflowRecord] = {}
        # pod_name -> PodRecord
        self.pods: Dict[str, PodRecord] = {}
        # product_id -> ProductRecord
        self.products: Dict[str, ProductRecord] = {}
        # product_id -> StacRecord
        self.stac_records: Dict[str, StacRecord] = {}
        # ordered list of TimeseriesSnapshot
        self.timeseries: List[TimeseriesSnapshot] = []

        # Dirty flags for incremental flush
        self._workflows_dirty: List[str] = []    # workflow_names
        self._pods_dirty: List[str] = []         # pod_names
        self._products_dirty: List[str] = []     # product_ids

        # --- Stop signal ---
        self.stop_event = threading.Event()

        logger.info(
            "RunContext initialized: run_id=%s output=%s",
            self.run_id,
            self.output_dir,
        )

    # ------------------------------------------------------------------
    # Accessors with lock
    # ------------------------------------------------------------------

    def add_or_update_workflow(self, record: WorkflowRecord) -> None:
        with self._lock:
            self.workflows[record.workflow_name] = record
            self._workflows_dirty.append(record.workflow_name)

    def add_or_update_pod(self, record: PodRecord) -> None:
        with self._lock:
            self.pods[record.pod_name] = record
            self._pods_dirty.append(record.pod_name)

    def add_or_update_product(self, record: ProductRecord) -> None:
        with self._lock:
            self.products[record.product_id] = record
            self._products_dirty.append(record.product_id)

    def add_or_update_stac(self, record: StacRecord) -> None:
        with self._lock:
            self.stac_records[record.product_id] = record

    def append_timeseries(self, snapshot: TimeseriesSnapshot) -> None:
        with self._lock:
            self.timeseries.append(snapshot)

    def snapshot_workflows(self) -> List[WorkflowRecord]:
        with self._lock:
            return list(self.workflows.values())

    def snapshot_pods(self) -> List[PodRecord]:
        with self._lock:
            return list(self.pods.values())

    def snapshot_products(self) -> List[ProductRecord]:
        with self._lock:
            return list(self.products.values())

    def snapshot_stac(self) -> List[StacRecord]:
        with self._lock:
            return list(self.stac_records.values())

    def snapshot_timeseries(self) -> List[TimeseriesSnapshot]:
        with self._lock:
            return list(self.timeseries)

    # ------------------------------------------------------------------
    # Correlation helpers (stateless, no lock needed)
    # ------------------------------------------------------------------

    def extract_product_id_from_object_key(self, object_key: str) -> Optional[str]:
        """Apply the configured regex to extract product_id from a MinIO object key."""
        if not self._product_id_re or not object_key:
            return None
        m = self._product_id_re.search(object_key)
        if m:
            try:
                return m.group("product_id")
            except IndexError:
                return None
        return None

    def extract_product_id_from_workflow_name(self, wf_name: str) -> Optional[str]:
        """Apply the configured regex to extract product_id from a workflow name."""
        if not self._wf_name_re or not wf_name:
            return None
        m = self._wf_name_re.search(wf_name)
        if m:
            try:
                return m.group("product_id")
            except IndexError:
                return None
        return None

    def derive_stac_item_id(self, product_id: str) -> str:
        """Derive the expected STAC item id from a product_id using the configured template."""
        if not self.corr_stac_id_template:
            return product_id
        return self.corr_stac_id_template.format(product_id=product_id)

    # ------------------------------------------------------------------
    # Lifecycle helpers
    # ------------------------------------------------------------------

    def elapsed_sec(self) -> float:
        return (datetime.now(timezone.utc) - self.started_at).total_seconds()

    def is_time_limit_reached(self) -> bool:
        if self.max_duration_sec <= 0:
            return False
        return self.elapsed_sec() >= self.max_duration_sec

    def request_stop(self) -> None:
        logger.info("Stop requested for run %s", self.run_id)
        self.stop_event.set()
