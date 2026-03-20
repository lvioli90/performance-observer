"""
collectors/argo.py
==================
Argo Workflow collector.

Primary path (Argo Server REST API)
-------------------------------------
Polls ``GET /api/v1/workflows/{namespace}`` on the Argo Server when
``argo.server_url`` is configured and accessible.

Fallback path (pod-based reconstruction)
-----------------------------------------
Used automatically when:
  - ``argo.server_url`` is empty, OR
  - the Argo Server returns HTTP 401 / 403 (insufficient RBAC for
    ``workflows.argoproj.io`` custom resources)

Argo labels every pod it creates with ``workflows.argoproj.io/workflow``.
The collector:
  1. Lists pods in the namespace that carry this label.
  2. Groups pods by workflow name.
  3. Synthesises a WorkflowRecord for each group by:
       - Reading the workflow UID from pod ``ownerReferences``
         (``kind=Workflow``).  The UID is required for the MinIO
         artifact path ``{uid}/kafka-message.json``.
       - Inferring workflow phase from the union of pod phases.
       - Deriving timestamps from pod metadata / container status.
       - Extracting workflow-level parameters from the
         ``workflows.argoproj.io/template`` annotation that Argo
         writes on every pod (JSON-encoded, contains
         ``inputs.parameters`` with fully-substituted values).

Parameter extraction notes
--------------------------
The ``workflows.argoproj.io/template`` annotation is available in
Argo Workflows 2.x and 3.x.  It contains the rendered template for
that specific step, including its ``inputs.parameters``.  By merging
parameters across all pods of the same workflow, we recover the
workflow-level arguments needed for product correlation
(``s3-key``, ``reference``, ``partitionkey``, …).

If the annotation is absent (stripped to save space in very large
workflows), parameters will be empty and the correlator falls back to
time-window matching.

Argo Server response shape (abbreviated)
-----------------------------------------
::

  {
    "items": [
      {
        "metadata": {
          "name": "ingestion-dispatcher-abc123",
          "namespace": "datalake",
          "uid": "9acd3eb9-...",
          "labels": {"partitionkey": "pdgs"},
          "creationTimestamp": "2024-01-01T10:00:00Z"
        },
        "spec": {
          "workflowTemplateRef": {"name": "ingestion-dispatcher"},
          "arguments": {
            "parameters": [{"name": "s3-key", "value": "product.zip"}]
          }
        },
        "status": {
          "phase": "Running",
          "startedAt": "2024-01-01T10:00:05Z",
          "finishedAt": null
        }
      }
    ]
  }
"""

from __future__ import annotations

import json
import logging
import time
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

import requests

from core.models import WorkflowRecord
from core.run_context import RunContext

logger = logging.getLogger(__name__)

# Argo pod label: value = workflow name
LABEL_WORKFLOW = "workflows.argoproj.io/workflow"
# Argo pod annotation: JSON-encoded resolved template (includes inputs.parameters)
ANNOTATION_TEMPLATE = "workflows.argoproj.io/template"

ACTIVE_PHASES = {"Pending", "Running"}


def _parse_iso(val: Optional[Any]) -> Optional[datetime]:
    """Parse an ISO-8601 string or datetime to a timezone-aware datetime."""
    if val is None:
        return None
    if isinstance(val, datetime):
        return val if val.tzinfo else val.replace(tzinfo=timezone.utc)
    if isinstance(val, str):
        try:
            return datetime.fromisoformat(val.replace("Z", "+00:00"))
        except ValueError:
            return None
    return None


def _dt_to_iso(dt: Optional[Any]) -> Optional[str]:
    """Convert a datetime (or ISO string) to a Z-suffixed ISO string."""
    if dt is None:
        return None
    if isinstance(dt, datetime):
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.strftime("%Y-%m-%dT%H:%M:%SZ")
    return str(dt)


def _infer_phase_from_pod_phases(phases: List[str]) -> str:
    """
    Infer the aggregate workflow phase from the set of its pod phases.

    Rules (in priority order):
      Failed  — any pod failed
      Running — any pod is still running or pending
      Succeeded — all pods in a terminal-success state
      Running — default (conservative; catches empty or all-Unknown)
    """
    lower = {p.lower() for p in phases if p and p.lower() != "unknown"}
    if "failed" in lower:
        return "Failed"
    if "running" in lower or "pending" in lower:
        return "Running"
    if lower and lower.issubset({"succeeded"}):
        return "Succeeded"
    return "Running"


class ArgoCollector:
    """
    Polls Argo for workflow status and updates the RunContext.

    Tries the Argo Server REST API first; falls back to pod-based
    workflow reconstruction when the API is unavailable or returns
    a permissions error (401 / 403).
    """

    def __init__(self, ctx: RunContext):
        self.ctx = ctx
        self._session = requests.Session()
        if ctx.argo_token:
            self._session.headers["Authorization"] = f"Bearer {ctx.argo_token}"
        self._session.verify = ctx.argo_verify_tls

        # Kubernetes CoreV1Api client (lazy, initialised on first pod-based poll)
        self._k8s_core = None

    # ------------------------------------------------------------------
    # Kubernetes client initialisation
    # ------------------------------------------------------------------

    def _ensure_k8s_client(self) -> None:
        """Initialise the Kubernetes Python client if not already done."""
        if self._k8s_core is not None:
            return
        try:
            from kubernetes import client as k8s_client, config as k8s_config

            if self.ctx.kubeconfig:
                k8s_config.load_kube_config(
                    config_file=self.ctx.kubeconfig,
                    context=self.ctx.k8s_context,
                )
            else:
                try:
                    k8s_config.load_incluster_config()
                except k8s_config.ConfigException:
                    k8s_config.load_kube_config(context=self.ctx.k8s_context)

            self._k8s_core = k8s_client.CoreV1Api()
            logger.debug("ArgoCollector: Kubernetes client initialised (pod-based mode)")
        except ImportError:
            logger.error("kubernetes package not installed. Run: pip install kubernetes")
            raise

    # ------------------------------------------------------------------
    # Main poll method
    # ------------------------------------------------------------------

    def poll(self) -> List[WorkflowRecord]:
        """
        Fetch current workflow states and update the RunContext.

        Returns the list of WorkflowRecord objects observed in this poll.
        """
        try:
            raw_workflows = self._fetch_workflows()
        except Exception as exc:
            logger.warning("Argo poll failed: %s", exc)
            return []

        records = []
        for raw in raw_workflows:
            try:
                record = self._parse_workflow(raw)
                if record is None:
                    continue
                if not self._passes_filter(record):
                    continue
                self.ctx.add_or_update_workflow(record)
                records.append(record)
            except Exception as exc:
                logger.warning("Error parsing workflow: %s", exc)

        logger.debug("Argo poll: %d workflows observed", len(records))
        return records

    # ------------------------------------------------------------------
    # Fetch routing
    # ------------------------------------------------------------------

    def _fetch_workflows(self) -> List[dict]:
        """
        Route to the Argo Server REST API or pod-based reconstruction.

        If ``argo.server_url`` is configured we try the REST API first.
        On HTTP 401/403 (or if the URL is empty) we fall back to
        reconstructing workflows from pod labels and annotations.
        """
        if self.ctx.argo_server_url:
            try:
                return self._fetch_workflows_from_server()
            except requests.HTTPError as exc:
                if exc.response is not None and exc.response.status_code in (401, 403):
                    logger.warning(
                        "Argo Server returned HTTP %d (insufficient RBAC for "
                        "workflows.argoproj.io). Switching to pod-based "
                        "workflow reconstruction for this run.",
                        exc.response.status_code,
                    )
                else:
                    logger.warning(
                        "Argo Server HTTP error (%s). "
                        "Falling back to pod-based reconstruction.",
                        exc,
                    )
                return self._fetch_workflows_from_pods()
            except Exception as exc:
                logger.warning(
                    "Argo Server unreachable (%s). "
                    "Falling back to pod-based reconstruction.",
                    exc,
                )
                return self._fetch_workflows_from_pods()

        return self._fetch_workflows_from_pods()

    # ------------------------------------------------------------------
    # Path 1: Argo Server REST API
    # ------------------------------------------------------------------

    def _fetch_workflows_from_server(self) -> List[dict]:
        """Call the Argo Server REST API and return raw workflow dicts."""
        ns = self.ctx.argo_namespace
        url = f"{self.ctx.argo_server_url.rstrip('/')}/api/v1/workflows/{ns}"

        params: Dict[str, str] = {}
        if self.ctx.argo_label_selector:
            params["listOptions.labelSelector"] = self.ctx.argo_label_selector

        resp = self._session.get(url, params=params, timeout=30)
        resp.raise_for_status()
        data = resp.json()
        return data.get("items") or []

    # ------------------------------------------------------------------
    # Path 2: pod-based workflow reconstruction
    # ------------------------------------------------------------------

    def _fetch_workflows_from_pods(self) -> List[dict]:
        """
        Reconstruct Argo workflow records by grouping pods.

        Required RBAC: ``pods: [get list watch]`` in the target namespace.
        Does NOT require access to ``workflows.argoproj.io`` CRDs.
        """
        try:
            self._ensure_k8s_client()
        except Exception:
            return []

        ns = self.ctx.argo_namespace

        # Build label selector: require the Argo workflow label (key presence)
        # and optionally the user-configured selector (e.g. partitionkey=pdgs).
        label_parts = [LABEL_WORKFLOW]
        if self.ctx.argo_label_selector:
            label_parts.append(self.ctx.argo_label_selector)
        label_selector = ",".join(label_parts)

        try:
            result = self._k8s_core.list_namespaced_pod(
                namespace=ns,
                label_selector=label_selector,
                timeout_seconds=30,
            )
        except Exception as exc:
            logger.warning("Pod list failed (pod-based Argo mode): %s", exc)
            return []

        pods = result.items or []

        # Pre-compute the workflow name keywords so we only reconstruct
        # dispatcher and omnipass workflows (ignore all other Argo workflows).
        dispatcher_kw = self.ctx.corr_dispatcher_template.lower()
        omnipass_kw = self.ctx.corr_omnipass_template.lower()

        # Group pods by workflow name, keeping only relevant workflows
        groups: Dict[str, List[Any]] = {}
        for pod in pods:
            wf_name = (pod.metadata.labels or {}).get(LABEL_WORKFLOW)
            if not wf_name:
                continue
            wf_lower = wf_name.lower()
            if dispatcher_kw not in wf_lower and omnipass_kw not in wf_lower:
                continue
            groups.setdefault(wf_name, []).append(pod)

        # Synthesise one workflow dict per group
        workflows = []
        for wf_name, wf_pods in groups.items():
            wf_dict = self._synthesize_workflow_from_pods(wf_name, wf_pods, ns)
            if wf_dict:
                workflows.append(wf_dict)

        logger.debug(
            "Pod-based Argo mode: %d pods → %d workflows in ns=%s",
            len(pods),
            len(workflows),
            ns,
        )
        return workflows

    def _synthesize_workflow_from_pods(
        self, wf_name: str, pods: List[Any], ns: str
    ) -> Optional[dict]:
        """
        Build a synthetic Argo-format workflow dict from a list of pods.

        The resulting dict is compatible with ``_parse_workflow()``.
        """
        labels: Dict[str, str] = {}
        uid: Optional[str] = None
        template_name: Optional[str] = None
        parameters: Dict[str, str] = {}

        created_timestamps: List[datetime] = []
        started_timestamps: List[datetime] = []
        finished_timestamps: List[datetime] = []
        all_phases: List[str] = []

        for pod in pods:
            meta = pod.metadata
            status = pod.status

            # Merge pod labels into workflow labels (Argo propagates wf labels)
            for k, v in (meta.labels or {}).items():
                if k not in labels:
                    labels[k] = v

            # Workflow UID from pod ownerReferences (kind=Workflow)
            if uid is None:
                for ref in (meta.owner_references or []):
                    if getattr(ref, "kind", None) == "Workflow":
                        uid = ref.uid
                        break

            # Template name from pod label (Argo 3.x sets this)
            if template_name is None:
                for lbl in (
                    "workflows.argoproj.io/workflow-template",
                    "workflows.argoproj.io/workflow-template-name",
                ):
                    v = (meta.labels or {}).get(lbl)
                    if v:
                        template_name = v
                        break

            # Creation timestamp
            if meta.creation_timestamp:
                ts = _parse_iso(meta.creation_timestamp)
                if ts:
                    created_timestamps.append(ts)

            # Pod phase
            phase = (status.phase if status else None) or "Unknown"
            all_phases.append(phase)

            # Container start / finish timestamps
            if status and status.container_statuses:
                cs = status.container_statuses[0]
                state = cs.state if cs else None
                if state:
                    if state.running and state.running.started_at:
                        ts = _parse_iso(state.running.started_at)
                        if ts:
                            started_timestamps.append(ts)
                    elif state.terminated:
                        term = state.terminated
                        if term.started_at:
                            ts = _parse_iso(term.started_at)
                            if ts:
                                started_timestamps.append(ts)
                        if term.finished_at:
                            ts = _parse_iso(term.finished_at)
                            if ts:
                                finished_timestamps.append(ts)

            # Extract parameters from template annotation (merge across all pods)
            pod_params = self._extract_params_from_pod_annotation(meta)
            for k, v in pod_params.items():
                if k not in parameters:
                    parameters[k] = v

        if not created_timestamps and not all_phases:
            return None

        inferred_phase = _infer_phase_from_pod_phases(all_phases)

        created_at = min(created_timestamps) if created_timestamps else None
        started_at = min(started_timestamps) if started_timestamps else None
        # Only set finished_at when all pods have reached a terminal state
        finished_at = (
            max(finished_timestamps)
            if finished_timestamps and inferred_phase in ("Succeeded", "Failed", "Error")
            else None
        )

        return {
            "metadata": {
                "name": wf_name,
                "namespace": ns,
                "uid": uid,
                "labels": labels,
                "annotations": {},
                "creationTimestamp": _dt_to_iso(created_at),
            },
            "spec": {
                "workflowTemplateRef": {"name": template_name or ""},
                "arguments": {
                    "parameters": [
                        {"name": k, "value": v} for k, v in parameters.items()
                    ]
                },
            },
            "status": {
                "phase": inferred_phase,
                "startedAt": _dt_to_iso(started_at),
                "finishedAt": _dt_to_iso(finished_at),
                "templateName": template_name,
            },
        }

    def _extract_params_from_pod_annotation(self, meta: Any) -> Dict[str, str]:
        """
        Parse ``workflows.argoproj.io/template`` pod annotation.

        Argo writes the JSON-encoded resolved template on every pod,
        including ``inputs.parameters`` with fully-substituted values.
        Returns a ``{name: value}`` dict, or an empty dict on failure.
        """
        annotations = meta.annotations or {}
        raw = annotations.get(ANNOTATION_TEMPLATE)
        if not raw:
            return {}
        try:
            template = json.loads(raw)
            params: Dict[str, str] = {}
            for p in (template.get("inputs") or {}).get("parameters") or []:
                if isinstance(p, dict) and p.get("name") and p.get("value") is not None:
                    params[str(p["name"])] = str(p["value"])
            return params
        except (json.JSONDecodeError, TypeError, AttributeError):
            return {}

    # ------------------------------------------------------------------
    # Parsing (works for both Argo Server and synthesised dicts)
    # ------------------------------------------------------------------

    def _parse_workflow(self, raw: dict) -> Optional[WorkflowRecord]:
        """
        Parse a raw Argo workflow dict (or synthesised equivalent) into a
        WorkflowRecord.
        """
        meta = raw.get("metadata", {})
        spec = raw.get("spec", {})
        status = raw.get("status", {})

        name = meta.get("name")
        uid = meta.get("uid")
        ns = meta.get("namespace", self.ctx.argo_namespace)
        if not name:
            return None

        labels = meta.get("labels") or {}
        annotations = meta.get("annotations") or {}

        # Parameters from spec.arguments
        parameters: Dict[str, str] = {}
        for param in (spec.get("arguments") or {}).get("parameters") or []:
            if isinstance(param, dict) and param.get("name"):
                parameters[param["name"]] = param.get("value", "")

        # Merge annotations that look like parameters (Argo Server path)
        for k, v in annotations.items():
            if k not in parameters:
                parameters[k] = v

        phase = status.get("phase", "Unknown")

        template_name = status.get("templateName")
        if not template_name:
            wf_spec = spec.get("workflowTemplateRef", {})
            template_name = wf_spec.get("name") or None

        record = WorkflowRecord(
            run_id=self.ctx.run_id,
            workflow_name=name,
            namespace=ns,
            uid=uid,
            phase=phase,
            template_name=template_name,
            labels=labels,
            parameters=parameters,
            created_at=_parse_iso(meta.get("creationTimestamp")),
            started_at=_parse_iso(status.get("startedAt")),
            finished_at=_parse_iso(status.get("finishedAt")),
        )
        return record

    # ------------------------------------------------------------------
    # Filters
    # ------------------------------------------------------------------

    def _passes_filter(self, record: WorkflowRecord) -> bool:
        """Return True if this workflow should be tracked."""
        if self.ctx.argo_template_filter:
            template = record.template_name or record.workflow_name
            if self.ctx.argo_template_filter.lower() not in template.lower():
                return False
        return True

    # ------------------------------------------------------------------
    # Timeseries snapshot helper
    # ------------------------------------------------------------------

    def snapshot_counts(self) -> Dict[str, int]:
        """
        Return current workflow phase counts from the in-memory store.
        Used by the timeseries builder.
        """
        counts = {"pending": 0, "running": 0, "succeeded": 0, "failed": 0}
        for wf in self.ctx.snapshot_workflows():
            phase = (wf.phase or "").lower()
            if phase == "pending":
                counts["pending"] += 1
            elif phase == "running":
                counts["running"] += 1
            elif phase == "succeeded":
                counts["succeeded"] += 1
            elif phase in ("failed", "error"):
                counts["failed"] += 1
        return counts
