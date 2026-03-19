"""
collectors/argo.py
==================
Argo Workflow collector.

Polls the Argo API (or falls back to the Kubernetes API) to discover and
track all ingestion workflows in the configured namespace.

API assumptions
---------------
The Argo Server exposes a REST API at:
  GET /api/v1/workflows/{namespace}

Response shape (abbreviated):
  {
    "items": [
      {
        "metadata": {
          "name": "ingestion-abc123",
          "namespace": "datalake",
          "labels": {"workflows.argoproj.io/workflow-template": "ingestion-template"},
          "annotations": {},
          "creationTimestamp": "2024-01-01T10:00:00Z"
        },
        "spec": {
          "templates": [...],
          "arguments": {
            "parameters": [{"name": "product-key", "value": "bucket/product.zip"}]
          }
        },
        "status": {
          "phase": "Running",       # Pending | Running | Succeeded | Failed | Error
          "startedAt": "2024-01-01T10:00:05Z",
          "finishedAt": null,
          "templateName": "ingestion-template"
        }
      }
    ]
  }

Fallback (kubectl-proxy / kubeconfig):
  If argo_server_url is empty, we use the Kubernetes custom resource API:
  GET /apis/argoproj.io/v1alpha1/namespaces/{ns}/workflows

Plug-in points
--------------
- Replace ``_fetch_workflows_from_server()`` with a real HTTP call.
- Replace ``_fetch_workflows_from_k8s()`` with a kubernetes-client call.
- Adjust ``_parse_workflow()`` if your Argo version returns different fields.
"""

from __future__ import annotations

import logging
import time
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

import requests

from core.models import WorkflowRecord
from core.run_context import RunContext

logger = logging.getLogger(__name__)

# Argo workflow phases that indicate the workflow is still alive
ACTIVE_PHASES = {"Pending", "Running"}


def _parse_iso(val: Optional[str]) -> Optional[datetime]:
    if not val:
        return None
    try:
        # K8s uses Z suffix; fromisoformat handles it in Python 3.11+
        # For older Python we strip and replace
        return datetime.fromisoformat(val.replace("Z", "+00:00"))
    except ValueError:
        return None


class ArgoCollector:
    """
    Polls Argo for workflow status and updates the RunContext.

    Parameters
    ----------
    ctx : RunContext
        Shared run context (config + in-memory stores).
    """

    def __init__(self, ctx: RunContext):
        self.ctx = ctx
        self._session = requests.Session()
        if ctx.argo_token:
            self._session.headers["Authorization"] = f"Bearer {ctx.argo_token}"
        self._session.verify = ctx.argo_verify_tls

    # ------------------------------------------------------------------
    # Main poll method (called by the observer loop)
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
    # Fetch (plug in real HTTP / K8s client here)
    # ------------------------------------------------------------------

    def _fetch_workflows(self) -> List[dict]:
        """Route to server API or K8s API based on config."""
        if self.ctx.argo_server_url:
            return self._fetch_workflows_from_server()
        else:
            return self._fetch_workflows_from_k8s()

    def _fetch_workflows_from_server(self) -> List[dict]:
        """
        Call the Argo Server REST API.

        PLUG-IN: Replace this with real HTTP call if needed.
        Currently implements the real API call structure.
        """
        ns = self.ctx.argo_namespace
        url = f"{self.ctx.argo_server_url.rstrip('/')}/api/v1/workflows/{ns}"

        params: Dict[str, str] = {}
        if self.ctx.argo_label_selector:
            params["listOptions.labelSelector"] = self.ctx.argo_label_selector

        resp = self._session.get(url, params=params, timeout=30)
        resp.raise_for_status()
        data = resp.json()
        return data.get("items") or []

    def _fetch_workflows_from_k8s(self) -> List[dict]:
        """
        Use the Kubernetes Python client to list Argo Workflow custom resources.

        PLUG-IN: Install kubernetes package and configure client.
        This uses the kubernetes package (pip install kubernetes).
        """
        try:
            from kubernetes import client as k8s_client, config as k8s_config

            # Load kubeconfig
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

            custom = k8s_client.CustomObjectsApi()
            ns = self.ctx.argo_namespace

            kwargs: Dict[str, Any] = {}
            if self.ctx.argo_label_selector:
                kwargs["label_selector"] = self.ctx.argo_label_selector

            result = custom.list_namespaced_custom_object(
                group="argoproj.io",
                version="v1alpha1",
                namespace=ns,
                plural="workflows",
                **kwargs,
            )
            return result.get("items") or []

        except ImportError:
            logger.error(
                "kubernetes package not installed. "
                "Run: pip install kubernetes"
            )
            raise
        except Exception as exc:
            logger.error("K8s API error fetching workflows: %s", exc)
            raise

    # ------------------------------------------------------------------
    # Parsing
    # ------------------------------------------------------------------

    def _parse_workflow(self, raw: dict) -> Optional[WorkflowRecord]:
        """
        Parse a raw Argo workflow dict into a WorkflowRecord.

        ADJUST THIS METHOD if your Argo version returns different fields.
        """
        meta = raw.get("metadata", {})
        spec = raw.get("spec", {})
        status = raw.get("status", {})

        name = meta.get("name")
        uid = meta.get("uid")   # K8s UUID — used as MinIO artifact path prefix
        ns = meta.get("namespace", self.ctx.argo_namespace)
        if not name:
            return None

        # Labels (used for product correlation)
        labels = meta.get("labels") or {}
        annotations = meta.get("annotations") or {}

        # Parameters from spec.arguments (flatten to name->value dict)
        parameters: Dict[str, str] = {}
        for param in (spec.get("arguments") or {}).get("parameters") or []:
            if isinstance(param, dict) and param.get("name"):
                parameters[param["name"]] = param.get("value", "")

        # Also check labels for parameter-like values (some Argo versions do this)
        # Merge annotations that look like parameters
        for k, v in annotations.items():
            if k not in parameters:
                parameters[k] = v

        phase = status.get("phase", "Unknown")

        # Template name: prefer status.templateName, fall back to spec template
        template_name = status.get("templateName")
        if not template_name:
            wf_spec = spec.get("workflowTemplateRef", {})
            template_name = wf_spec.get("name")

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
        # Template name filter (case-insensitive partial match)
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
        counts = {
            "pending": 0,
            "running": 0,
            "succeeded": 0,
            "failed": 0,
        }
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
