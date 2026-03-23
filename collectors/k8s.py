"""
collectors/k8s.py
=================
Kubernetes pod status and resource metrics collector.

Responsibilities
----------------
1. List pods in the configured namespaces that belong to Argo workflows
   (identified by the label ``workflows.argoproj.io/workflow``).
2. Extract timing, phase, restart count, OOM status per pod.
3. Query the Kubernetes Metrics Server (metrics.k8s.io) for CPU and memory
   usage per pod / container.

API assumptions
---------------
Pod list response (kubernetes Python client: V1Pod):
  pod.metadata.name
  pod.metadata.labels["workflows.argoproj.io/workflow"]       -> workflow_name
  pod.metadata.labels["workflows.argoproj.io/workflow-step-name"] -> step_name (argo v3+)
  pod.metadata.creation_timestamp                              -> pod_created_at
  pod.status.phase                                             -> Pending|Running|Succeeded|Failed
  pod.status.start_time                                        -> pod_started_at (first container started)
  pod.status.container_statuses[0].state.terminated.finished_at -> pod_finished_at
  pod.status.container_statuses[0].restart_count               -> restart_count
  pod.status.container_statuses[0].state.terminated.reason     -> "OOMKilled" if OOM

Metrics Server response (custom.metrics.k8s.io or metrics.k8s.io):
  GET /apis/metrics.k8s.io/v1beta1/namespaces/{ns}/pods/{pod_name}
  {
    "containers": [
      {
        "name": "main",
        "usage": {
          "cpu": "250m",        # milli-cores
          "memory": "128Mi"     # memory
        }
      }
    ]
  }

CPU throttling:
  Available via the container filesystem at
  /sys/fs/cgroup/cpu/cpu.stat (throttled_time, nr_throttled, nr_periods)
  but NOT exposed via the standard metrics API.  We approximate from repeated
  Metrics Server samples: if CPU usage is consistently at the limit, throttling
  is likely.  A more accurate implementation requires Prometheus
  (container_cpu_cfs_throttled_seconds_total / container_cpu_cfs_periods_total).
  Set the placeholder to None when Prometheus is not available.

Plug-in points
--------------
- Replace ``_list_pods()`` with real kubernetes-client call.
- Replace ``_fetch_pod_metrics()`` with real metrics API call.
- Add Prometheus query support in ``_fetch_throttling()`` if available.
"""

from __future__ import annotations

import logging
import re
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

from core.models import PodRecord
from core.run_context import RunContext

logger = logging.getLogger(__name__)

# Labels set by Argo on every pod it creates
LABEL_WORKFLOW = "workflows.argoproj.io/workflow"
LABEL_STEP = "workflows.argoproj.io/workflow-step-name"   # Argo v3+
LABEL_STEP_V2 = "workflows.argoproj.io/workflow-node-name"  # alternative label in some builds

_MEMORY_RE = re.compile(r"^(\d+(?:\.\d+)?)(Ki|Mi|Gi|Ti|K|M|G|T)?$")
# Argo names pods as {workflow-name}-{step-name}-{hash}
# Hash may be purely numeric (FNV, e.g. "3279871329") or alphanumeric base32
# (e.g. "fnk7l", "a3b7cd9e"). Match both: 5-15 lowercase alphanumeric chars.
_ARGO_POD_HASH_RE = re.compile(r"-[a-z0-9]{5,15}$")
# Argo node names have the form:
#   {workflow-name}[{index}][{index}].{step-name}   (steps / DAG nodes)
#   {workflow-name}                                  (root entrypoint)
# Extract the terminal step name after the last dot.
_ARGO_NODE_STEP_RE = re.compile(r"\.([^.\[\]]+)$")


def _step_from_node_name(node_name: str) -> Optional[str]:
    """
    Extract the terminal step name from an Argo node-name label value.

    When a step uses ``templateRef``, Argo sets
    ``workflows.argoproj.io/workflow-step-name`` to the *external* template's
    name (e.g. ``main``) rather than the calling step name
    (e.g. ``send-message-success``).  The node-name label
    (``workflows.argoproj.io/workflow-node-name``) always carries the full
    path, e.g. ``ingestion-dispatcher-abc123[2][0].send-message-success``,
    from which we can recover the true step name.

    Returns None when the node name has no dot (root entrypoint node).
    """
    m = _ARGO_NODE_STEP_RE.search(node_name)
    return m.group(1) if m else None


def _parse_memory_mib(value: str) -> Optional[float]:
    """Convert K8s memory string (e.g. '128Mi', '2Gi', '500000Ki') to MiB."""
    if not value:
        return None
    m = _MEMORY_RE.match(value.strip())
    if not m:
        return None
    amount = float(m.group(1))
    suffix = m.group(2) or ""
    factors = {"Ki": 1 / 1024, "Mi": 1, "Gi": 1024, "Ti": 1024 * 1024,
               "K": 1 / 1024, "M": 1, "G": 1024, "T": 1024 * 1024}
    return amount * factors.get(suffix, 1)


def _parse_cpu_millicores(value: str) -> Optional[float]:
    """Convert K8s CPU string (e.g. '250m', '1', '0.5') to milli-cores."""
    if not value:
        return None
    value = value.strip()
    if value.endswith("m"):
        try:
            return float(value[:-1])
        except ValueError:
            return None
    try:
        return float(value) * 1000
    except ValueError:
        return None


def _parse_iso(val) -> Optional[datetime]:
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


class K8sCollector:
    """
    Polls Kubernetes for pod status and resource metrics.

    Maintains a per-pod CPU sample list so that cpu_avg can be computed
    from multiple readings taken during the pod's lifetime.
    """

    def __init__(self, ctx: RunContext):
        self.ctx = ctx
        # pod_name -> list of (cpu_millicores, mem_mib) tuples from Metrics Server
        self._metrics_history: Dict[str, List[tuple]] = {}
        self._k8s_client = None
        self._metrics_client = None

    # ------------------------------------------------------------------
    # Kubernetes client init (lazy, so we can run without K8s in tests)
    # ------------------------------------------------------------------

    def _ensure_client(self) -> None:
        """Initialize the kubernetes client if not already done."""
        if self._k8s_client is not None:
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

            self._k8s_client = k8s_client.CoreV1Api()
            self._custom_client = k8s_client.CustomObjectsApi()
            logger.debug("Kubernetes client initialized")
        except ImportError:
            logger.error("kubernetes package not installed. Run: pip install kubernetes")
            raise

    # ------------------------------------------------------------------
    # Pod status polling
    # ------------------------------------------------------------------

    def poll_pods(self) -> List[PodRecord]:
        """
        List all Argo-managed pods in the configured namespaces and update
        the RunContext.
        """
        try:
            self._ensure_client()
        except Exception:
            return []

        records = []
        for ns in self.ctx.pod_namespaces:
            try:
                pods = self._list_pods(ns)
                for raw_pod in pods:
                    try:
                        record = self._parse_pod(raw_pod)
                        if record is None:
                            continue
                        # Apply workflow type filter: keep only dispatcher and omnipass pods
                        if record.workflow_name:
                            wn = record.workflow_name.lower()
                            dispatcher_kw = self.ctx.corr_dispatcher_template.lower()
                            omnipass_kw = self.ctx.corr_omnipass_template.lower()
                            if dispatcher_kw not in wn and omnipass_kw not in wn:
                                continue
                        # Apply tracked_steps filter
                        if self.ctx.tracked_steps and record.step_name:
                            if not any(
                                ts.lower() in record.step_name.lower()
                                for ts in self.ctx.tracked_steps
                            ):
                                continue
                        self.ctx.add_or_update_pod(record)
                        records.append(record)
                    except Exception as exc:
                        logger.warning("Error parsing pod: %s", exc)
            except Exception as exc:
                logger.warning("Pod list failed for ns=%s: %s", ns, exc)

        logger.debug("Pod poll: %d pods observed", len(records))
        return records

    def _list_pods(self, namespace: str) -> List[Any]:
        """
        Return a list of raw V1Pod objects for Argo-managed pods.

        PLUG-IN: This calls the real kubernetes-client API.
        """
        label_selector = f"{LABEL_WORKFLOW}"  # any value, just presence of label
        result = self._k8s_client.list_namespaced_pod(
            namespace=namespace,
            label_selector=label_selector,
            timeout_seconds=30,
        )
        return result.items or []

    def _parse_pod(self, pod: Any) -> Optional[PodRecord]:
        """Parse a V1Pod object into a PodRecord."""
        meta = pod.metadata
        status = pod.status

        pod_name = meta.name
        if not pod_name:
            return None

        labels = meta.labels or {}
        workflow_name = labels.get(LABEL_WORKFLOW, "")

        # Prefer the node-name label: it always reflects the calling step name
        # even for templateRef steps (where workflow-step-name is the external
        # template's name, not the step name in the parent workflow).
        node_name = labels.get(LABEL_STEP_V2)
        step_name = (
            _step_from_node_name(node_name)
            if node_name
            else None
        ) or labels.get(LABEL_STEP)

        # Fallback: derive step_name from pod_name when labels are absent.
        # Argo names pods as {workflow-name}-{step-name}-{numeric-hash}, e.g.
        # pdgs-omnipass-ingestion-40074-qwmkj-main-3279871329 → step "main"
        if not step_name and workflow_name and pod_name.startswith(workflow_name + "-"):
            remainder = pod_name[len(workflow_name) + 1:]
            step_name = _ARGO_POD_HASH_RE.sub("", remainder) or None

        # Fallback: Calrissian pods are created via a K8s Job and named
        # {step-name}-{wf-short-uid}-{pod-suffix}, e.g.:
        #   calrissian-argo-wf-runner-zdk42-jk2b6
        # They carry job-name=calrissian-argo-wf-runner-{wf-short-uid} but
        # their pod name does NOT start with the workflow name, so the
        # previous fallback does not fire.  Derive step from job-name label.
        if not step_name:
            job_name = labels.get("job-name", "")
            if job_name:
                step_name = _ARGO_POD_HASH_RE.sub("", job_name) or None

        # Pod phase
        phase = (status.phase or "Unknown") if status else "Unknown"

        # Timestamps
        created_at = _parse_iso(meta.creation_timestamp)

        started_at: Optional[datetime] = None
        finished_at: Optional[datetime] = None
        restart_count = 0
        oom_killed = False

        if status and status.container_statuses:
            cs = status.container_statuses[0]
            restart_count = cs.restart_count or 0

            state = cs.state
            if state:
                if state.running and state.running.started_at:
                    started_at = _parse_iso(state.running.started_at)
                elif state.terminated:
                    term = state.terminated
                    if term.started_at:
                        started_at = _parse_iso(term.started_at)
                    if term.finished_at:
                        finished_at = _parse_iso(term.finished_at)
                    if term.reason == "OOMKilled":
                        oom_killed = True

            # Fall back to pod start_time if container start not available
            if started_at is None and status.start_time:
                started_at = _parse_iso(status.start_time)

        # Check last state for OOM (e.g. after a restart)
        if not oom_killed and status and status.container_statuses:
            cs = status.container_statuses[0]
            if cs.last_state and cs.last_state.terminated:
                if cs.last_state.terminated.reason == "OOMKilled":
                    oom_killed = True

        record = PodRecord(
            run_id=self.ctx.run_id,
            workflow_name=workflow_name,
            pod_name=pod_name,
            step_name=step_name,
            pod_phase=phase,
            pod_created_at=created_at,
            pod_started_at=started_at,
            pod_finished_at=finished_at,
            restart_count=restart_count,
            oom_killed=oom_killed,
        )

        # Compute derived durations
        if created_at and started_at:
            record.pod_pending_sec = (started_at - created_at).total_seconds()
        if started_at and finished_at:
            record.pod_running_sec = (finished_at - started_at).total_seconds()

        return record

    # ------------------------------------------------------------------
    # Resource metrics polling
    # ------------------------------------------------------------------

    def poll_metrics(self) -> int:
        """
        Query the Metrics Server for CPU/memory usage of running pods.
        Updates the in-memory PodRecord with rolling averages.

        Returns count of pods updated.
        """
        try:
            self._ensure_client()
        except Exception:
            return 0

        updated = 0
        for ns in self.ctx.pod_namespaces:
            try:
                metrics_list = self._fetch_all_pod_metrics(ns)
                for item in metrics_list:
                    pod_name = item.get("metadata", {}).get("name", "")
                    containers = item.get("containers", [])
                    if not pod_name or not containers:
                        continue

                    # Sum CPU and memory across all containers in the pod
                    total_cpu_m: float = 0
                    total_mem_mib: float = 0
                    for c in containers:
                        usage = c.get("usage", {})
                        cpu_m = _parse_cpu_millicores(usage.get("cpu", "0"))
                        mem_mib = _parse_memory_mib(usage.get("memory", "0"))
                        if cpu_m is not None:
                            total_cpu_m += cpu_m
                        if mem_mib is not None:
                            total_mem_mib += mem_mib

                    # Accumulate history
                    if pod_name not in self._metrics_history:
                        self._metrics_history[pod_name] = []
                    self._metrics_history[pod_name].append((total_cpu_m, total_mem_mib))

                    # Update PodRecord
                    with self.ctx._lock:
                        pod_record = self.ctx.pods.get(pod_name)
                        if pod_record:
                            samples = self._metrics_history[pod_name]
                            cpu_samples = [s[0] for s in samples]
                            mem_samples = [s[1] for s in samples]
                            pod_record.cpu_avg = sum(cpu_samples) / len(cpu_samples)
                            pod_record.cpu_peak = max(cpu_samples)
                            pod_record.mem_avg = sum(mem_samples) / len(mem_samples)
                            pod_record.mem_peak = max(mem_samples)
                            self.ctx._pods_dirty.append(pod_name)
                            updated += 1

            except Exception as exc:
                logger.warning("Metrics poll failed for ns=%s: %s", ns, exc)

        logger.debug("Metrics poll: %d pods updated", updated)
        return updated

    def _fetch_all_pod_metrics(self, namespace: str) -> List[dict]:
        """
        Fetch pod metrics from the Kubernetes Metrics Server.

        PLUG-IN: Uses the CustomObjectsApi to call metrics.k8s.io.
        Returns a list of pod metrics dicts.
        """
        try:
            result = self._custom_client.list_namespaced_custom_object(
                group="metrics.k8s.io",
                version="v1beta1",
                namespace=namespace,
                plural="pods",
            )
            return result.get("items") or []
        except Exception as exc:
            logger.debug("Metrics Server unavailable for ns=%s: %s", namespace, exc)
            return []

    def _fetch_pod_metrics(self, namespace: str, pod_name: str) -> Optional[dict]:
        """
        Fetch metrics for a single pod.

        PLUG-IN: Calls metrics.k8s.io/v1beta1.
        """
        try:
            result = self._custom_client.get_namespaced_custom_object(
                group="metrics.k8s.io",
                version="v1beta1",
                namespace=namespace,
                plural="pods",
                name=pod_name,
            )
            return result
        except Exception as exc:
            logger.debug("Pod metrics unavailable for %s: %s", pod_name, exc)
            return None
