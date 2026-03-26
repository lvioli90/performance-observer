"""
reporting/export_json.py
========================
Writes the run_summary.json file containing all computed KPIs.

Also provides a markdown summary writer for quick human-readable inspection.
"""

from __future__ import annotations

import json
import logging
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# JSON serialization
# ---------------------------------------------------------------------------

class _Encoder(json.JSONEncoder):
    def default(self, obj: Any) -> Any:
        if isinstance(obj, datetime):
            return obj.isoformat()
        return super().default(obj)


def write_run_summary_json(output_dir: Path, summary: dict) -> Path:
    """Write run_summary.json."""
    path = output_dir / "run_summary.json"
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", encoding="utf-8") as fh:
        json.dump(summary, fh, cls=_Encoder, indent=2, ensure_ascii=False)
    logger.info("Wrote run_summary.json to %s", path)
    return path


# ---------------------------------------------------------------------------
# Markdown summary
# ---------------------------------------------------------------------------

def write_markdown_summary(output_dir: Path, summary: dict) -> Path:
    """
    Write a detailed technical Markdown report for the run.

    Sections
    --------
    1. Run metadata
    2. Executive summary (outcome + primary bottleneck in one sentence)
    3. Pipeline timing (dispatcher → gap → omnipass queue → run → STAC)
    4. Per-step overhead analysis (scheduling | vol_attach | init_rest | running)
    5. Resource usage (CPU / memory per step)
    6. Reliability (failures, retries, OOM)
    7. Bottleneck diagnosis (raw hints from KPIEngine)
    8. Prioritised recommendations (derived from hints)
    """
    path = output_dir / "run_summary.md"

    biz = summary.get("business", {})
    wf  = summary.get("workflows", {})
    steps = summary.get("steps", [])
    hints = summary.get("bottleneck_hints", [])
    meta  = summary.get("meta", {})

    # ------------------------------------------------------------------ #
    # Formatting helpers                                                    #
    # ------------------------------------------------------------------ #

    def _f(val, unit="", decimals=1) -> str:
        """Format a numeric value; 'N/A' when None."""
        if val is None:
            return "N/A"
        if isinstance(val, (int, float)):
            if isinstance(val, float):
                return f"{val:.{decimals}f}{unit}"
            return f"{val}{unit}"
        return f"{val}{unit}"

    def _pf(val) -> str:
        """Format a 0..1 ratio as XX.X %."""
        if val is None:
            return "N/A"
        return f"{val * 100:.1f}%"

    def _sec(val) -> str:
        return _f(val, "s")

    def _overhead_badge(overhead_pct: Optional[float]) -> str:
        """Return a text badge for the overhead fraction."""
        if overhead_pct is None:
            return ""
        pct = overhead_pct * 100
        if pct >= 80:
            return f"**{pct:.0f}% ⚠ CRITICAL**"
        if pct >= 50:
            return f"**{pct:.0f}% ⚠ HIGH**"
        return f"{pct:.0f}%"

    # ------------------------------------------------------------------ #
    # Derived quantities for the executive summary                         #
    # ------------------------------------------------------------------ #

    n_observed  = biz.get("total_products_observed", 0)
    n_completed = biz.get("total_products_completed", 0)
    n_failed    = biz.get("total_products_failed", 0)
    success_pct = (biz.get("success_rate") or 0) * 100

    # Compute total overhead across all steps (scheduling + init)
    total_overhead_sec = 0.0
    total_wall_sec     = 0.0
    vol_attach_available = False
    for s in steps:
        sched = s.get("scheduling_p50") or 0.0
        init  = s.get("init_duration_p50") or 0.0
        run   = s.get("running_p50") or 0.0
        total_wall_sec     += sched + init + run
        total_overhead_sec += sched + init
        if s.get("volume_attach_p50") is not None:
            vol_attach_available = True

    overall_overhead_pct = (
        total_overhead_sec / total_wall_sec if total_wall_sec > 0 else None
    )

    # Primary bottleneck sentence
    if hints:
        primary_hint_type = hints[0].split(":")[0].strip() if hints else ""
    else:
        primary_hint_type = ""

    evidence_note = (
        " (measured via K8s Events API)"
        if vol_attach_available
        else " (estimated from pod timestamps — enable K8s Events for sub-component detail)"
    )

    lines: List[str] = []

    # ================================================================== #
    # 1. Header + metadata                                                 #
    # ================================================================== #
    lines += [
        "# Performance Observer — Technical Report",
        "",
        f"| Field | Value |",
        f"|-------|-------|",
        f"| Run ID | `{meta.get('run_id', 'N/A')}` |",
        f"| Started | {meta.get('started_at', 'N/A')} |",
        f"| Finished | {meta.get('finished_at', 'N/A')} |",
        f"| Duration | {_sec(meta.get('duration_sec'))} |",
        f"| Computed at | {meta.get('computed_at', 'N/A')} |",
        "",
    ]

    # ================================================================== #
    # 2. Executive summary                                                 #
    # ================================================================== #
    outcome = "✅ PASSED" if n_failed == 0 and n_completed > 0 else "❌ FAILED" if n_failed > 0 else "⚠ INCOMPLETE"

    lines += [
        "## Executive Summary",
        "",
        f"**Outcome:** {outcome} — "
        f"{n_completed}/{n_observed} product(s) completed ({success_pct:.0f}% success rate).",
        "",
    ]

    if overall_overhead_pct is not None:
        lines.append(
            f"**Platform overhead (scheduling + init containers):** "
            f"{overall_overhead_pct * 100:.0f}% of total step wall-clock time "
            f"({total_overhead_sec:.0f}s out of {total_wall_sec:.0f}s){evidence_note}."
        )
        lines.append("")

    if hints:
        lines += [
            "**Primary bottleneck detected:**",
            "",
        ]
        lines.append(f"> {hints[0]}")
        lines.append("")

    # ================================================================== #
    # 3. Pipeline timing breakdown                                         #
    # ================================================================== #
    lines += [
        "## Pipeline Timing",
        "",
        "| Phase | p50 | p95 | p99 |",
        "|-------|-----|-----|-----|",
        f"| Dispatcher queue (semaphore wait) | {_sec(biz.get('dispatcher_queue_p50') or wf.get('queue_p50'))} | — | — |",
        f"| Dispatcher run | {_sec(biz.get('dispatcher_run_p50'))} | — | — |",
        f"| Pipeline gap (Kafka → omnipass) | — | — | — |",
        f"| Omnipass queue (semaphore wait) | {_sec(wf.get('queue_p50'))} | {_sec(wf.get('queue_p95'))} | {_sec(wf.get('queue_p99'))} |",
        f"| Omnipass run (calrissian) | {_sec(wf.get('duration_p50'))} | {_sec(wf.get('duration_p95'))} | {_sec(wf.get('duration_p99'))} |",
        f"| STAC publish latency | {_sec(biz.get('stac_latency_avg'))} | {_sec(biz.get('stac_latency_p95'))} | — |",
        f"| **End-to-end total** | **{_sec(biz.get('e2e_p50'))}** | **{_sec(biz.get('e2e_p95'))}** | **{_sec(biz.get('e2e_p99'))}** |",
        "",
    ]

    # ================================================================== #
    # 4. Per-step overhead analysis                                        #
    # ================================================================== #
    if steps:
        lines += [
            "## Per-Step Overhead Analysis",
            "",
            "_All durations are p50 of observed executions._  ",
            "_`volume_attach` requires K8s Events API; shown as N/A when events were not collected._",
            "",
            "| Step | wf | n | sched | vol_attach | init_rest | running | total | **overhead%** |",
            "|------|----|---|-------|------------|-----------|---------|-------|---------------|",
        ]

        for s in steps:
            sched   = s.get("scheduling_p50")
            vol     = s.get("volume_attach_p50")
            init_d  = s.get("init_duration_p50") or 0.0
            vol_c   = min(vol, init_d) if vol is not None else None
            init_r  = (init_d - vol_c) if vol_c is not None else init_d
            run     = s.get("running_p50")
            sched_v = sched or 0.0
            run_v   = run   or 0.0
            total   = sched_v + init_d + run_v
            overhead = (sched_v + init_d) / total if total > 0 else None
            lines.append(
                f"| {s['step_name']} "
                f"| {s.get('workflow_type', '?')} "
                f"| {s.get('total_executions', '?')} "
                f"| {_sec(sched)} "
                f"| {_sec(vol_c)} "
                f"| {_sec(init_r) if init_r else 'N/A'} "
                f"| {_sec(run)} "
                f"| {_sec(total) if total > 0 else 'N/A'} "
                f"| {_overhead_badge(overhead)} |"
            )

        lines.append("")

        # Volume attach sub-table (if events available)
        vol_steps = [
            s for s in steps
            if s.get("volume_attach_p50") is not None or s.get("volume_attach_p95") is not None
        ]
        if vol_steps:
            lines += [
                "### Volume Attach Detail (K8s Events)",
                "",
                "| Step | vol_attach p50 | vol_attach p95 | failed_attach_count |",
                "|------|----------------|----------------|---------------------|",
            ]
            for s in vol_steps:
                fa = s.get("failed_attach_total") or 0
                fa_str = f"**{fa} ⚠**" if fa > 0 else str(fa)
                lines.append(
                    f"| {s['step_name']} "
                    f"| {_sec(s.get('volume_attach_p50'))} "
                    f"| {_sec(s.get('volume_attach_p95'))} "
                    f"| {fa_str} |"
                )
            lines.append("")

        # Init duration sub-table
        lines += [
            "### Init Duration Detail",
            "",
            "| Step | init p50 | init p95 | image_pull p95 | init > 15s? |",
            "|------|----------|----------|----------------|-------------|",
        ]
        for s in steps:
            ip50 = s.get("init_duration_p50")
            ip95 = s.get("init_duration_p95")
            pull = s.get("image_pull_p95")
            flag = "**YES ⚠**" if (ip95 or 0) > 15 else "no"
            lines.append(
                f"| {s['step_name']} "
                f"| {_sec(ip50)} "
                f"| {_sec(ip95)} "
                f"| {_sec(pull)} "
                f"| {flag} |"
            )
        lines.append("")

        # Overhead chart — generated by plot_results.py into plots/
        lines += [
            "### Overhead Breakdown Chart",
            "",
            "![Per-Step Overhead Breakdown](plots/step_init_overhead.png)",
            "",
            "_Left panel: absolute seconds per phase. "
            "Right panel: 100 % normalised — dashed line at 50 % separates "
            "'mostly overhead' from 'mostly work'. "
            "Generated by `plot_results.py`._",
            "",
        ]

    # ================================================================== #
    # 5. Resource usage                                                    #
    # ================================================================== #
    resource_steps = [
        s for s in steps
        if s.get("cpu_peak_max") is not None or s.get("mem_peak_max") is not None
    ]
    if resource_steps:
        lines += [
            "## Resource Usage",
            "",
            "| Step | CPU avg | CPU peak avg | CPU peak max | Mem avg | Mem peak max | Throttling |",
            "|------|---------|-------------|--------------|---------|--------------|------------|",
        ]
        for s in resource_steps:
            thr = s.get("cpu_throttling_avg")
            thr_str = f"**{thr * 100:.0f}% ⚠**" if thr is not None and thr > 0.1 else (
                f"{thr * 100:.0f}%" if thr is not None else "N/A"
            )
            lines.append(
                f"| {s['step_name']} "
                f"| {_f(s.get('cpu_avg'), 'm', 0)} "
                f"| {_f(s.get('cpu_peak_avg'), 'm', 0)} "
                f"| {_f(s.get('cpu_peak_max'), 'm', 0)} "
                f"| {_f(s.get('mem_avg'), ' MiB', 0)} "
                f"| {_f(s.get('mem_peak_max'), ' MiB', 0)} "
                f"| {thr_str} |"
            )
        lines.append("")

    # ================================================================== #
    # 6. Reliability                                                       #
    # ================================================================== #
    if steps:
        rel_steps = [
            s for s in steps
            if (s.get("failure_rate") or 0) > 0
            or (s.get("retry_rate") or 0) > 0
            or (s.get("oom_count") or 0) > 0
        ]
        lines += [
            "## Reliability",
            "",
        ]
        if rel_steps:
            lines += [
                "| Step | Executions | Failures | Failure rate | Retries | OOM |",
                "|------|------------|----------|-------------|---------|-----|",
            ]
            for s in rel_steps:
                lines.append(
                    f"| {s['step_name']} "
                    f"| {s.get('total_executions', '?')} "
                    f"| {s.get('failure_count', 0)} "
                    f"| {_pf(s.get('failure_rate'))} "
                    f"| {s.get('retry_count', 0)} "
                    f"| {s.get('oom_count', 0)} |"
                )
            lines.append("")
        else:
            lines += ["All steps completed without failures, retries, or OOM events.", ""]

    # ================================================================== #
    # 7. Bottleneck diagnosis                                              #
    # ================================================================== #
    if hints:
        lines += [
            "## Bottleneck Diagnosis",
            "",
            "_Automatically detected by the KPI engine._",
            "",
        ]
        for i, hint in enumerate(hints, 1):
            lines.append(f"**{i}.** {hint}")
            lines.append("")

    # ================================================================== #
    # 8. Recommendations                                                   #
    # ================================================================== #
    recommendations = _build_recommendations(hints, steps, overall_overhead_pct)
    if recommendations:
        lines += [
            "## Recommendations",
            "",
            "| Priority | Action | Expected impact | Effort |",
            "|----------|--------|----------------|--------|",
        ]
        for rec in recommendations:
            lines.append(
                f"| {rec['priority']} | {rec['action']} "
                f"| {rec['impact']} | {rec['effort']} |"
            )
        lines += ["", "### Detail", ""]
        for rec in recommendations:
            lines += [
                f"#### P{rec['priority']} — {rec['action']}",
                "",
                rec["detail"],
                "",
            ]

    content = "\n".join(lines)
    with open(path, "w", encoding="utf-8") as fh:
        fh.write(content)

    logger.info("Wrote run_summary.md to %s", path)
    return path


# ---------------------------------------------------------------------------
# Recommendation builder
# ---------------------------------------------------------------------------

def _build_recommendations(
    hints: List[str],
    steps: List[dict],
    overall_overhead_pct: Optional[float],
) -> List[dict]:
    """
    Derive a prioritised list of recommendations from bottleneck hints and
    step KPIs.  Returns a list of dicts with keys:
      priority, action, impact, effort, detail
    """
    recs: List[dict] = []
    priority = 1

    hint_text = " ".join(hints).lower()

    # ------------------------------------------------------------------ #
    # R-STORAGE: Longhorn NodeStageVolume / volume attach overhead         #
    # ------------------------------------------------------------------ #
    vol_steps = [
        s for s in steps
        if (s.get("volume_attach_p50") or 0) > 10
        or (s.get("init_duration_p50") or 0) > 15
    ]
    if vol_steps or "longhorn" in hint_text or "volume attach" in hint_text or "nodestage" in hint_text:
        # Estimate savings: current overhead − 10s (realistic residual with NFS subdir)
        total_vol = sum((s.get("volume_attach_p50") or s.get("init_duration_p50") or 0) for s in vol_steps)
        savings_str = f"~{max(0, total_vol - 10):.0f}s/run" if total_vol > 0 else "significant"
        recs.append({
            "priority": priority,
            "action": "Switch PVC storage class to NFS subdir provisioner",
            "impact": f"{savings_str} reduction in init overhead",
            "effort": "Medium",
            "detail": (
                "**Root cause:** Longhorn RWX implements NFS via a per-volume share-manager pod. "
                "Each `NodeStageVolume` (new mount on a node) takes 20–37s because the CSI driver "
                "must verify replica health and start/wait-for the NFS share-manager. "
                "Sequential Argo steps trigger repeated unmount/remount cycles.\n\n"
                "**Fix:** Replace `storageClassRWX: longhorn` with a traditional NFS subdir provisioner "
                "(e.g. `nfs-subdir-external-provisioner`). With a pre-existing NFS server the mount "
                "is a bind operation — typically < 1s. "
                "In the Calrissian Helm chart this is a single value change:\n"
                "```yaml\n"
                "storageClassRWX: nfs-client   # was: longhorn\n"
                "```\n"
                "Verify available storage classes: `kubectl get storageclass`"
            ),
        })
        priority += 1

        recs.append({
            "priority": priority,
            "action": "Add a 'keeper' pod to hold PVC mount open across steps",
            "impact": "~90–95% reduction in remount overhead (no storage class change needed)",
            "effort": "Low",
            "detail": (
                "**Root cause:** Longhorn calls `NodeUnstageVolume` when the **last** pod on a node "
                "using a volume terminates. The next sequential step must wait for a full "
                "`NodeStageVolume` cycle.\n\n"
                "**Fix:** Add a lightweight sidecar step in the Argo WorkflowTemplate that mounts "
                "the calrissian-wdir PVC and sleeps for the entire workflow duration. "
                "This prevents `NodeUnstageVolume` between steps, eliminating all remount cycles "
                "except the very first one.\n\n"
                "```yaml\n"
                "# Argo step (runs in parallel with first PVC-dependent step)\n"
                "- name: pvc-keeper\n"
                "  template: pvc-keeper-tmpl\n"
                "  # Terminates only after the last PVC-using step completes\n"
                "```"
            ),
        })
        priority += 1

    # ------------------------------------------------------------------ #
    # R-AFFINITY: multiple nodes → more NodeStageVolume calls              #
    # ------------------------------------------------------------------ #
    if "node affinity" in hint_text or "multiple node" in hint_text or (
        len({s.get("node_name") for s in steps if s.get("node_name")}) > 1
    ):
        recs.append({
            "priority": priority,
            "action": "Add node affinity to co-locate PVC-dependent steps on one node",
            "impact": "Reduces NodeStageVolume calls from 2 nodes to 1; combined with Longhorn dataLocality=strict-local → local NFS loop",
            "effort": "Low",
            "detail": (
                "**Root cause:** Steps scheduled on different nodes each need an independent "
                "`NodeStageVolume` → separate 20–37s mounts.\n\n"
                "**Fix:** Add `podAffinity` or node selector to all PVC-dependent steps:\n"
                "```yaml\n"
                "affinity:\n"
                "  podAffinity:\n"
                "    requiredDuringSchedulingIgnoredDuringExecution:\n"
                "      - topologyKey: kubernetes.io/hostname\n"
                "        labelSelector: {matchLabels: {workflow-name: '{{workflow.name}}'}}\n"
                "```\n"
                "Also set `dataLocality: strict-local` on the Longhorn volume so the NFS "
                "share-manager runs on the same node as the pod — mount becomes a loopback."
            ),
        })
        priority += 1

    # ------------------------------------------------------------------ #
    # R-CWL-RESOURCES: missing coresMin/ramMin                             #
    # ------------------------------------------------------------------ #
    if "coresmin" in hint_text or "resource requirement" in hint_text or "scheduling" in hint_text:
        recs.append({
            "priority": priority,
            "action": "Add coresMin/ramMin to CWL ResourceRequirement",
            "impact": "Reliable K8s scheduling; avoids over-provisioned pods landing on resource-starved nodes",
            "effort": "Minimal",
            "detail": (
                "**Root cause:** The CWL `appPackage` declares only `coresMax`/`ramMax` → "
                "K8s sets `resources.limits` but not `resources.requests`. "
                "The scheduler may place pods on nodes that are already overloaded.\n\n"
                "**Fix:** Add minimum values to the CWL tool descriptor:\n"
                "```json\n"
                "\"ResourceRequirement\": {\n"
                '  "coresMin": 1, "coresMax": 2,\n'
                '  "ramMin": 2048, "ramMax": 3192\n'
                "}\n"
                "```"
            ),
        })
        priority += 1

    # ------------------------------------------------------------------ #
    # R-SEMAPHORE: workflow queue time is high                             #
    # ------------------------------------------------------------------ #
    if "semaphore" in hint_text or "queue" in hint_text or "case b" in hint_text or "case c" in hint_text:
        recs.append({
            "priority": priority,
            "action": "Increase semaphore limit for ingestor-omnipass",
            "impact": "Reduces workflow queue time; increases pipeline concurrency",
            "effort": "Low",
            "detail": (
                "**Root cause:** The `semaphore-ingestors-uat` ConfigMap caps concurrent "
                "omnipass executions. When the queue time p95 > 60s the limit is a bottleneck.\n\n"
                "**Fix:** Check current limit and cluster capacity:\n"
                "```bash\n"
                "kubectl get configmap semaphore-ingestors-uat -n datalake -o yaml\n"
                "kubectl top nodes\n"
                "```\n"
                "Increase the `workflow` key value proportionally to available CPU/RAM."
            ),
        })
        priority += 1

    # ------------------------------------------------------------------ #
    # R-IMAGE: cold image pull                                             #
    # ------------------------------------------------------------------ #
    if "image pull" in hint_text or "image_pull" in hint_text or "cold" in hint_text:
        recs.append({
            "priority": priority,
            "action": "Pre-cache container images on all nodes (DaemonSet prepuller)",
            "impact": "Eliminates cold image pull latency (typically 200ms–5s per pod)",
            "effort": "Minimal",
            "detail": (
                "**Root cause:** When a container image is not cached on the target node "
                "the kubelet must pull it before starting the init phase, adding seconds to "
                "`init_duration_sec`.\n\n"
                "**Fix:** If a DaemonSet image-prepuller exists, add the affected images. "
                "Otherwise, for the `container-send-message` image (onExit step only) the "
                "impact is minimal and can be deprioritised."
            ),
        })
        priority += 1

    return recs


def _pct(val: Optional[float]) -> Optional[float]:
    """Convert 0..1 ratio to 0..100 percentage."""
    if val is None:
        return None
    return val * 100
