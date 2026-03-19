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
    Write a human-readable Markdown summary of the run KPIs.
    Useful for quick inspection without opening CSV files.
    """
    path = output_dir / "run_summary.md"

    biz = summary.get("business", {})
    wf = summary.get("workflows", {})
    steps = summary.get("steps", [])
    hints = summary.get("bottleneck_hints", [])
    meta = summary.get("meta", {})

    def fmt(val, unit="", decimals=1) -> str:
        if val is None:
            return "N/A"
        if isinstance(val, float):
            return f"{val:.{decimals}f}{unit}"
        return f"{val}{unit}"

    lines = [
        f"# Performance Observer Run Summary",
        f"",
        f"**Run ID:** {meta.get('run_id', 'N/A')}",
        f"**Started:** {meta.get('started_at', 'N/A')}",
        f"**Finished:** {meta.get('finished_at', 'N/A')}",
        f"**Duration:** {fmt(meta.get('duration_sec'), 's', 0)}",
        f"",
        f"## Business KPIs",
        f"",
        f"| Metric | Value |",
        f"|--------|-------|",
        f"| Products observed | {fmt(biz.get('total_products_observed'), '')} |",
        f"| Products completed | {fmt(biz.get('total_products_completed'), '')} |",
        f"| Products failed | {fmt(biz.get('total_products_failed'), '')} |",
        f"| Success rate | {fmt(_pct(biz.get('success_rate')), '%')} |",
        f"| Avg throughput | {fmt(biz.get('avg_throughput_per_min'), '/min')} |",
        f"| Peak throughput | {fmt(biz.get('peak_throughput_per_min'), '/min')} |",
        f"| E2E latency avg | {fmt(biz.get('e2e_avg'), 's')} |",
        f"| E2E latency p50 | {fmt(biz.get('e2e_p50'), 's')} |",
        f"| E2E latency p95 | {fmt(biz.get('e2e_p95'), 's')} |",
        f"| E2E latency p99 | {fmt(biz.get('e2e_p99'), 's')} |",
        f"| STAC publish latency avg | {fmt(biz.get('stac_latency_avg'), 's')} |",
        f"| STAC publish latency p95 | {fmt(biz.get('stac_latency_p95'), 's')} |",
        f"",
        f"## Workflow KPIs",
        f"",
        f"| Metric | Value |",
        f"|--------|-------|",
        f"| Queue time avg | {fmt(wf.get('queue_avg'), 's')} |",
        f"| Queue time p50 | {fmt(wf.get('queue_p50'), 's')} |",
        f"| Queue time p95 | {fmt(wf.get('queue_p95'), 's')} |",
        f"| Queue time p99 | {fmt(wf.get('queue_p99'), 's')} |",
        f"| Duration avg | {fmt(wf.get('duration_avg'), 's')} |",
        f"| Duration p50 | {fmt(wf.get('duration_p50'), 's')} |",
        f"| Duration p95 | {fmt(wf.get('duration_p95'), 's')} |",
        f"| Duration p99 | {fmt(wf.get('duration_p99'), 's')} |",
        f"| Max pending workflows | {fmt(wf.get('max_pending_workflows'), '')} |",
        f"| Max running workflows | {fmt(wf.get('max_running_workflows'), '')} |",
        f"| Avg concurrency | {fmt(wf.get('avg_concurrency'), '')} |",
        f"",
    ]

    if steps:
        lines += [
            f"## Per-Step KPIs",
            f"",
            f"| Step | Executions | Fail% | Retry% | OOM | Dur p50 | Dur p95 | "
            f"CPU peak max | Mem peak max |",
            f"|------|------------|-------|--------|-----|---------|---------|"
            f"------------|------------|",
        ]
        for step in steps:
            lines.append(
                f"| {step['step_name']} "
                f"| {step['total_executions']} "
                f"| {fmt(_pct(step.get('failure_rate')), '%')} "
                f"| {fmt(_pct(step.get('retry_rate')), '%')} "
                f"| {step.get('oom_count', 0)} "
                f"| {fmt(step.get('duration_p50'), 's')} "
                f"| {fmt(step.get('duration_p95'), 's')} "
                f"| {fmt(step.get('cpu_peak_max'), 'm')} "
                f"| {fmt(step.get('mem_peak_max'), ' MiB')} |"
            )
        lines.append("")

    if hints:
        lines += [f"## Bottleneck Diagnosis", f""]
        for hint in hints:
            lines.append(f"- {hint}")
        lines.append("")

    content = "\n".join(lines)
    with open(path, "w", encoding="utf-8") as fh:
        fh.write(content)

    logger.info("Wrote run_summary.md to %s", path)
    return path


def _pct(val: Optional[float]) -> Optional[float]:
    """Convert 0..1 ratio to 0..100 percentage."""
    if val is None:
        return None
    return val * 100
