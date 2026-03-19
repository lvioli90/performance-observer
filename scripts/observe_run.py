#!/usr/bin/env python3
"""
scripts/observe_run.py
======================
Main observer entry point.

Starts watching an ALREADY-RUNNING ingestion test and collects:
  - Argo workflow states (polled every ``workflow_status_sec``)
  - Kubernetes pod states (polled every ``pod_status_sec``)
  - Kubernetes pod metrics / CPU / memory (polled every ``pod_metrics_sec``)
  - STAC item visibility (polled every ``stac_sec``)

Writes all raw observations incrementally to NDJSON files.
Saves checkpoints periodically to survive interruptions.
Runs a grace period after the injection workflow finishes.

Usage
-----
  python scripts/observe_run.py --config config/example.config.yaml

  # Override run_id from CLI
  python scripts/observe_run.py --config config/example.config.yaml --run-id smoke-001

  # Resume from an existing checkpoint
  python scripts/observe_run.py --config config/example.config.yaml --resume results/baseline-run-001

  # Run for a fixed duration (seconds) then stop
  python scripts/observe_run.py --config config/example.config.yaml --duration 1800
"""

import argparse
import logging
import os
import signal
import sys
import threading
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Optional

# Ensure repo root is on the path when running scripts/ directly
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from core.run_context import RunContext, load_config
from core.correlator import Correlator
from core.models import TimeseriesSnapshot
from core.persistence import PersistenceManager
from collectors.argo import ArgoCollector
from collectors.k8s import K8sCollector
from collectors.stac import StacCollector
from collectors.minio_artifact import MinioArtifactCollector

# ---------------------------------------------------------------------------
# Logging setup
# ---------------------------------------------------------------------------

def _setup_logging(output_dir: Optional[Path] = None, verbose: bool = False) -> None:
    level = logging.DEBUG if verbose else logging.INFO
    fmt = "%(asctime)s %(levelname)-8s %(name)s: %(message)s"
    handlers = [logging.StreamHandler(sys.stdout)]
    if output_dir:
        log_path = output_dir / "observer.log"
        handlers.append(logging.FileHandler(log_path, encoding="utf-8"))
    logging.basicConfig(level=level, format=fmt, handlers=handlers)


# ---------------------------------------------------------------------------
# Rolling window helpers for timeseries
# ---------------------------------------------------------------------------

def _compute_rolling_throughput(ctx: RunContext, window_sec: int = 300) -> Optional[float]:
    """Products completed in the last ``window_sec`` seconds, per minute."""
    cutoff = datetime.now(timezone.utc) - timedelta(seconds=window_sec)
    count = 0
    for p in ctx.snapshot_products():
        if p.final_status == "succeeded":
            ts = p.workflow_finished_at or p.stac_seen_at
            if ts and ts >= cutoff:
                count += 1
    return (count / (window_sec / 60)) if window_sec > 0 else None


def _compute_rolling_queue_stats(ctx: RunContext, window_sec: int = 300):
    """Avg and p95 workflow queue time for workflows that started in last window_sec."""
    import numpy as np
    cutoff = datetime.now(timezone.utc) - timedelta(seconds=window_sec)
    values = []
    for wf in ctx.snapshot_workflows():
        if wf.started_at and wf.started_at >= cutoff and wf.created_at:
            q = (wf.started_at - wf.created_at).total_seconds()
            if q >= 0:
                values.append(q)
    if not values:
        return None, None
    return float(np.mean(values)), float(np.percentile(values, 95))


# ---------------------------------------------------------------------------
# Timeseries snapshot builder
# ---------------------------------------------------------------------------

def _build_timeseries_snapshot(ctx: RunContext, argo_collector: ArgoCollector) -> TimeseriesSnapshot:
    counts = argo_collector.snapshot_counts()
    completed = sum(1 for p in ctx.snapshot_products() if p.final_status == "succeeded")
    throughput = _compute_rolling_throughput(ctx)
    avg_q, p95_q = _compute_rolling_queue_stats(ctx)

    return TimeseriesSnapshot(
        timestamp=datetime.now(timezone.utc),
        workflows_pending=counts["pending"],
        workflows_running=counts["running"],
        workflows_succeeded_total=counts["succeeded"],
        workflows_failed_total=counts["failed"],
        completed_products_total=completed,
        throughput_last_5m=throughput,
        avg_queue_time_last_5m=avg_q,
        p95_queue_time_last_5m=p95_q,
    )


# ---------------------------------------------------------------------------
# Observer loop threads
# ---------------------------------------------------------------------------

def _workflow_loop(
    ctx: RunContext,
    argo: ArgoCollector,
    correlator: Correlator,
    pm: PersistenceManager,
    minio_collector=None,
) -> None:
    """
    Polls Argo workflows, runs correlation, and resolves T0 from MinIO.
    T0 = LastModified of the drop-bucket object (true ingest time).
    Falls back to dispatcher.created_at when MinIO is unavailable.
    """
    logger = logging.getLogger("observer.workflow_loop")
    while not ctx.stop_event.is_set():
        try:
            records = argo.poll()
            for wf in records:
                correlator.correlate_workflow(wf, minio_collector=minio_collector)
            pm.flush_dirty_workflows(ctx)
            pm.flush_dirty_products(ctx)
        except Exception as exc:
            logger.warning("Workflow poll error (will retry): %s", exc)

        ctx.stop_event.wait(timeout=ctx.poll_workflow_sec)


def _pod_loop(
    ctx: RunContext,
    k8s: K8sCollector,
    pm: PersistenceManager,
) -> None:
    """Polls pod statuses on a fixed interval."""
    logger = logging.getLogger("observer.pod_loop")
    while not ctx.stop_event.is_set():
        try:
            k8s.poll_pods()
            pm.flush_dirty_pods(ctx)
        except Exception as exc:
            logger.warning("Pod poll error (will retry): %s", exc)

        ctx.stop_event.wait(timeout=ctx.poll_pod_sec)


def _metrics_loop(
    ctx: RunContext,
    k8s: K8sCollector,
    pm: PersistenceManager,
) -> None:
    """Polls pod resource metrics on a fixed interval."""
    logger = logging.getLogger("observer.metrics_loop")
    while not ctx.stop_event.is_set():
        try:
            k8s.poll_metrics()
            pm.flush_dirty_pods(ctx)
        except Exception as exc:
            logger.warning("Metrics poll error (will retry): %s", exc)

        ctx.stop_event.wait(timeout=ctx.poll_metrics_sec)


def _stac_loop(
    ctx: RunContext,
    stac: StacCollector,
    pm: PersistenceManager,
) -> None:
    """Polls STAC for item visibility on a fixed interval."""
    logger = logging.getLogger("observer.stac_loop")
    while not ctx.stop_event.is_set():
        try:
            new_items = stac.poll()
            for item in new_items:
                pm.append_stac(item)
            pm.flush_dirty_products(ctx)
        except Exception as exc:
            logger.warning("STAC poll error (will retry): %s", exc)

        ctx.stop_event.wait(timeout=ctx.poll_stac_sec)


def _timeseries_loop(
    ctx: RunContext,
    argo: ArgoCollector,
    pm: PersistenceManager,
) -> None:
    """Captures timeseries snapshots and flushes them to disk."""
    logger = logging.getLogger("observer.timeseries_loop")
    while not ctx.stop_event.is_set():
        try:
            snapshot = _build_timeseries_snapshot(ctx, argo)
            ctx.append_timeseries(snapshot)
            pm.append_timeseries(snapshot)
        except Exception as exc:
            logger.warning("Timeseries snapshot error: %s", exc)

        ctx.stop_event.wait(timeout=ctx.poll_timeseries_flush_sec)


def _artifact_loop(
    ctx: RunContext,
    minio_collector,
    correlator,
    pm: PersistenceManager,
) -> None:
    """
    Polls MinIO for kafka-message.json artifacts from completed omnipass workflows.
    This is the primary STAC item discovery path (most reliable source).
    """
    logger = logging.getLogger("observer.artifact_loop")
    while not ctx.stop_event.is_set():
        try:
            minio_collector.poll(correlator)
            pm.flush_dirty_products(ctx)
        except Exception as exc:
            logger.warning("Artifact poll error (will retry): %s", exc)

        ctx.stop_event.wait(timeout=ctx.poll_artifact_sec)


def _checkpoint_loop(ctx: RunContext, pm: PersistenceManager) -> None:
    """Periodically saves a checkpoint of the full in-memory state."""
    logger = logging.getLogger("observer.checkpoint_loop")
    if ctx.checkpoint_interval_sec <= 0:
        return
    while not ctx.stop_event.is_set():
        ctx.stop_event.wait(timeout=ctx.checkpoint_interval_sec)
        if ctx.stop_event.is_set():
            break
        try:
            pm.save_checkpoint(ctx)
        except Exception as exc:
            logger.warning("Checkpoint error: %s", exc)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> None:
    parser = argparse.ArgumentParser(
        description="Performance Observer: watch an ongoing ingestion run"
    )
    parser.add_argument(
        "--config", required=True,
        help="Path to YAML config file (e.g. config/example.config.yaml)"
    )
    parser.add_argument("--run-id", default=None, help="Override run_id from config")
    parser.add_argument(
        "--duration", type=int, default=None,
        help="Override max_duration_sec from config"
    )
    parser.add_argument(
        "--resume", default=None,
        help="Path to an existing run output dir to resume from its checkpoint"
    )
    parser.add_argument("--verbose", "-v", action="store_true", help="Debug logging")
    args = parser.parse_args()

    # Load config
    cfg = load_config(args.config)
    if args.run_id:
        cfg["run_id"] = args.run_id
    if args.duration is not None:
        cfg.setdefault("timing", {})["max_duration_sec"] = args.duration

    ctx = RunContext(cfg)
    _setup_logging(ctx.output_dir, verbose=args.verbose)
    logger = logging.getLogger("observer.main")
    logger.info("=" * 60)
    logger.info("Performance Observer starting — IRIDE ingestion pipeline")
    logger.info("run_id            = %s", ctx.run_id)
    logger.info("output_dir        = %s", ctx.output_dir)
    logger.info("argo_ns           = %s", ctx.argo_namespace)
    logger.info("dispatcher tpl    = %s", ctx.corr_dispatcher_template)
    logger.info("omnipass tpl      = %s", ctx.corr_omnipass_template)
    logger.info("max_duration      = %ds", ctx.max_duration_sec)
    logger.info("grace_period      = %ds", ctx.grace_period_sec)
    logger.info("poll_pod_sec      = %ds  (aggressive: podGC 30s on omnipass)", ctx.poll_pod_sec)
    logger.info("poll_metrics_sec  = %ds  (aggressive: podGC 30s on omnipass)", ctx.poll_metrics_sec)
    logger.info("minio_endpoint    = %s", ctx.minio_endpoint or "(not configured)")
    logger.info("artifact_stac     = %s", ctx.corr_use_artifact_stac)
    logger.info("=" * 60)
    if not ctx.minio_endpoint:
        logger.warning(
            "MinIO not configured — STAC discovery will use API polling fallback only. "
            "Set minio.* in config for artifact-based (reliable) STAC item discovery."
        )

    # Initialize components
    pm = PersistenceManager(ctx.output_dir)
    argo = ArgoCollector(ctx)
    k8s = K8sCollector(ctx)
    stac_col = StacCollector(ctx)
    minio_col = MinioArtifactCollector(ctx)
    correlator = Correlator(ctx)

    # Optional resume from checkpoint
    if args.resume:
        checkpoint_path = Path(args.resume) / "checkpoint.json"
        data = PersistenceManager.load_checkpoint(checkpoint_path)
        if data:
            logger.info("Resuming from checkpoint: %s", checkpoint_path)
            # NOTE: Full resume re-population from checkpoint is left as a
            # future enhancement.  The raw NDJSON files are preserved and can
            # be re-read by compute_kpis.py for analysis regardless.

    # Signal handling for graceful shutdown
    def _handle_signal(signum, frame):
        logger.info("Signal %d received; stopping observer...", signum)
        ctx.request_stop()

    signal.signal(signal.SIGINT, _handle_signal)
    signal.signal(signal.SIGTERM, _handle_signal)

    # Launch background threads
    threads = [
        threading.Thread(
            target=_workflow_loop, args=(ctx, argo, correlator, pm, minio_col),
            name="workflow-loop", daemon=True
        ),
        threading.Thread(
            target=_pod_loop, args=(ctx, k8s, pm),
            name="pod-loop", daemon=True
            # NOTE: poll_pod_sec=15 is intentionally aggressive due to
            # podGC: OnPodSuccess with deleteDelayDuration=30s on ingestor-omnipass.
        ),
        threading.Thread(
            target=_metrics_loop, args=(ctx, k8s, pm),
            name="metrics-loop", daemon=True
            # NOTE: poll_metrics_sec=20 for same reason — pods are deleted 30s after success.
        ),
        threading.Thread(
            target=_stac_loop, args=(ctx, stac_col, pm),
            name="stac-loop", daemon=True
            # Fallback STAC polling for when MinIO artifact is unavailable.
        ),
        threading.Thread(
            target=_artifact_loop, args=(ctx, minio_col, correlator, pm),
            name="artifact-loop", daemon=True
            # Primary STAC discovery: reads kafka-message.json from MinIO after omnipass completes.
        ),
        threading.Thread(
            target=_timeseries_loop, args=(ctx, argo, pm),
            name="timeseries-loop", daemon=True
        ),
        threading.Thread(
            target=_checkpoint_loop, args=(ctx, pm),
            name="checkpoint-loop", daemon=True
        ),
    ]

    for t in threads:
        t.start()
        logger.debug("Started thread: %s", t.name)

    # --- Main wait loop ---
    logger.info("Observer running. Press Ctrl-C to stop early.")
    try:
        while not ctx.stop_event.is_set():
            time.sleep(5)

            # Time limit check
            if ctx.is_time_limit_reached():
                logger.info(
                    "Max duration %ds reached. Entering grace period (%ds)...",
                    ctx.max_duration_sec,
                    ctx.grace_period_sec,
                )
                # Wait grace period before stopping (to catch late STAC items)
                ctx.stop_event.wait(timeout=ctx.grace_period_sec)
                ctx.request_stop()
                break

            # Progress log every 60 seconds
            elapsed = ctx.elapsed_sec()
            if int(elapsed) % 60 < 5:
                wfs = ctx.snapshot_workflows()
                prods = ctx.snapshot_products()
                pending = sum(1 for w in wfs if w.phase == "Pending")
                running = sum(1 for w in wfs if w.phase == "Running")
                completed = sum(1 for p in prods if p.final_status == "succeeded")
                logger.info(
                    "[+%dm] workflows: pending=%d running=%d | products: completed=%d",
                    int(elapsed / 60),
                    pending,
                    running,
                    completed,
                )

    except KeyboardInterrupt:
        logger.info("KeyboardInterrupt; stopping...")
        ctx.request_stop()

    # --- Shutdown ---
    logger.info("Waiting for threads to finish...")
    ctx.stop_event.set()
    for t in threads:
        t.join(timeout=10)

    # Final flush and checkpoint
    logger.info("Final data flush...")
    pm.flush_dirty_workflows(ctx)
    pm.flush_dirty_pods(ctx)
    pm.flush_dirty_products(ctx)
    pm.save_checkpoint(ctx)
    pm.close()

    # Print summary
    wfs = ctx.snapshot_workflows()
    prods = ctx.snapshot_products()
    pods = ctx.snapshot_pods()
    completed = sum(1 for p in prods if p.final_status == "succeeded")
    failed = sum(1 for p in prods if p.final_status == "failed")

    logger.info("=" * 60)
    logger.info("Observer finished")
    logger.info("  Workflows observed : %d", len(wfs))
    logger.info("  Pods observed      : %d", len(pods))
    logger.info("  Products correlated: %d", len(prods))
    logger.info("  Completed          : %d", completed)
    logger.info("  Failed             : %d", failed)
    logger.info("  Duration           : %.0fs", ctx.elapsed_sec())
    logger.info("  Output dir         : %s", ctx.output_dir)
    logger.info("")
    logger.info("Next steps:")
    logger.info("  python scripts/compute_kpis.py --run-dir %s", ctx.output_dir)
    logger.info("  python scripts/plot_results.py --run-dir %s", ctx.output_dir)
    logger.info("=" * 60)


if __name__ == "__main__":
    main()
