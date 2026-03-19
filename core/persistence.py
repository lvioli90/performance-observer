"""
core/persistence.py
===================
Incremental persistence of observations to NDJSON files during the run,
plus checkpoint save/restore for long-run resilience.

Strategy
--------
- Raw observations are appended to NDJSON (newline-delimited JSON) files
  in the output directory as they arrive.  This is safe for concurrent writes
  because each record is a single atomic ``file.write()`` call.
- Structured CSV / JSON outputs are written by reporting/ modules AFTER the
  run completes (or on demand via compute_kpis.py).
- A checkpoint file stores a snapshot of all in-memory stores so the observer
  can be resumed after an unexpected interruption.

File layout
-----------
  <output_dir>/
    raw/
      workflows.ndjson        <- one WorkflowRecord JSON per line
      pods.ndjson             <- one PodRecord JSON per line
      products.ndjson         <- one ProductRecord JSON per line
      stac.ndjson             <- one StacRecord JSON per line
      timeseries.ndjson       <- one TimeseriesSnapshot JSON per line
    checkpoint.json           <- full state snapshot (optional)
"""

from __future__ import annotations

import dataclasses
import json
import logging
import os
import threading
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional

from core.models import (
    PodRecord,
    ProductRecord,
    StacRecord,
    TimeseriesSnapshot,
    WorkflowRecord,
)

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# JSON serialization helpers
# ---------------------------------------------------------------------------

class _ObserverEncoder(json.JSONEncoder):
    """Serialize datetime objects and dataclasses to JSON."""

    def default(self, obj: Any) -> Any:
        if isinstance(obj, datetime):
            return obj.isoformat()
        if dataclasses.is_dataclass(obj) and not isinstance(obj, type):
            return dataclasses.asdict(obj)
        return super().default(obj)


def _to_json(obj: Any) -> str:
    return json.dumps(obj, cls=_ObserverEncoder, ensure_ascii=False)


def _from_dict_datetime(d: dict, *keys) -> None:
    """Parse ISO datetime strings back to datetime objects in-place."""
    for k in keys:
        val = d.get(k)
        if isinstance(val, str):
            try:
                d[k] = datetime.fromisoformat(val)
            except ValueError:
                d[k] = None


# ---------------------------------------------------------------------------
# PersistenceManager
# ---------------------------------------------------------------------------

class PersistenceManager:
    """
    Manages all incremental I/O for a single run.

    Usage
    -----
    pm = PersistenceManager(output_dir)
    pm.append_workflow(wf_record)
    pm.append_pod(pod_record)
    pm.append_timeseries(snapshot)
    pm.save_checkpoint(ctx)   # periodically
    """

    def __init__(self, output_dir: Path):
        self.output_dir = output_dir
        self.raw_dir = output_dir / "raw"
        self.raw_dir.mkdir(parents=True, exist_ok=True)

        # One write lock per file to prevent interleaving from multiple threads
        self._locks: Dict[str, threading.Lock] = {}
        self._files: Dict[str, Any] = {}

        self._open_all()

    def _open_all(self) -> None:
        names = ["workflows", "pods", "products", "stac", "timeseries"]
        for name in names:
            path = self.raw_dir / f"{name}.ndjson"
            self._locks[name] = threading.Lock()
            # Open in append mode so reruns / resumes don't overwrite
            self._files[name] = open(path, "a", encoding="utf-8")

    def _append(self, name: str, obj: Any) -> None:
        line = _to_json(obj) + "\n"
        with self._locks[name]:
            self._files[name].write(line)
            self._files[name].flush()

    # ------------------------------------------------------------------
    # Public append methods
    # ------------------------------------------------------------------

    def append_workflow(self, record: WorkflowRecord) -> None:
        self._append("workflows", record)

    def append_pod(self, record: PodRecord) -> None:
        self._append("pods", record)

    def append_product(self, record: ProductRecord) -> None:
        self._append("products", record)

    def append_stac(self, record: StacRecord) -> None:
        self._append("stac", record)

    def append_timeseries(self, snapshot: TimeseriesSnapshot) -> None:
        self._append("timeseries", snapshot)

    # ------------------------------------------------------------------
    # Checkpoint save / restore
    # ------------------------------------------------------------------

    def save_checkpoint(self, ctx) -> None:
        """
        Persist a full snapshot of the RunContext in-memory stores to
        ``checkpoint.json``.  This allows a crashed observer to be restarted
        and resume from the last checkpoint rather than from scratch.

        Note: this acquires ctx._lock briefly to snapshot all stores.
        """
        checkpoint_path = self.output_dir / "checkpoint.json"
        try:
            workflows = ctx.snapshot_workflows()
            pods = ctx.snapshot_pods()
            products = ctx.snapshot_products()
            stac_records = ctx.snapshot_stac()
            timeseries = ctx.snapshot_timeseries()

            data = {
                "run_id": ctx.run_id,
                "saved_at": datetime.now(timezone.utc).isoformat(),
                "started_at": ctx.started_at.isoformat(),
                "workflows": [dataclasses.asdict(w) for w in workflows],
                "pods": [dataclasses.asdict(p) for p in pods],
                "products": [dataclasses.asdict(p) for p in products],
                "stac_records": [dataclasses.asdict(s) for s in stac_records],
                "timeseries": [dataclasses.asdict(t) for t in timeseries],
            }

            tmp_path = checkpoint_path.with_suffix(".tmp")
            with open(tmp_path, "w", encoding="utf-8") as fh:
                json.dump(data, fh, cls=_ObserverEncoder, indent=2)
            # Atomic rename to avoid partial writes
            tmp_path.replace(checkpoint_path)
            logger.debug("Checkpoint saved to %s", checkpoint_path)
        except Exception as exc:
            logger.warning("Checkpoint save failed: %s", exc)

    @staticmethod
    def load_checkpoint(checkpoint_path: Path) -> Optional[dict]:
        """
        Load a checkpoint JSON file.  Returns the raw dict for the caller to
        reconstruct RunContext stores from.
        """
        if not checkpoint_path.exists():
            return None
        try:
            with open(checkpoint_path, "r", encoding="utf-8") as fh:
                data = json.load(fh)
            logger.info("Checkpoint loaded from %s", checkpoint_path)
            return data
        except Exception as exc:
            logger.warning("Checkpoint load failed: %s", exc)
            return None

    # ------------------------------------------------------------------
    # Flush helpers (for batch flush of dirty records)
    # ------------------------------------------------------------------

    def flush_dirty_workflows(self, ctx) -> int:
        """Flush newly updated workflow records to disk. Returns count flushed."""
        with ctx._lock:
            dirty = list(set(ctx._workflows_dirty))
            ctx._workflows_dirty.clear()
        count = 0
        for name in dirty:
            record = ctx.workflows.get(name)
            if record:
                self.append_workflow(record)
                count += 1
        return count

    def flush_dirty_pods(self, ctx) -> int:
        with ctx._lock:
            dirty = list(set(ctx._pods_dirty))
            ctx._pods_dirty.clear()
        count = 0
        for name in dirty:
            record = ctx.pods.get(name)
            if record:
                self.append_pod(record)
                count += 1
        return count

    def flush_dirty_products(self, ctx) -> int:
        with ctx._lock:
            dirty = list(set(ctx._products_dirty))
            ctx._products_dirty.clear()
        count = 0
        for pid in dirty:
            record = ctx.products.get(pid)
            if record:
                self.append_product(record)
                count += 1
        return count

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    def close(self) -> None:
        """Flush and close all open file handles."""
        for name, fh in self._files.items():
            try:
                fh.flush()
                fh.close()
            except Exception as exc:
                logger.warning("Error closing %s file: %s", name, exc)
        self._files.clear()


# ---------------------------------------------------------------------------
# NDJSON reader (used by compute_kpis.py and plot_results.py)
# ---------------------------------------------------------------------------

def read_ndjson(path: Path) -> List[dict]:
    """Read all records from an NDJSON file. Returns list of dicts."""
    records = []
    if not path.exists():
        logger.warning("NDJSON file not found: %s", path)
        return records
    with open(path, "r", encoding="utf-8") as fh:
        for lineno, line in enumerate(fh, 1):
            line = line.strip()
            if not line:
                continue
            try:
                records.append(json.loads(line))
            except json.JSONDecodeError as exc:
                logger.warning("Bad JSON at %s line %d: %s", path, lineno, exc)
    return records


def deduplicate_ndjson(records: List[dict], key_field: str) -> List[dict]:
    """
    For NDJSON files that may contain multiple snapshots of the same entity
    (because append_workflow / append_pod are called on each poll), keep only
    the LAST record for each unique key.  This gives the most up-to-date state.
    """
    seen: Dict[str, dict] = {}
    for rec in records:
        key = rec.get(key_field)
        if key is not None:
            seen[key] = rec
    return list(seen.values())
