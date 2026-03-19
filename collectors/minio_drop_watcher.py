"""
collectors/minio_drop_watcher.py
=================================
Watches the MinIO drop-bucket for new objects and records T0.

T0 = the moment a product file appears in the drop-bucket.

This is architecturally the correct source of T0:
  - Independent of the Argo workflow chain
  - Observed in near-real-time (polling interval ~15s)
  - Detects products that DROP but NEVER trigger a workflow (trigger failure)
  - Uses object.last_modified (actual write time) not the poll time

Full observable pipeline from this vantage point:
  T0  Object appears in drop-bucket          ← this collector
  T1  ingestion-dispatcher workflow created   ← argo.py
  T2  dispatcher started / finished
  T3  ingestor-omnipass created
  T4  omnipass started / finished
  T5  STAC item visible                       ← minio_artifact.py + stac.py

Objects that have T0 but never reach T1 (no dispatcher workflow within
`orphan_timeout_sec`) are flagged as "orphaned" in the RunContext.

Implementation
--------------
List objects in the drop-bucket using a sliding time-window watermark:
  - On first poll: scan objects modified in the last `lookback_sec` seconds
    (catches in-flight objects that were dropped before the observer started)
  - On subsequent polls: scan objects modified since the last seen timestamp
  - Use object.last_modified (UTC) as T0 — this is the actual write time,
    not the poll time, so it is accurate regardless of polling jitter

Configuration (under minio.* in config YAML):
  drop_bucket:        bucket to watch (default: "drop-bucket")
  drop_prefix:        optional key prefix filter (default: "" = all)
  drop_poll_sec:      polling interval in seconds (default: 15)
  drop_lookback_sec:  on startup, scan this many seconds back (default: 120)
  drop_orphan_sec:    flag as orphan if no dispatcher within N seconds (default: 180)
"""

from __future__ import annotations

import logging
from datetime import datetime, timedelta, timezone
from typing import Dict, Optional, Set

from core.run_context import RunContext

logger = logging.getLogger(__name__)


class MinioDropWatcher:
    """
    Polls the MinIO drop-bucket and records T0 for each new object.

    Discovered objects are stored in ctx.drop_bucket_events:
      Dict[s3_key, datetime]  →  T0 per object key

    The correlator reads from this dict when setting ingest_reference_time.
    """

    def __init__(self, ctx: RunContext):
        self.ctx = ctx
        self._client = None
        # s3_key → T0 (datetime, UTC)
        # Mirrored into ctx.drop_bucket_events for cross-thread access
        self._seen: Dict[str, datetime] = {}
        # Watermark: only list objects newer than this
        self._watermark: Optional[datetime] = None
        # Keys flagged as orphaned (no dispatcher workflow seen within timeout)
        self._orphaned: Set[str] = set()

    # ------------------------------------------------------------------
    # MinIO client (shared factory — same pattern as minio_artifact.py)
    # ------------------------------------------------------------------

    def _ensure_client(self) -> bool:
        if self._client is not None:
            return True
        if not self.ctx.minio_endpoint:
            logger.debug("MinIO endpoint not configured; drop-bucket watching disabled")
            return False
        try:
            from minio import Minio
            endpoint = (
                self.ctx.minio_endpoint
                .replace("http://", "")
                .replace("https://", "")
            )
            self._client = Minio(
                endpoint=endpoint,
                access_key=self.ctx.minio_access_key or None,
                secret_key=self.ctx.minio_secret_key or None,
                secure=self.ctx.minio_secure,
            )
            logger.info(
                "MinioDropWatcher: connected to %s, watching bucket=%s prefix='%s'",
                self.ctx.minio_endpoint,
                self.ctx.minio_drop_bucket,
                self.ctx.minio_drop_prefix,
            )
            return True
        except ImportError:
            logger.warning("minio package not installed. Run: pip install minio")
            return False
        except Exception as exc:
            logger.warning("MinioDropWatcher: client init failed: %s", exc)
            return False

    # ------------------------------------------------------------------
    # Main poll
    # ------------------------------------------------------------------

    def poll(self) -> int:
        """
        List new objects in the drop-bucket since the last watermark.
        Records T0 = object.last_modified for each new object.

        Returns number of new objects discovered.
        """
        if not self._ensure_client():
            return 0

        bucket = self.ctx.minio_drop_bucket
        if not bucket:
            logger.debug("minio.drop_bucket not configured; skipping drop-bucket watch")
            return 0

        now = datetime.now(timezone.utc)

        # On first poll: look back to catch objects dropped before observer started
        if self._watermark is None:
            lookback = timedelta(seconds=self.ctx.minio_drop_lookback_sec)
            self._watermark = now - lookback
            logger.info(
                "MinioDropWatcher: initial scan from %s (lookback=%ds)",
                self._watermark.isoformat(),
                self.ctx.minio_drop_lookback_sec,
            )

        new_objects = 0
        latest_seen = self._watermark

        try:
            objects = self._client.list_objects(
                bucket_name=bucket,
                prefix=self.ctx.minio_drop_prefix or None,
                recursive=True,
            )
            for obj in objects:
                t0 = obj.last_modified
                if t0 is None:
                    continue
                # Ensure timezone-aware
                if t0.tzinfo is None:
                    t0 = t0.replace(tzinfo=timezone.utc)

                # Only process objects newer than the watermark
                if t0 <= self._watermark:
                    continue

                s3_key = obj.object_name
                if s3_key in self._seen:
                    continue   # already recorded

                # New object — record T0
                self._seen[s3_key] = t0
                with self.ctx._lock:
                    self.ctx.drop_bucket_events[s3_key] = t0

                logger.info(
                    "Drop-bucket: new object observed s3://%s/%s T0=%s",
                    bucket, s3_key, t0.isoformat(),
                )
                new_objects += 1

                if t0 > latest_seen:
                    latest_seen = t0

            # Advance watermark to latest object seen (minus a small overlap buffer
            # to avoid missing objects written in the same second on next poll)
            if latest_seen > self._watermark:
                self._watermark = latest_seen - timedelta(seconds=2)

        except Exception as exc:
            logger.warning("MinioDropWatcher: list_objects failed: %s", exc)

        return new_objects

    # ------------------------------------------------------------------
    # Orphan detection
    # ------------------------------------------------------------------

    def detect_orphans(self) -> list:
        """
        Return list of s3_keys that appeared in the drop-bucket but have
        NOT been correlated to any dispatcher workflow within orphan_timeout_sec.

        An orphan indicates the MinIO event notification or Argo trigger failed.
        Called periodically by the observer loop.
        """
        timeout = timedelta(seconds=self.ctx.minio_drop_orphan_sec)
        now = datetime.now(timezone.utc)
        orphans = []

        # Build set of object_keys that HAVE been correlated
        correlated_keys: Set[str] = set()
        for wf in self.ctx.snapshot_workflows():
            if wf.object_key:
                # Strip s3://bucket/ prefix to get relative key
                key = wf.object_key.split("/", 3)[-1] if wf.object_key.startswith("s3://") else wf.object_key
                correlated_keys.add(key)

        for s3_key, t0 in list(self._seen.items()):
            if s3_key in self._orphaned:
                continue  # already flagged
            if s3_key in correlated_keys:
                continue  # successfully correlated
            if (now - t0) > timeout:
                self._orphaned.add(s3_key)
                orphans.append({"s3_key": s3_key, "t0": t0.isoformat()})
                logger.warning(
                    "ORPHAN: s3://%s/%s dropped at %s but no dispatcher workflow "
                    "seen after %ds — possible trigger failure",
                    self.ctx.minio_drop_bucket,
                    s3_key,
                    t0.isoformat(),
                    self.ctx.minio_drop_orphan_sec,
                )

        return orphans

    # ------------------------------------------------------------------
    # T0 lookup (used by correlator)
    # ------------------------------------------------------------------

    def get_t0(self, s3_key: str) -> Optional[datetime]:
        """Return T0 for a given s3_key, or None if not yet seen."""
        return self._seen.get(s3_key)
