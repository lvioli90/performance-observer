"""
collectors/minio_artifact.py
=============================
Two responsibilities using the same MinIO connection:

1. T0 RESOLUTION — fetch_ingest_time()
   ─────────────────────────────────────
   The true T0 (product dropped into MinIO) is the LastModified timestamp
   of the object in the drop-bucket.  We read it via stat_object() using
   the s3-key from the dispatcher workflow parameter.

   Timeline of the full pipeline:
     T0  Object written to MinIO drop-bucket       ← LastModified (this method)
      ↓  MinIO event → Argo webhook latency (~seconds)
     T1  ingestion-dispatcher workflow created      ← dispatcher.created_at
      ↓  dispatcher queue + run (~seconds)
     T2  dispatcher finished → Kafka dispatched
      ↓  Kafka latency + omnipass scheduling (pipeline_gap_sec)
     T3  ingestor-omnipass created
      ↓  omnipass queue (semaphore wait)
     T4  omnipass started
      ↓  omnipass run (~minutes, calrissian CWL)
     T5  omnipass finished
      ↓  STAC publish latency
     T6  STAC item visible

   Without T0: end_to_end = T6 - T1  (misses MinIO event latency T0→T1)
   With T0:    end_to_end = T6 - T0  (true end-to-end, most accurate)

   T0→T1 (event notification latency) is usually small (<5s at low load)
   but may grow under stress — worth knowing for a proper soak test.

2. STAC ITEM DISCOVERY — poll()
   ─────────────────────────────
   After ingestor-omnipass completes, its exit-handler writes:
     s3://{artifact_bucket}/{workflow.uid}/kafka-message.json

   On SUCCESS:
     {"exit_code": "0",
      "stac_url": "https://discover-uat.iride.earth/collections/{coll}/items/{id}",
      "stac_item": {"id": "...", "collection": "...", ...}}

   On FAILURE:
     {"exit_code": "1", "error": "..."}

   This is the only reliable source of the exact STAC item id and collection
   (the calrissian CWL tool determines them at runtime).

Configuration required (minio.* section in config YAML):
  endpoint:        e.g. "http://minio.datalake.svc:9000"
  access_key:      MinIO access key
  secret_key:      MinIO secret key
  artifact_bucket: Argo artifact repository bucket name
  drop_bucket:     MinIO drop-bucket name (for stat_object T0 reads)
                   default: "drop-bucket" (from dispatcher s3-bucket parameter)
  secure:          false for HTTP, true for HTTPS

Dependencies:
  pip install minio
"""

from __future__ import annotations

import json
import logging
from datetime import datetime, timezone
from typing import Optional, Set

from core.run_context import RunContext

logger = logging.getLogger(__name__)


class MinioArtifactCollector:
    """
    Uses MinIO for two purposes:
      1. stat_object() on the drop-bucket to get true T0 per product
      2. get_object() on the artifact bucket for STAC item discovery
    """

    def __init__(self, ctx: RunContext):
        self.ctx = ctx
        # workflow_name → already checked (dedup for artifact reads)
        self._artifact_checked: Set[str] = set()
        # product_id → already fetched T0 (dedup for stat_object reads)
        self._t0_fetched: Set[str] = set()
        self._client = None
        # Clock-skew error: log actionable message once, then suppress
        self._clock_skew_warned: bool = False

    # ------------------------------------------------------------------
    # MinIO client (lazy init, shared for both T0 and artifact reads)
    # ------------------------------------------------------------------

    def _ensure_client(self) -> bool:
        if self._client is not None:
            return True
        if not self.ctx.minio_endpoint:
            logger.debug("MinIO endpoint not configured; T0 and artifact reading disabled")
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
            logger.info("MinIO client initialized: %s", self.ctx.minio_endpoint)
            return True
        except ImportError:
            logger.warning("minio package not installed. Run: pip install minio")
            return False
        except Exception as exc:
            logger.warning("MinIO client init failed: %s", exc)
            return False

    # ------------------------------------------------------------------
    # 1. T0 resolution: stat_object on drop-bucket
    # ------------------------------------------------------------------

    def fetch_ingest_time(self, product_id: str, s3_key: str, s3_bucket: str) -> Optional[datetime]:
        """
        Return the true T0 for a product: the LastModified timestamp of the
        object in the MinIO drop-bucket.

        This is called by the workflow loop after the dispatcher workflow
        is first correlated (s3_key and s3_bucket are available from dispatcher
        parameters at that point).

        Parameters
        ----------
        product_id : str
            Used for deduplication (only fetch T0 once per product).
        s3_key : str
            The relative object key, e.g. "path/to/product.zip"
        s3_bucket : str
            The bucket name from the dispatcher s3-bucket parameter.

        Returns
        -------
        datetime (UTC) or None if not accessible.
        """
        if product_id in self._t0_fetched:
            return None   # already fetched; caller should read from ProductRecord
        if not self._ensure_client():
            return None

        self._t0_fetched.add(product_id)
        try:
            stat = self._client.stat_object(s3_bucket, s3_key)
            t0 = stat.last_modified
            # minio returns timezone-aware datetime; ensure UTC
            if t0.tzinfo is None:
                t0 = t0.replace(tzinfo=timezone.utc)
            logger.debug(
                "T0 resolved: product_id=%s bucket=%s key=%s last_modified=%s",
                product_id, s3_bucket, s3_key, t0.isoformat(),
            )
            return t0
        except Exception as exc:
            if _is_clock_skew_error(exc):
                if not self._clock_skew_warned:
                    self._clock_skew_warned = True
                    logger.error(
                        "MinioArtifactCollector: clock skew detected — T0 resolution "
                        "from drop-bucket will fail until local clock is synced with %s.\n"
                        "  Fix (Linux):  sudo ntpdate -u pool.ntp.org\n"
                        "            or: sudo timedatectl set-ntp true && sudo systemctl restart systemd-timesyncd\n"
                        "  Fix (macOS):  sudo sntp -sS pool.ntp.org",
                        self.ctx.minio_endpoint,
                    )
            else:
                # Object may have been deleted or key is wrong
                logger.warning(
                    "stat_object failed for s3://%s/%s (product=%s): %s",
                    s3_bucket, s3_key, product_id, exc,
                )
            return None

    # ------------------------------------------------------------------
    # 2. STAC discovery: read kafka-message.json artifact
    # ------------------------------------------------------------------

    def poll(self, correlator) -> int:
        """
        Check completed omnipass workflows for unread kafka-message.json artifacts.
        Returns number of artifacts successfully processed.
        """
        if not self.ctx.corr_use_artifact_stac:
            return 0
        if not self._ensure_client():
            return 0

        omnipass_kw = self.ctx.corr_omnipass_template.lower()
        candidates = [
            wf for wf in self.ctx.snapshot_workflows()
            if omnipass_kw in (wf.template_name or wf.workflow_name).lower()
            and wf.phase in ("Succeeded", "Failed")
            and wf.workflow_name not in self._artifact_checked
            and wf.product_id is not None
            and wf.uid is not None
        ]

        if not candidates:
            return 0

        processed = 0
        for wf in candidates:
            self._artifact_checked.add(wf.workflow_name)
            try:
                artifact = self._read_artifact(wf)
                if artifact is None:
                    # Not yet written — remove from checked so we retry next poll
                    self._artifact_checked.discard(wf.workflow_name)
                    continue
                self._process_artifact(wf, artifact, correlator)
                processed += 1
            except Exception as exc:
                logger.warning("Artifact processing failed for %s: %s", wf.workflow_name, exc)

        if processed:
            logger.info("Artifact poll: %d kafka-message.json artifacts read", processed)
        return processed

    def _read_artifact(self, wf) -> Optional[dict]:
        """
        Fetch {workflow.uid}/kafka-message.json from the Argo artifact bucket.
        Returns None if the object does not yet exist (exit-handler still running).
        """
        bucket = self.ctx.minio_artifact_bucket
        artifact_key = f"{wf.uid}/kafka-message.json"
        try:
            response = self._client.get_object(bucket, artifact_key)
            data = response.read()
            response.close()
            response.release_conn()
            return json.loads(data)
        except Exception as exc:
            if _is_not_found(exc):
                logger.debug("Artifact not yet available: %s/%s", bucket, artifact_key)
                return None
            logger.warning("get_object error for %s/%s: %s", bucket, artifact_key, exc)
            return None

    def _process_artifact(self, wf, artifact: dict, correlator) -> None:
        """Parse kafka-message.json and update ProductRecord via correlator."""
        product_id = wf.product_id
        seen_at = datetime.now(timezone.utc)
        exit_code = str(artifact.get("exit_code", "1"))

        if exit_code == "0":
            stac_url = artifact.get("stac_url", "")
            stac_item = artifact.get("stac_item") or {}
            item_id = stac_item.get("id") or _item_id_from_url(stac_url)
            collection_id = stac_item.get("collection", "")

            if not item_id:
                logger.warning(
                    "Artifact for %s: exit_code=0 but no STAC item id found", product_id
                )
                return

            logger.info(
                "STAC from artifact: product=%s collection=%s item_id=%s",
                product_id, collection_id, item_id,
            )
            correlator.update_stac_from_artifact(
                product_id=product_id,
                stac_item_id=item_id,
                collection_id=collection_id,
                stac_seen_at=seen_at,
                stac_url=stac_url,
            )
        else:
            error_msg = artifact.get("error", "unknown")
            logger.debug("Artifact for product=%s shows failure: %s", product_id, error_msg)
            with self.ctx._lock:
                product = self.ctx.products.get(product_id)
                if product and product.final_status == "in_progress":
                    product.final_status = "failed"
                    self.ctx._products_dirty.append(product_id)


# ------------------------------------------------------------------
# Helpers
# ------------------------------------------------------------------

def _is_not_found(exc: Exception) -> bool:
    """Return True if the MinIO exception means the object doesn't exist yet."""
    msg = str(exc).lower()
    return any(k in msg for k in ("nosuchkey", "no such key", "does not exist", "404"))


def _is_clock_skew_error(exc: Exception) -> bool:
    """Return True if the exception is an S3 RequestTimeTooSkewed error."""
    msg = str(exc)
    return "RequestTimeTooSkewed" in msg or ("request time" in msg.lower() and "skew" in msg.lower())


def _item_id_from_url(stac_url: str) -> Optional[str]:
    """Extract item_id from .../collections/{coll}/items/{item_id}"""
    if not stac_url:
        return None
    parts = stac_url.rstrip("/").split("/")
    if len(parts) >= 2 and parts[-2] == "items":
        return parts[-1]
    return None
