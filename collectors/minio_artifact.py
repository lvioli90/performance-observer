"""
collectors/minio_artifact.py
=============================
Reads the kafka-message.json artifact written by the ingestor-omnipass
exit-handler to MinIO after each successful (or failed) workflow run.

Why this exists
---------------
The omnipass exit-handler writes:
  s3://{artifact_bucket}/{workflow.uid}/kafka-message.json

On SUCCESS the file contains:
  {
    "exit_code": "0",
    "stac_url": "https://discover-uat.iride.earth/collections/{coll}/items/{item_id}",
    "stac_item": {
      "id": "<stac item id>",
      "collection": "<collection name>",
      ...full STAC item...
    }
  }

On FAILURE:
  {
    "exit_code": "1",
    "error": "<error message>"
  }

This is the ONLY reliable way to get the exact STAC item id and collection
without guessing, because the calrissian CWL tool determines both values
at runtime and the observer has no other hook to read them.

Strategy
--------
1. After an omnipass workflow reaches Succeeded or Failed phase, its
   workflow.uid is known.
2. This collector reads the artifact at {uid}/kafka-message.json.
3. On success: extract stac_url, stac_item.id, stac_item.collection.
4. Update the ProductRecord via the Correlator.

Plug-in points
--------------
- Requires the `minio` Python package: pip install minio
- minio.* credentials must be set in config.
- The artifact_bucket must match Argo's artifact repository config.
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
    Polls completed omnipass workflows and reads their kafka-message.json
    artifact from MinIO to discover the actual STAC item id and collection.

    Only omnipass workflows in Succeeded or Failed state are checked.
    Each workflow_uid is only checked once (tracked in _checked_uids).
    """

    def __init__(self, ctx: RunContext):
        self.ctx = ctx
        self._checked_uids: Set[str] = set()
        self._minio_client = None

    def _ensure_client(self) -> bool:
        """Lazy-init the MinIO client. Returns True if client is available."""
        if self._minio_client is not None:
            return True
        if not self.ctx.minio_endpoint:
            logger.debug("MinIO endpoint not configured; artifact reading disabled")
            return False
        try:
            from minio import Minio
            self._minio_client = Minio(
                endpoint=self.ctx.minio_endpoint.replace("http://", "").replace("https://", ""),
                access_key=self.ctx.minio_access_key or None,
                secret_key=self.ctx.minio_secret_key or None,
                secure=self.ctx.minio_secure,
            )
            logger.debug("MinIO client initialized: endpoint=%s", self.ctx.minio_endpoint)
            return True
        except ImportError:
            logger.warning(
                "minio package not installed; artifact reading disabled. "
                "Run: pip install minio"
            )
            return False
        except Exception as exc:
            logger.warning("MinIO client init failed: %s", exc)
            return False

    def poll(self, correlator) -> int:
        """
        Check completed omnipass workflows for unread artifacts.

        For each new completed omnipass workflow (not yet in _checked_uids):
          1. Derive the artifact S3 key: {workflow_uid}/kafka-message.json
          2. Read and parse the artifact from MinIO
          3. Update the ProductRecord via correlator

        Returns number of artifacts successfully read.
        """
        if not self.ctx.corr_use_artifact_stac:
            return 0
        if not self._ensure_client():
            return 0

        # Find completed omnipass workflows not yet processed
        omnipass_kw = self.ctx.corr_omnipass_template.lower()
        completed_omnipass = [
            wf for wf in self.ctx.snapshot_workflows()
            if omnipass_kw in (wf.template_name or wf.workflow_name).lower()
            and wf.phase in ("Succeeded", "Failed")
            and wf.workflow_name not in self._checked_uids
            and wf.product_id is not None
        ]

        if not completed_omnipass:
            return 0

        processed = 0
        for wf in completed_omnipass:
            self._checked_uids.add(wf.workflow_name)
            try:
                artifact = self._read_artifact(wf.workflow_name)
                if artifact is None:
                    continue
                self._process_artifact(wf, artifact, correlator)
                processed += 1
            except Exception as exc:
                logger.warning(
                    "Artifact read failed for workflow %s: %s",
                    wf.workflow_name, exc,
                )

        if processed:
            logger.info("MinIO artifact poll: %d artifacts read", processed)
        return processed

    def _read_artifact(self, workflow_name: str) -> Optional[dict]:
        """
        Read the kafka-message.json artifact for a given workflow.

        The artifact key is: {workflow_name}/kafka-message.json
        (Argo uses the workflow name as the uid-based prefix in artifact keys,
        matching the template: s3:key: "{{workflow.uid}}/kafka-message.json").

        NOTE: Argo substitutes workflow.uid at runtime. If your cluster stores
        artifacts under workflow.uid (not workflow.name), look up the uid from
        the WorkflowRecord metadata. Currently we use workflow_name as the key
        since uid is not directly stored on WorkflowRecord. Adjust if needed.

        PLUG-IN: If artifacts use workflow.uid instead of workflow.name,
        add uid to WorkflowRecord and use it here.
        """
        bucket = self.ctx.minio_artifact_bucket
        # Try workflow.name-based key first, then as fallback check if wf has a uid
        artifact_key = f"{workflow_name}/kafka-message.json"

        try:
            response = self._minio_client.get_object(bucket, artifact_key)
            data = response.read()
            response.close()
            response.release_conn()
            return json.loads(data)
        except Exception as exc:
            # Object may not exist yet (e.g., exit-handler still running)
            err_str = str(exc).lower()
            if "nosuchkey" in err_str or "no such key" in err_str or "does not exist" in err_str:
                logger.debug(
                    "Artifact not yet available: bucket=%s key=%s",
                    bucket, artifact_key,
                )
            else:
                logger.warning("MinIO get_object error: %s", exc)
            return None

    def _process_artifact(self, wf, artifact: dict, correlator) -> None:
        """
        Parse the kafka-message.json and update the ProductRecord.

        Success artifact schema:
          {"exit_code": "0", "stac_url": "...", "stac_item": {"id": ..., "collection": ...}}

        Failure artifact schema:
          {"exit_code": "1", "error": "..."}
        """
        exit_code = str(artifact.get("exit_code", "1"))
        product_id = wf.product_id
        seen_at = datetime.now(timezone.utc)

        if exit_code == "0":
            stac_url = artifact.get("stac_url", "")
            stac_item = artifact.get("stac_item") or {}
            item_id = stac_item.get("id") or self._extract_item_id_from_url(stac_url)
            collection_id = stac_item.get("collection", "")

            if not item_id:
                logger.warning(
                    "Artifact for %s has exit_code=0 but no stac item id", wf.workflow_name
                )
                return

            logger.info(
                "STAC item from artifact: product=%s collection=%s item_id=%s",
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
            logger.debug(
                "Artifact for product=%s shows failure: %s", product_id, error_msg
            )
            # Update final_status if workflow shows failure in artifact
            with self.ctx._lock:
                product = self.ctx.products.get(product_id)
                if product and product.final_status == "in_progress":
                    product.final_status = "failed"
                    self.ctx._products_dirty.append(product_id)

    @staticmethod
    def _extract_item_id_from_url(stac_url: str) -> Optional[str]:
        """
        Extract item_id from a STAC URL of the form:
        https://host/collections/{collection}/items/{item_id}
        """
        if not stac_url:
            return None
        parts = stac_url.rstrip("/").split("/")
        if len(parts) >= 2 and parts[-2] == "items":
            return parts[-1]
        return None
