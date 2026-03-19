"""
core/correlator.py
==================
Correlation logic for the IRIDE ingestion pipeline.

Pipeline architecture
---------------------
  MinIO drop-bucket/{s3-key}
      │
      └─► ingestion-dispatcher workflow
              param: s3-key   = "path/to/product.zip"
              param: s3-bucket = "drop-bucket"
              │
              │  on success → Kafka iride.{partitionkey}.dispatched
              │  (triggers omnipass externally)
              │
              └─► ingestor-omnipass workflow
                      param: reference = "s3://drop-bucket/path/to/product.zip"
                      │
                      │  steps: resolve-config → prepare → argo-cwl (calrissian)
                      │
                      │  on exit → writes {workflow.uid}/kafka-message.json to MinIO
                      │            contains: stac_url, stac_item.id, stac_item.collection
                      │
                      └─► STAC catalog

Shared identifier
-----------------
The s3 object key is the only identifier shared by both workflows:
  dispatcher:  s3-key parameter          (relative: "path/to/product.zip")
  omnipass:    reference parameter        (absolute: "s3://drop-bucket/path/to/product.zip")
  product_id:  filename without extension ("product")

Correlation priority
--------------------
1. PARAMETER-BASED (primary):
   - For dispatcher: read 's3-key' parameter → extract product_id by regex
   - For omnipass:   read 'reference' parameter → strip s3://bucket/ → same regex
   Both link to the same ProductRecord via product_id.

2. ARTIFACT-BASED STAC discovery (when MinIO is accessible):
   After omnipass completes, read {workflow.uid}/kafka-message.json from MinIO.
   This gives the exact STAC item id and collection without guessing.
   Handled by collectors/minio_artifact.py + update_stac_from_artifact().

3. TIME-WINDOW FALLBACK (last resort):
   Match workflows by creation timestamp proximity. Unreliable at high concurrency
   — logged but not used for automatic ProductRecord creation.

IMPORTANT: podGC in omnipass
-----------------------------
ingestor-omnipass has podGC: OnPodSuccess with deleteDelayDuration=30s.
Succeeded pods are deleted from Kubernetes after 30s.
The pod status poller (k8s.py) must therefore run aggressively (≤15s interval)
to catch pod timing before GC removes them from the API.
"""

from __future__ import annotations

import logging
import re
from datetime import datetime, timedelta, timezone
from typing import Dict, List, Optional

from core.models import ProductRecord, WorkflowRecord
from core.run_context import RunContext

logger = logging.getLogger(__name__)

# Workflow type identifiers (matched against template_name or workflow_name)
_DISPATCHER = "dispatcher"
_OMNIPASS = "omnipass"


class Correlator:
    """
    Maintains the product ↔ workflow ↔ pod ↔ STAC correlation map.

    Call ``correlate_workflow(record)`` on every WorkflowRecord update.
    The correlator detects the workflow type (dispatcher vs omnipass),
    extracts the product_id, and upserts the shared ProductRecord.
    """

    def __init__(self, ctx: RunContext):
        self.ctx = ctx
        self._product_id_re: Optional[re.Pattern] = None
        if ctx.corr_product_id_s3_key_regex:
            self._product_id_re = re.compile(ctx.corr_product_id_s3_key_regex)

    # ------------------------------------------------------------------
    # Main entry point
    # ------------------------------------------------------------------

    def correlate_workflow(
        self,
        wf: WorkflowRecord,
        minio_collector=None,
    ) -> Optional[str]:
        """
        Determine product_id for a workflow and upsert the ProductRecord.

        Parameters
        ----------
        wf : WorkflowRecord
        minio_collector : MinioArtifactCollector or None
            If provided and this is a dispatcher workflow, resolve T0 from
            ctx.drop_bucket_events (set by MinioDropWatcher) or fall back
            to MinIO stat_object via minio_collector.

        Returns product_id if found, None otherwise.
        """
        wf_type = self._detect_workflow_type(wf)
        if wf_type is None:
            logger.debug(
                "Unknown workflow type for %s (template=%s); skipping",
                wf.workflow_name, wf.template_name,
            )
            return None

        product_id = None
        if wf_type == _DISPATCHER:
            product_id = self._correlate_dispatcher(wf)
        elif wf_type == _OMNIPASS:
            product_id = self._correlate_omnipass(wf)

        if product_id is None:
            logger.warning(
                "Cannot correlate %s wf=%s (params=%s) — "
                "check that pod annotations contain inputs.parameters "
                "or that the drop-bucket watcher is running",
                wf_type, wf.workflow_name, list(wf.parameters.keys()) or "empty",
            )
            return None

        wf.product_id = product_id
        self._upsert_product(product_id, wf, wf_type)

        # Resolve true T0 from MinIO stat_object immediately after dispatcher correlation.
        # T0 = LastModified of the object in the drop-bucket = most accurate ingest time.
        if wf_type == _DISPATCHER and minio_collector is not None:
            self._resolve_t0(product_id, wf, minio_collector)

        return product_id

    def _resolve_t0(self, product_id: str, wf: WorkflowRecord, minio_collector) -> None:
        """
        Set ingest_reference_time (true T0) on the ProductRecord.

        Priority:
          1. ctx.drop_bucket_events[s3_key]  — set by MinioDropWatcher in real-time
             (most accurate: records the exact moment the object appeared)
          2. minio_collector.fetch_ingest_time()  — retroactive stat_object call
             (accurate but requires a live MinIO connection at correlation time)
          3. Falls back to dispatcher.created_at already set in _update_from_dispatcher
        """
        s3_key = wf.parameters.get(self.ctx.corr_dispatcher_s3_key_param, "")
        if not s3_key:
            return

        # Priority 1: real-time observation from drop-bucket watcher
        with self.ctx._lock:
            t0 = self.ctx.drop_bucket_events.get(s3_key)

        if t0 is not None:
            source = "drop-watcher"
        else:
            # Priority 2: retroactive stat_object via MinioArtifactCollector
            s3_bucket = wf.parameters.get(
                self.ctx.corr_dispatcher_s3_bucket_param, "drop-bucket"
            )
            t0 = minio_collector.fetch_ingest_time(product_id, s3_key, s3_bucket)
            source = "stat_object"

        if t0 is None:
            return   # falls back to dispatcher.created_at set in _update_from_dispatcher

        with self.ctx._lock:
            product = self.ctx.products.get(product_id)
            if product is not None:
                product.ingest_reference_time = t0
                self.ctx._products_dirty.append(product_id)
                dispatcher_created = product.dispatcher_created_at
                delta = (
                    (dispatcher_created - t0).total_seconds()
                    if dispatcher_created else float("nan")
                )
                logger.info(
                    "T0 set from %s: product_id=%s T0=%s "
                    "(dispatcher.created_at=%s delta=%.1fs)",
                    source,
                    product_id,
                    t0.isoformat(),
                    dispatcher_created.isoformat() if dispatcher_created else "N/A",
                    delta,
                )

    # ------------------------------------------------------------------
    # Workflow type detection
    # ------------------------------------------------------------------

    def _detect_workflow_type(self, wf: WorkflowRecord) -> Optional[str]:
        """
        Identify whether a workflow is a dispatcher or omnipass ingestor.
        Checks template_name first, then workflow_name as fallback.
        """
        search_in = [
            (wf.template_name or "").lower(),
            wf.workflow_name.lower(),
        ]
        dispatcher_kw = self.ctx.corr_dispatcher_template.lower()
        omnipass_kw = self.ctx.corr_omnipass_template.lower()

        for text in search_in:
            if dispatcher_kw and dispatcher_kw in text:
                return _DISPATCHER
            if omnipass_kw and omnipass_kw in text:
                return _OMNIPASS
        return None

    # ------------------------------------------------------------------
    # Dispatcher correlation
    # ------------------------------------------------------------------

    def _correlate_dispatcher(self, wf: WorkflowRecord) -> Optional[str]:
        """
        Extract product_id from the dispatcher's 's3-key' parameter.
        Also builds the full s3_url and stores it as object_key.

          s3-key   = "path/to/S2A_MSIL2A_20230101.zip"
          s3-bucket = "drop-bucket"
          object_key = "s3://drop-bucket/path/to/S2A_MSIL2A_20230101.zip"
          product_id = "S2A_MSIL2A_20230101"
        """
        s3_key_param = self.ctx.corr_dispatcher_s3_key_param
        s3_bucket_param = self.ctx.corr_dispatcher_s3_bucket_param

        s3_key = wf.parameters.get(s3_key_param, "")
        s3_bucket = wf.parameters.get(s3_bucket_param, "drop-bucket")

        if not s3_key:
            # Fallback: match against MinIO drop-watcher events by time-window.
            # Needed when pod annotations don't carry workflow-level parameters.
            s3_key, s3_bucket_from_event = self._s3_key_from_drop_events(wf)
            if s3_key:
                s3_bucket = s3_bucket_from_event or s3_bucket
                logger.info(
                    "Dispatcher %s: '%s' not in pod params — resolved from drop-watcher: %s",
                    wf.workflow_name, s3_key_param, s3_key,
                )
            else:
                logger.debug(
                    "Dispatcher %s has no '%s' parameter and no drop-watcher match",
                    wf.workflow_name, s3_key_param,
                )
                return None

        s3_url = f"s3://{s3_bucket}/{s3_key}"
        wf.object_key = s3_url

        product_id = self._extract_product_id_from_s3_key(s3_key)
        if product_id:
            logger.info(
                "Dispatcher correlated: wf=%s s3_key=%s product_id=%s",
                wf.workflow_name, s3_key, product_id,
            )
        return product_id

    # ------------------------------------------------------------------
    # Omnipass correlation
    # ------------------------------------------------------------------

    def _correlate_omnipass(self, wf: WorkflowRecord) -> Optional[str]:
        """
        Extract product_id from the omnipass 'reference' parameter.

          reference = "s3://drop-bucket/path/to/S2A_MSIL2A_20230101.zip"
          object_key = reference (stored as-is)
          s3_key extracted = "path/to/S2A_MSIL2A_20230101.zip"
          product_id = "S2A_MSIL2A_20230101"
        """
        reference_param = self.ctx.corr_omnipass_reference_param
        reference = wf.parameters.get(reference_param, "")

        if not reference:
            # Fallback: match against a dispatcher ProductRecord by time-window.
            # Needed when pod annotations don't carry workflow-level parameters.
            product_id = self._product_id_from_dispatcher_timing(wf)
            if product_id:
                logger.info(
                    "Omnipass %s: '%s' not in pod params — matched product_id=%s "
                    "from dispatcher timing",
                    wf.workflow_name, reference_param, product_id,
                )
            else:
                logger.debug(
                    "Omnipass %s has no '%s' parameter and no dispatcher match",
                    wf.workflow_name, reference_param,
                )
            return product_id

        wf.object_key = reference

        # Strip the "s3://bucket/" prefix to get the relative key
        s3_key = re.sub(r"^s3://[^/]+/", "", reference)

        product_id = self._extract_product_id_from_s3_key(s3_key)
        if product_id:
            logger.info(
                "Omnipass correlated: wf=%s reference=%s product_id=%s",
                wf.workflow_name, reference, product_id,
            )
        return product_id

    # ------------------------------------------------------------------
    # Parameter fallback helpers
    # ------------------------------------------------------------------

    def _s3_key_from_drop_events(
        self, wf: WorkflowRecord
    ) -> tuple:
        """
        Find the closest MinIO drop-watcher event within corr_time_window_sec
        of wf.created_at.  Returns (s3_key, s3_bucket) or ("", "").
        """
        if wf.created_at is None:
            return ("", "")
        window = self.ctx.corr_time_window_sec
        with self.ctx._lock:
            events = dict(self.ctx.drop_bucket_events)
        best_key = ""
        best_delta: Optional[float] = None
        for s3_key, t0 in events.items():
            delta = abs((wf.created_at - t0).total_seconds())
            if delta <= window and (best_delta is None or delta < best_delta):
                best_delta = delta
                best_key = s3_key
        bucket = self.ctx.minio_drop_bucket if best_key else ""
        return (best_key, bucket)

    def _product_id_from_dispatcher_timing(
        self, wf: WorkflowRecord
    ) -> Optional[str]:
        """
        Find a dispatcher-correlated ProductRecord whose dispatcher finished
        within corr_time_window_sec before this omnipass started.
        Used when omnipass pod annotations don't carry the 'reference' parameter.
        """
        if wf.created_at is None:
            return None
        window = self.ctx.corr_time_window_sec
        with self.ctx._lock:
            products = list(self.ctx.products.values())
        best_id: Optional[str] = None
        best_delta: Optional[float] = None
        for prod in products:
            # Only match products that have a dispatcher but no omnipass yet
            if not prod.dispatcher_finished_at:
                continue
            if prod.workflow_name:
                continue  # already has an omnipass
            delta = (wf.created_at - prod.dispatcher_finished_at).total_seconds()
            if 0 <= delta <= window and (best_delta is None or delta < best_delta):
                best_delta = delta
                best_id = prod.product_id
        return best_id

    # ------------------------------------------------------------------
    # Regex helpers
    # ------------------------------------------------------------------

    def _extract_product_id_from_s3_key(self, s3_key: str) -> Optional[str]:
        """Apply the configured regex to extract product_id from an s3 key."""
        if not self._product_id_re or not s3_key:
            return None
        m = self._product_id_re.search(s3_key)
        if m:
            try:
                return m.group("product_id")
            except IndexError:
                return None
        return None

    # ------------------------------------------------------------------
    # ProductRecord upsert
    # ------------------------------------------------------------------

    def _upsert_product(
        self, product_id: str, wf: WorkflowRecord, wf_type: str
    ) -> ProductRecord:
        """
        Create or update the ProductRecord for a given product_id.

        Both dispatcher and omnipass workflows contribute to the same record:
        - dispatcher provides: workflow_created_at, workflow_started_at (fast steps)
        - omnipass provides:   the actual processing times (heavy steps)
        The ProductRecord uses omnipass timing for the core workflow KPIs
        because that is where the real work happens. Both are stored for
        full end-to-end analysis.
        """
        with self.ctx._lock:
            existing = self.ctx.products.get(product_id)

            if existing is None:
                record = ProductRecord(
                    run_id=self.ctx.run_id,
                    product_id=product_id,
                    object_key=wf.object_key,
                )
                self.ctx.products[product_id] = record
                self.ctx._products_dirty.append(product_id)
                logger.info(
                    "New product tracked: product_id=%s from %s wf=%s",
                    product_id, wf_type, wf.workflow_name,
                )
            else:
                record = existing
                # Prefer the earliest object_key (dispatcher's may be cleaner)
                if wf.object_key and not record.object_key:
                    record.object_key = wf.object_key

            # Update fields based on workflow type
            if wf_type == _DISPATCHER:
                self._update_from_dispatcher(record, wf)
            elif wf_type == _OMNIPASS:
                self._update_from_omnipass(record, wf)

            self.ctx._products_dirty.append(product_id)
            return record

    def _update_from_dispatcher(
        self, record: ProductRecord, wf: WorkflowRecord
    ) -> None:
        """
        Populate ProductRecord fields from the dispatcher workflow.

        The dispatcher is the entry point — its created_at is the earliest
        observable timestamp and serves as ingest_reference_time.
        """
        record.dispatcher_workflow_name = wf.workflow_name

        # ingest_reference_time: use dispatcher created_at as the best available
        # approximation of when the product entered the system.
        if record.ingest_reference_time is None and wf.created_at:
            record.ingest_reference_time = wf.created_at

        # Dispatcher-level timing (fast: typically seconds)
        if record.dispatcher_created_at is None:
            record.dispatcher_created_at = wf.created_at
        record.dispatcher_started_at = wf.started_at
        record.dispatcher_finished_at = wf.finished_at
        record.dispatcher_status = wf.phase

    def _update_from_omnipass(
        self, record: ProductRecord, wf: WorkflowRecord
    ) -> None:
        """
        Populate ProductRecord fields from the omnipass ingestor workflow.

        The omnipass is where the actual ingestion happens — its timing
        determines the core workflow_queue_sec, workflow_run_sec KPIs.
        """
        record.workflow_name = wf.workflow_name
        record.workflow_created_at = wf.created_at
        record.workflow_started_at = wf.started_at
        record.workflow_finished_at = wf.finished_at
        record.final_status = _phase_to_status(wf.phase)

        # If dispatcher didn't provide ingest_reference_time, fall back to
        # omnipass created_at (slightly less accurate but still useful).
        if record.ingest_reference_time is None and wf.created_at:
            record.ingest_reference_time = wf.created_at

    # ------------------------------------------------------------------
    # STAC update from MinIO artifact
    # ------------------------------------------------------------------

    def update_stac_from_artifact(
        self,
        product_id: str,
        stac_item_id: str,
        collection_id: str,
        stac_seen_at: datetime,
        stac_url: Optional[str] = None,
    ) -> None:
        """
        Register STAC coordinates (item_id, collection) discovered from the
        omnipass kafka-message.json artifact.

        Called by collectors/minio_artifact.py after reading the artifact.

        IMPORTANT — re-ingestion semantics:
        The artifact tells us *which* STAC item was written, but NOT when the
        STAC catalog actually reflected the update.  For re-ingested products
        the item already exists in the catalog, so finding it is not proof of
        publication.  The authoritative signal is ``properties.updated`` from
        the STAC API, which the StacCollector checks on every polling cycle.

        Therefore this method only stores the STAC coordinates in a StacRecord
        and does NOT set ``product.stac_seen_at``.  The StacCollector will set
        ``stac_seen_at`` once it confirms that ``properties.updated`` is past
        ``workflow_finished_at``.
        """
        from core.models import StacRecord

        stac_record = StacRecord(
            run_id=self.ctx.run_id,
            product_id=product_id,
            stac_item_id=stac_item_id,
            collection_id=collection_id,
            first_seen_at=None,        # set by StacCollector after verifying properties.updated
            verification_status="pending_update_check",
            discovery_method="artifact",
            stac_url=stac_url,
        )
        self.ctx.add_or_update_stac(stac_record)
        logger.debug(
            "STAC coordinates registered from artifact: product_id=%s item_id=%s collection=%s"
            " (stac_seen_at deferred to StacCollector)",
            product_id, stac_item_id, collection_id,
        )

    # ------------------------------------------------------------------
    # STAC update from polling
    # ------------------------------------------------------------------

    def update_stac_from_poll(
        self, product_id: str, stac_seen_at: datetime
    ) -> None:
        """
        Mark a product as seen in STAC from direct polling (fallback path).
        """
        with self.ctx._lock:
            product = self.ctx.products.get(product_id)
            if product and product.stac_seen_at is None:
                product.stac_seen_at = stac_seen_at
                self.ctx._products_dirty.append(product_id)

    # ------------------------------------------------------------------
    # Time-window fallback (informational)
    # ------------------------------------------------------------------

    def time_window_candidates(
        self,
        reference_time: datetime,
        window_sec: Optional[int] = None,
    ) -> List[WorkflowRecord]:
        """
        Return workflows whose created_at falls within window_sec of reference_time.
        For informational use only — not used for automatic correlation.
        """
        window = timedelta(seconds=window_sec or self.ctx.corr_time_window_sec)
        candidates = []
        for wf in self.ctx.snapshot_workflows():
            if wf.created_at is None:
                continue
            if abs((wf.created_at - reference_time).total_seconds()) <= window.total_seconds():
                candidates.append(wf)
        return candidates

    # ------------------------------------------------------------------
    # Semaphore capacity helper
    # ------------------------------------------------------------------

    def get_semaphore_capacity(self) -> Optional[int]:
        """
        Read the current semaphore limit for ingestor-omnipass from the
        semaphore-ingestors-uat ConfigMap.

        Returns the integer value, or None if not accessible.
        This represents the maximum concurrent omnipass workflows allowed.
        """
        try:
            from kubernetes import client as k8s_client, config as k8s_config
            try:
                k8s_config.load_incluster_config()
            except Exception:
                k8s_config.load_kube_config(context=self.ctx.k8s_context)

            v1 = k8s_client.CoreV1Api()
            cm_name = getattr(self.ctx, "semaphore_configmap_name", "semaphore-ingestors-uat")
            cm_key = getattr(self.ctx, "semaphore_configmap_key", "workflow")
            ns = self.ctx.argo_namespace

            cm = v1.read_namespaced_config_map(name=cm_name, namespace=ns)
            val = (cm.data or {}).get(cm_key)
            if val is not None:
                return int(val)
        except Exception as exc:
            logger.debug("Could not read semaphore ConfigMap: %s", exc)
        return None


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _phase_to_status(phase: str) -> str:
    mapping = {
        "Succeeded": "succeeded",
        "Failed": "failed",
        "Error": "failed",
        "Running": "in_progress",
        "Pending": "in_progress",
    }
    return mapping.get(phase, "in_progress")
