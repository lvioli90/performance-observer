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
                      ├─► STAC catalog
                      │
                      └─► deletion workflow
                              param: url = "s3://drop-bucket/path/to/product.zip"
                              │
                              │  deletes product from drop-bucket after successful ingestion
                              │  completion marks true T_final for end-to-end KPI
                              │
                              └─► (product removed from drop-bucket)

Shared identifier
-----------------
The s3 object key is the only identifier shared by all three workflows:
  dispatcher:  s3-key parameter          (relative: "path/to/product.zip")
  omnipass:    reference parameter        (absolute: "s3://drop-bucket/path/to/product.zip")
  deletion:    url parameter              (absolute: "s3://drop-bucket/path/to/product.zip")
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
_OMNIPASS   = "omnipass"
_DELETION   = "deletion"


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
        elif wf_type == _DELETION:
            product_id = self._correlate_deletion(wf)

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

        # When the omnipass brings a real product_id via its reference parameter,
        # a phantom dispatcher product may exist (created by Fallback 4 when the
        # drop-watcher was unavailable, and possibly already claimed by THIS omnipass
        # via Fallback 3 in a prior poll before the reference param was available).
        # Merge its dispatcher timing into the real product and remove the phantom.
        if wf_type == _OMNIPASS and wf.object_key:
            self._merge_phantom_dispatcher(product_id, omnipass_wf_name=wf.workflow_name)

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

    def _merge_phantom_dispatcher(
        self, real_product_id: str, omnipass_wf_name: Optional[str] = None
    ) -> None:
        """
        After an omnipass resolves a real product_id via the 'reference' parameter,
        check whether a phantom dispatcher product exists (product_id == dispatcher
        workflow name, created by Fallback 4 when the drop-watcher was unavailable).

        The phantom may have been previously claimed by THIS same omnipass via
        Fallback 3 (in an earlier poll before the reference param was available).
        In that case ``p.workflow_name`` is already set — we still merge it.

        If exactly one such phantom is found, transfer its dispatcher timing to
        the real product (if missing) and delete the phantom from tracking.
        """
        with self.ctx._lock:
            phantom_id = None
            for pid, p in list(self.ctx.products.items()):
                # A phantom created by Fallback 4 always has dispatcher_workflow_name == pid.
                # It either has no omnipass yet, OR was already claimed by THIS omnipass
                # via Fallback 3 before the reference param became available.
                already_claimed_by_this_omnipass = (
                    omnipass_wf_name and p.workflow_name == omnipass_wf_name
                )
                if (
                    pid != real_product_id
                    and p.dispatcher_workflow_name == pid   # Fallback-4 signature
                    and not p.object_key                    # no s3 key resolved
                    and (not p.workflow_name or already_claimed_by_this_omnipass)
                ):
                    if phantom_id is not None:
                        # Multiple phantoms — too ambiguous to merge safely
                        return
                    phantom_id = pid

            if phantom_id is None:
                return

            phantom = self.ctx.products[phantom_id]
            real = self.ctx.products[real_product_id]

            if real.dispatcher_workflow_name is None:
                real.dispatcher_workflow_name = phantom.dispatcher_workflow_name
                real.dispatcher_created_at = phantom.dispatcher_created_at
                real.dispatcher_started_at = phantom.dispatcher_started_at
                real.dispatcher_finished_at = phantom.dispatcher_finished_at
                real.dispatcher_status = phantom.dispatcher_status
            if real.ingest_reference_time is None and phantom.ingest_reference_time is not None:
                real.ingest_reference_time = phantom.ingest_reference_time

            del self.ctx.products[phantom_id]
            self.ctx._products_dirty.append(real_product_id)

        logger.info(
            "Phantom dispatcher product %s merged into %s and removed",
            phantom_id, real_product_id,
        )

    # ------------------------------------------------------------------
    # Workflow type detection
    # ------------------------------------------------------------------

    def _detect_workflow_type(self, wf: WorkflowRecord) -> Optional[str]:
        """
        Identify whether a workflow is a dispatcher, omnipass ingestor, or deletion.
        Checks template_name first, then workflow_name as fallback.
        """
        search_in = [
            (wf.template_name or "").lower(),
            wf.workflow_name.lower(),
        ]
        dispatcher_kw = self.ctx.corr_dispatcher_template.lower()
        omnipass_kw   = self.ctx.corr_omnipass_template.lower()
        deletion_kw   = self.ctx.corr_deletion_template.lower()

        for text in search_in:
            if dispatcher_kw and dispatcher_kw in text:
                return _DISPATCHER
            if omnipass_kw and omnipass_kw in text:
                return _OMNIPASS
            if deletion_kw and deletion_kw in text:
                return _DELETION
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

        Fallback chain (each step tried only if the previous returns nothing):
          1. Pod annotation params (s3-key / s3-bucket)
          2. Drop-watcher time-window match
          3. Nearest drop event regardless of time (works when observer started late)
          4. Workflow name itself as product_id (last resort, always succeeds)
        """
        # Already correlated on a previous poll — skip fallback chain so the
        # caller can still update dispatcher timing (started_at, finished_at).
        with self.ctx._lock:
            for prod in self.ctx.products.values():
                if prod.dispatcher_workflow_name == wf.workflow_name:
                    return prod.product_id

        s3_key_param = self.ctx.corr_dispatcher_s3_key_param
        s3_bucket_param = self.ctx.corr_dispatcher_s3_bucket_param

        s3_key = wf.parameters.get(s3_key_param, "")
        s3_bucket = wf.parameters.get(s3_bucket_param, "drop-bucket")

        if not s3_key:
            # Fallback 2: time-window match
            s3_key, s3_bucket_from_event = self._s3_key_from_drop_events(wf)
            if s3_key:
                s3_bucket = s3_bucket_from_event or s3_bucket
                logger.info(
                    "Dispatcher %s: '%s' not in params — from drop-watcher (window): %s",
                    wf.workflow_name, s3_key_param, s3_key,
                )
            else:
                # Fallback 3: nearest drop event, ignoring time window
                s3_key, s3_bucket_from_event = self._s3_key_nearest_drop_event(wf)
                if s3_key:
                    s3_bucket = s3_bucket_from_event or s3_bucket
                    logger.info(
                        "Dispatcher %s: no params, no window match — "
                        "using nearest drop event: %s",
                        wf.workflow_name, s3_key,
                    )

        if s3_key:
            wf.object_key = f"s3://{s3_bucket}/{s3_key}"
            product_id = self._extract_product_id_from_s3_key(s3_key)
            if product_id:
                logger.info(
                    "Dispatcher correlated: wf=%s s3_key=%s product_id=%s",
                    wf.workflow_name, s3_key, product_id,
                )
                return product_id

        # Fallback 4: use workflow name — always produces a stable identifier.
        # product_id will be updated later if the artifact provides a better name.
        product_id = wf.workflow_name
        logger.info(
            "Dispatcher %s: no s3-key available — using workflow name as product_id",
            wf.workflow_name,
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

        Fallback chain:
          1. Pod annotation params (reference)
          2. Dispatcher timing time-window match
          3. Any unclaimed dispatcher product (for dryrun with 1 product)
        """
        # Already correlated on a previous poll — skip fallback chain so the
        # caller can still update omnipass timing (started_at, finished_at, phase).
        with self.ctx._lock:
            for prod in self.ctx.products.values():
                if prod.workflow_name == wf.workflow_name:
                    return prod.product_id

        reference_param = self.ctx.corr_omnipass_reference_param
        reference = wf.parameters.get(reference_param, "")

        if not reference:
            # Fallback 2: time-window match
            product_id = self._product_id_from_dispatcher_timing(wf)
            if product_id:
                logger.info(
                    "Omnipass %s: no '%s' param — matched product_id=%s from dispatcher timing",
                    wf.workflow_name, reference_param, product_id,
                )
                return product_id

            # Fallback 3: any unclaimed dispatcher product (works for 1-product dryrun)
            product_id = self._product_id_any_unclaimed_dispatcher()
            if product_id:
                logger.info(
                    "Omnipass %s: no params, no timing match — "
                    "linked to sole unclaimed dispatcher product_id=%s",
                    wf.workflow_name, product_id,
                )
                return product_id

            logger.debug(
                "Omnipass %s: no '%s' parameter and no dispatcher product to match",
                wf.workflow_name, reference_param,
            )
            return None

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
    # Deletion workflow correlation
    # ------------------------------------------------------------------

    def _correlate_deletion(self, wf: WorkflowRecord) -> Optional[str]:
        """
        Extract product_id from the deletion workflow 'url' parameter.

          url = "s3://drop-bucket/path/to/S2A_MSIL2A_20230101.zip"
          object_key = url (stored as-is)
          s3_key extracted = "path/to/S2A_MSIL2A_20230101.zip"
          product_id = "S2A_MSIL2A_20230101"

        The deletion workflow runs after omnipass completes successfully and
        removes the product from the drop-bucket.  Its url parameter has the
        same format as the omnipass reference parameter.

        Fallback: match against an existing product whose object_key equals
        the url (already correlated by dispatcher/omnipass).
        """
        # Already correlated on a previous poll — just update timing.
        with self.ctx._lock:
            for prod in self.ctx.products.values():
                if prod.deletion_workflow_name == wf.workflow_name:
                    return prod.product_id

        url_param = self.ctx.corr_deletion_url_param
        url = wf.parameters.get(url_param, "")

        if not url:
            logger.debug(
                "Deletion %s: no '%s' parameter — cannot correlate",
                wf.workflow_name, url_param,
            )
            return None

        wf.object_key = url

        # Strip the "s3://bucket/" prefix to get the relative key
        s3_key = re.sub(r"^s3://[^/]+/", "", url)

        product_id = self._extract_product_id_from_s3_key(s3_key)

        if product_id:
            logger.info(
                "Deletion correlated: wf=%s url=%s product_id=%s",
                wf.workflow_name, url, product_id,
            )
            return product_id

        # Fallback: match by object_key on existing products
        with self.ctx._lock:
            for prod in self.ctx.products.values():
                if prod.object_key and prod.object_key == url:
                    logger.info(
                        "Deletion %s: matched product_id=%s via object_key",
                        wf.workflow_name, prod.product_id,
                    )
                    return prod.product_id

        return None

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
        Find a dispatcher-correlated ProductRecord whose dispatcher timing
        overlaps with this omnipass start within corr_time_window_sec.
        Used when omnipass pod annotations don't carry the 'reference' parameter.

        Anchor selection:
          - dispatcher_finished_at  if available (precise: end of dispatcher)
          - dispatcher_created_at   fallback (omnipass may start before dispatcher
            finishes — pipeline_gap can be negative when Kafka is sent mid-run)

        Delta sign:
          Both positive (omnipass after dispatcher) and negative (omnipass starts
          before dispatcher finishes) are valid.  We accept |delta| <= window.
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
            if prod.workflow_name:
                continue  # already has an omnipass
            # Use finished_at when available; fall back to created_at so we can
            # still match when the dispatcher is still running at poll time.
            anchor = prod.dispatcher_finished_at or prod.dispatcher_created_at
            if not anchor:
                continue
            delta = abs((wf.created_at - anchor).total_seconds())
            if delta <= window and (best_delta is None or delta < best_delta):
                best_delta = delta
                best_id = prod.product_id
        return best_id

    def _s3_key_nearest_drop_event(
        self, wf: WorkflowRecord
    ) -> tuple:
        """
        Return the closest drop-watcher event to wf.created_at with NO time constraint.
        Used as a last resort when the time-window fallback finds nothing — typically
        because the observer started after the product was already dropped.
        Only triggers when there is exactly 1 drop event (safe for dryrun).
        """
        with self.ctx._lock:
            events = dict(self.ctx.drop_bucket_events)
        if not events:
            return ("", "")
        if len(events) == 1:
            # Single-product dryrun: one event = one product, no ambiguity.
            s3_key = next(iter(events))
            return (s3_key, self.ctx.minio_drop_bucket)
        if wf.created_at is None:
            return ("", "")
        # Multiple events: return the closest one without any time constraint.
        best_key = min(events, key=lambda k: abs((wf.created_at - events[k]).total_seconds()))
        return (best_key, self.ctx.minio_drop_bucket)

    def _product_id_any_unclaimed_dispatcher(self) -> Optional[str]:
        """
        Return the product_id of any dispatcher-created ProductRecord that has
        not yet been claimed by an omnipass workflow.
        Safe for dryrun (1 product) where timing may prevent the window match.
        """
        with self.ctx._lock:
            products = list(self.ctx.products.values())
        unclaimed = [p for p in products if p.dispatcher_workflow_name and not p.workflow_name]
        if len(unclaimed) == 1:
            return unclaimed[0].product_id
        return None

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
            elif wf_type == _DELETION:
                self._update_from_deletion(record, wf)

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
        # Never clear a finish timestamp once observed: Argo can briefly oscillate
        # between Succeeded and Running between steps, causing wf.finished_at to
        # temporarily become None.
        if wf.finished_at and record.workflow_finished_at is None:
            record.workflow_finished_at = wf.finished_at
        # Never downgrade from succeeded: STAC confirmation or a prior Succeeded
        # phase must not be overwritten by a transient Running phase.
        if record.final_status != "succeeded":
            record.final_status = _phase_to_status(wf.phase)

        # If dispatcher didn't provide ingest_reference_time, fall back to
        # omnipass created_at (slightly less accurate but still useful).
        if record.ingest_reference_time is None and wf.created_at:
            record.ingest_reference_time = wf.created_at

    def _update_from_deletion(
        self, record: ProductRecord, wf: WorkflowRecord
    ) -> None:
        """
        Populate ProductRecord fields from the deletion workflow.

        The deletion workflow runs after omnipass succeeds and removes the
        product from the drop-bucket.  Its completion is the definitive
        T_final for the end-to-end KPI.
        """
        record.deletion_workflow_name = wf.workflow_name

        if record.deletion_created_at is None:
            record.deletion_created_at = wf.created_at
        record.deletion_started_at = wf.started_at
        # Never clear a finish timestamp once observed.
        if wf.finished_at and record.deletion_finished_at is None:
            record.deletion_finished_at = wf.finished_at
        record.deletion_status = wf.phase

        # Promote final_status to succeeded once deletion completes successfully,
        # unless it is already marked succeeded by a previous signal (STAC / omnipass).
        if wf.phase == "Succeeded":
            record.final_status = "succeeded"

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

        STAC publication is the authoritative completion signal: once the STAC
        API confirms the item is updated, the product is considered succeeded
        regardless of whether the Argo workflow phase has been polled yet.
        """
        with self.ctx._lock:
            product = self.ctx.products.get(product_id)
            if product and product.stac_seen_at is None:
                product.stac_seen_at = stac_seen_at
                product.final_status = "succeeded"
                self.ctx._products_dirty.append(product_id)

    # ------------------------------------------------------------------
    # Orphan drop registration
    # ------------------------------------------------------------------

    def register_orphan_drop(
        self,
        s3_key: str,
        t0: "datetime",
        s3_bucket: str = "",
    ) -> Optional[str]:
        """
        Create a ProductRecord for a drop-bucket object that was never picked
        up by a dispatcher workflow (orphan = trigger failure).

        Called by the drop-watcher loop after ``drop_orphan_sec`` expires.
        Sets ``final_status="failed"`` so the product is counted correctly by
        the KPI engine's failure metrics.

        Returns the product_id if a new record was created, None if the
        product is already tracked (dispatcher arrived late after all).
        """
        product_id = self._extract_product_id_from_s3_key(s3_key)
        if not product_id:
            # Fall back to filename without extension
            product_id = s3_key.rsplit("/", 1)[-1].rsplit(".", 1)[0] or s3_key

        bucket = s3_bucket or self.ctx.minio_drop_bucket or ""
        object_key = f"s3://{bucket}/{s3_key}" if bucket else s3_key

        with self.ctx._lock:
            # If a dispatcher arrived late and already created the record, skip.
            if product_id in self.ctx.products:
                return None
            record = ProductRecord(
                run_id=self.ctx.run_id,
                product_id=product_id,
                object_key=object_key,
                ingest_reference_time=t0,
                final_status="failed",
            )
            self.ctx.products[product_id] = record
            self.ctx._products_dirty.append(product_id)

        logger.warning(
            "Orphan drop registered as failed product: s3_key=%s product_id=%s "
            "(trigger failure — no dispatcher workflow observed within %ds)",
            s3_key, product_id, self.ctx.minio_drop_orphan_sec,
        )
        return product_id

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
