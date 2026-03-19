"""
core/correlator.py
==================
Isolates all correlation logic between:
  MinIO object key  ->  Argo workflow  ->  pod / step  ->  STAC item

Correlation strategy (in priority order)
-----------------------------------------
1. LABEL-BASED (primary, most reliable)
   The Argo workflow carries a label or parameter whose value is the MinIO
   object key.  The label name is configured via
   ``correlation.object_key_label``.  From the object key the product_id is
   extracted with ``correlation.product_id_from_object_key`` regex.

2. WORKFLOW-NAME-BASED (secondary)
   If ``correlation.workflow_name_contains_product`` is true, the product_id
   is extracted from the workflow name using
   ``correlation.product_id_from_workflow_name_regex``.

3. TIME-WINDOW FALLBACK (last resort)
   If neither label nor name correlation is possible, the correlator can
   match a workflow to a product by looking at creation timestamps within
   a configurable window (``correlation.time_window_fallback_sec``).
   NOTE: this fallback is unreliable when multiple products are injected
   simultaneously; it is provided only to allow partial data collection.

STAC correlation
-----------------
The STAC item id is derived from product_id using the template
``correlation.stac_item_id_from_product_id``.  If direct lookup fails,
the correlator searches the STAC collection by datetime range.

All assumptions are documented in the config schema (example.config.yaml).
"""

from __future__ import annotations

import logging
from datetime import datetime, timedelta, timezone
from typing import Dict, List, Optional

from core.models import ProductRecord, WorkflowRecord
from core.run_context import RunContext

logger = logging.getLogger(__name__)


class Correlator:
    """
    Maintains and updates the product->workflow->pod->STAC correlation map.

    Call ``correlate_workflow(record)`` every time a new or updated
    WorkflowRecord is available.  The correlator will attempt to link it to an
    existing ProductRecord or create a new one.
    """

    def __init__(self, ctx: RunContext):
        self.ctx = ctx

    # ------------------------------------------------------------------
    # Main entry point
    # ------------------------------------------------------------------

    def correlate_workflow(self, wf: WorkflowRecord) -> Optional[str]:
        """
        Try to determine the product_id for a workflow.

        Returns the product_id if found, None otherwise.
        Updates the in-memory ProductRecord (creating it if needed).
        """
        product_id = self._try_label_correlation(wf)

        if product_id is None and self.ctx.corr_wf_name_contains_product:
            product_id = self._try_name_correlation(wf)

        if product_id is None:
            # Time-window fallback is logged but not used for ProductRecord
            # creation because it cannot be made reliable without knowing
            # the injection timestamps.  It is available for manual analysis.
            logger.debug(
                "No direct correlation for workflow %s; time-window fallback available",
                wf.workflow_name,
            )

        if product_id:
            self._upsert_product(product_id, wf)
            # Update the workflow record with the resolved product_id
            wf.product_id = product_id

        return product_id

    # ------------------------------------------------------------------
    # Correlation strategies
    # ------------------------------------------------------------------

    def _try_label_correlation(self, wf: WorkflowRecord) -> Optional[str]:
        """
        Strategy 1: extract object_key from a workflow label/parameter,
        then derive product_id from the object_key via regex.
        """
        label_name = self.ctx.corr_object_key_label
        if not label_name:
            return None

        # Check labels first, then parameters
        object_key = wf.labels.get(label_name) or wf.parameters.get(label_name)
        if not object_key:
            return None

        product_id = self.ctx.extract_product_id_from_object_key(object_key)
        if product_id:
            wf.object_key = object_key
            logger.debug(
                "Label correlation: workflow=%s object_key=%s product_id=%s",
                wf.workflow_name,
                object_key,
                product_id,
            )
        return product_id

    def _try_name_correlation(self, wf: WorkflowRecord) -> Optional[str]:
        """
        Strategy 2: extract product_id from the workflow name via regex.
        """
        product_id = self.ctx.extract_product_id_from_workflow_name(wf.workflow_name)
        if product_id:
            logger.debug(
                "Name correlation: workflow=%s product_id=%s",
                wf.workflow_name,
                product_id,
            )
        return product_id

    def time_window_candidates(
        self,
        reference_time: datetime,
        window_sec: Optional[int] = None,
    ) -> List[WorkflowRecord]:
        """
        Strategy 3 (fallback): return workflows whose created_at falls within
        ``window_sec`` seconds of ``reference_time``.

        This is returned for informational purposes only; the caller decides
        what to do with the candidates.
        """
        window = timedelta(seconds=window_sec or self.ctx.corr_time_window_sec)
        candidates = []
        for wf in self.ctx.snapshot_workflows():
            if wf.created_at is None:
                continue
            delta = abs((wf.created_at - reference_time).total_seconds())
            if delta <= window.total_seconds():
                candidates.append(wf)
        return candidates

    # ------------------------------------------------------------------
    # ProductRecord upsert
    # ------------------------------------------------------------------

    def _upsert_product(self, product_id: str, wf: WorkflowRecord) -> ProductRecord:
        """
        Create or update a ProductRecord from a correlated WorkflowRecord.
        """
        with self.ctx._lock:
            existing = self.ctx.products.get(product_id)
            if existing is None:
                record = ProductRecord(
                    run_id=self.ctx.run_id,
                    product_id=product_id,
                    object_key=wf.object_key,
                    workflow_name=wf.workflow_name,
                    workflow_created_at=wf.created_at,
                    workflow_started_at=wf.started_at,
                    workflow_finished_at=wf.finished_at,
                    # Use workflow created_at as the ingest reference approximation
                    ingest_reference_time=wf.created_at,
                    final_status=_phase_to_status(wf.phase),
                )
                self.ctx.products[product_id] = record
                self.ctx._products_dirty.append(product_id)
                logger.debug("Created ProductRecord for product_id=%s", product_id)
            else:
                # Update fields that may have changed
                if wf.object_key and not existing.object_key:
                    existing.object_key = wf.object_key
                existing.workflow_name = wf.workflow_name
                existing.workflow_created_at = wf.created_at
                existing.workflow_started_at = wf.started_at
                existing.workflow_finished_at = wf.finished_at
                if existing.ingest_reference_time is None:
                    existing.ingest_reference_time = wf.created_at
                existing.final_status = _phase_to_status(wf.phase)
                self.ctx._products_dirty.append(product_id)
                return existing

        return self.ctx.products[product_id]

    # ------------------------------------------------------------------
    # STAC correlation
    # ------------------------------------------------------------------

    def derive_stac_item_id(self, product_id: str) -> str:
        """Return the expected STAC item id for a given product_id."""
        return self.ctx.derive_stac_item_id(product_id)

    def update_stac_visibility(
        self, product_id: str, stac_seen_at: datetime
    ) -> None:
        """
        Mark a product as seen in STAC and update the ProductRecord.
        """
        with self.ctx._lock:
            product = self.ctx.products.get(product_id)
            if product is not None:
                if product.stac_seen_at is None:
                    product.stac_seen_at = stac_seen_at
                    self.ctx._products_dirty.append(product_id)
                    logger.debug(
                        "STAC visibility updated: product_id=%s seen_at=%s",
                        product_id,
                        stac_seen_at.isoformat(),
                    )


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _phase_to_status(phase: str) -> str:
    """Map an Argo workflow phase to a product final_status string."""
    mapping = {
        "Succeeded": "succeeded",
        "Failed": "failed",
        "Error": "failed",
        "Running": "in_progress",
        "Pending": "in_progress",
    }
    return mapping.get(phase, "in_progress")
