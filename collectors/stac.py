"""
collectors/stac.py
==================
STAC catalog collector.

Polls the STAC API to check whether products have been (re-)published.

Re-ingestion semantics
----------------------
Products tracked by this observer are already present in the STAC catalog before
re-ingestion starts.  Simply detecting that the item exists (HTTP 200) is not
sufficient — the item was there all along.

The correct signal is the ``properties.updated`` field that the STAC catalog sets
each time an item is ingested/updated.  The collector compares that field against
the omnipass workflow completion time (``workflow_finished_at``) to determine
whether the catalog has reflected the current ingestion cycle:

    properties.updated > workflow_finished_at
        → this ingestion cycle's publication is visible → record stac_seen_at
    properties.updated ≤ workflow_finished_at
        → catalog not yet updated → keep polling

``stac_seen_at`` is set to the value of ``properties.updated`` (from the STAC
item itself), NOT to ``datetime.now()``.  This gives an accurate measure of when
the catalog was actually updated, independent of polling latency.

API endpoint
------------
When a ``stac_token`` is configured the collector uses ``stac_public_endpoint``
(e.g. https://discover-uat.iride.earth) because that is the externally
reachable endpoint that accepts Bearer-token auth.
Without a token it falls back to ``stac_endpoint`` (internal cluster URL).

Item id / collection resolution
--------------------------------
Priority:
  1. StacRecord already present in ctx (populated by MinIO artifact collector
     from kafka-message.json — most reliable source)
  2. Template derivation: ``stac_item_id_from_product_id`` config key

PLUG-IN points
--------------
- Replace ``_fetch_item()`` if your STAC endpoint uses a different path layout.
- Override ``_derive_item_id()`` for custom id derivation.
"""

from __future__ import annotations

import logging
import time
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

import requests

from core.models import StacRecord
from core.run_context import RunContext

logger = logging.getLogger(__name__)


class StacCollector:
    """
    Polls the STAC API to detect when products are (re-)published.

    For each pending product the collector:
      1. Resolves the STAC item_id and collection (from StacRecord or template).
      2. Fetches the item.
      3. Reads ``properties.updated``.
      4. If ``properties.updated > workflow_finished_at`` → publication confirmed.
         Sets ``stac_seen_at = properties.updated`` (catalog's own timestamp).
    """

    def __init__(self, ctx: RunContext):
        self.ctx = ctx
        self._session = requests.Session()
        if ctx.stac_token:
            self._session.headers["Authorization"] = f"Bearer {ctx.stac_token}"
        self._session.verify = ctx.stac_verify_tls
        self._min_request_interval = 0.5
        self._last_request_time = 0.0

        # Determine polling endpoint:
        # - public endpoint (with token) when running outside the cluster
        # - internal endpoint otherwise
        if ctx.stac_token and ctx.stac_public_endpoint:
            self._poll_endpoint = ctx.stac_public_endpoint.rstrip("/")
        else:
            self._poll_endpoint = (ctx.stac_endpoint or "").rstrip("/")

    # ------------------------------------------------------------------
    # Main poll
    # ------------------------------------------------------------------

    def poll(self) -> List[StacRecord]:
        """
        Check STAC for all products not yet confirmed as (re-)published.

        Returns list of StacRecord objects where publication was newly confirmed.
        """
        if not self._poll_endpoint:
            logger.debug("STAC endpoint not configured; skipping STAC poll")
            return []

        pending_products = [
            p for p in self.ctx.snapshot_products()
            if p.stac_seen_at is None
            and p.final_status in ("succeeded", "in_progress")
        ]

        if not pending_products:
            return []

        newly_found: List[StacRecord] = []

        for product in pending_products:
            item_id, collection_id = self._resolve_stac_coords(product)
            if not item_id:
                continue

            try:
                item = self._fetch_item(collection_id, item_id)
                if not item:
                    continue

                published_at = self._check_publication(item, product)
                if published_at is None:
                    # properties.updated not yet past workflow_finished_at
                    continue

                stac_record = StacRecord(
                    run_id=self.ctx.run_id,
                    product_id=product.product_id,
                    stac_item_id=item_id,
                    collection_id=collection_id,
                    first_seen_at=published_at,
                    verification_status="found",
                    discovery_method="poll",
                    stac_datetime=item.get("properties", {}).get("datetime"),
                    stac_bbox=item.get("bbox"),
                    stac_updated=item.get("properties", {}).get("updated"),
                )
                self.ctx.add_or_update_stac(stac_record)

                with self.ctx._lock:
                    prod_record = self.ctx.products.get(product.product_id)
                    if prod_record and prod_record.stac_seen_at is None:
                        prod_record.stac_seen_at = published_at
                        self.ctx._products_dirty.append(product.product_id)

                newly_found.append(stac_record)
                logger.info(
                    "STAC published: product_id=%s item_id=%s updated=%s",
                    product.product_id,
                    item_id,
                    stac_record.stac_updated,
                )
            except Exception as exc:
                logger.warning(
                    "STAC lookup failed for product_id=%s: %s",
                    product.product_id,
                    exc,
                )

        if newly_found:
            logger.info("STAC poll: %d items confirmed as (re-)published", len(newly_found))

        return newly_found

    # ------------------------------------------------------------------
    # Publication check
    # ------------------------------------------------------------------

    def _check_publication(
        self, item: dict, product
    ) -> Optional[datetime]:
        """
        Return the publication datetime if the STAC item reflects the current
        ingestion cycle, otherwise return None (keep polling).

        Logic:
          - Parse ``properties.updated`` from the item.
          - Compare against the reference time: ``workflow_finished_at``
            (omnipass completion) or, as a fallback, ``ctx.started_at``
            (observer start time).
          - If ``properties.updated > reference_time`` → confirmed.
          - If ``properties.updated`` is absent but the product is newly
            succeeded (no prior catalog entry expected) → accept as confirmed
            using ``datetime.now(utc)`` so we don't block indefinitely.
        """
        props = item.get("properties") or {}
        updated_str: Optional[str] = props.get("updated")

        reference_time: datetime = (
            product.workflow_finished_at
            or self.ctx.started_at
        )

        if updated_str:
            published_at = _parse_dt(updated_str)
            if published_at is not None:
                if published_at > reference_time:
                    return published_at
                else:
                    logger.debug(
                        "STAC item %s: properties.updated=%s not yet past reference=%s",
                        item.get("id"), updated_str, reference_time.isoformat(),
                    )
                    return None
            # Unparseable updated field — fall through to None
            logger.debug(
                "STAC item %s: could not parse properties.updated=%r",
                item.get("id"), updated_str,
            )
            return None

        # No ``updated`` field at all (catalog doesn't populate it).
        # For new products (no prior entry) accept the item as published.
        # For re-ingested products we cannot confirm without updated → log warning.
        logger.warning(
            "STAC item %s has no properties.updated — cannot confirm re-ingestion. "
            "Accepting as published (may be inaccurate for re-ingested products).",
            item.get("id"),
        )
        return datetime.now(timezone.utc)

    # ------------------------------------------------------------------
    # STAC coordinate resolution
    # ------------------------------------------------------------------

    def _resolve_stac_coords(self, product) -> Tuple[Optional[str], str]:
        """
        Return (item_id, collection_id) for a product.

        Priority:
          1. StacRecord already in ctx (populated by MinIO artifact collector)
          2. Template derivation from product_id
        """
        stac_rec = self.ctx.stac_records.get(product.product_id)
        if stac_rec and stac_rec.stac_item_id:
            return stac_rec.stac_item_id, stac_rec.collection_id or self.ctx.stac_collection

        # Fallback: derive from configured template
        item_id = self.ctx.derive_stac_item_id(product.product_id)
        return item_id, self.ctx.stac_collection

    # ------------------------------------------------------------------
    # Item fetching
    # ------------------------------------------------------------------

    def _fetch_item(self, collection_id: str, item_id: str) -> Optional[dict]:
        """
        Fetch a single STAC item by collection and item id.

        Returns the item dict if found, None if not found (404), raises on
        other errors.

        PLUG-IN: Replace with your STAC endpoint's actual path structure.
        """
        self._rate_limit()

        url = f"{self._poll_endpoint}/collections/{collection_id}/items/{item_id}"
        try:
            resp = self._session.get(url, timeout=30)
            if resp.status_code == 404:
                return None
            resp.raise_for_status()
            return resp.json()
        except requests.exceptions.HTTPError as exc:
            if exc.response is not None and exc.response.status_code == 404:
                return None
            raise

    def batch_search(
        self,
        item_ids: List[str],
        max_per_request: int = 50,
    ) -> Dict[str, dict]:
        """
        Search for multiple STAC items in a single POST /search request.

        Returns dict: item_id -> item dict for found items.

        PLUG-IN: Adjust the request body schema if your STAC API differs.
        """
        if not self._poll_endpoint:
            return {}

        found: Dict[str, dict] = {}
        url = f"{self._poll_endpoint}/search"

        for i in range(0, len(item_ids), max_per_request):
            batch = item_ids[i : i + max_per_request]
            self._rate_limit()
            try:
                body = {
                    "ids": batch,
                    "collections": [self.ctx.stac_collection] if self.ctx.stac_collection else [],
                }
                resp = self._session.post(url, json=body, timeout=30)
                resp.raise_for_status()
                data = resp.json()
                for feature in data.get("features") or []:
                    fid = feature.get("id")
                    if fid:
                        found[fid] = feature
            except Exception as exc:
                logger.warning("STAC batch search failed: %s", exc)

        return found

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    def _rate_limit(self) -> None:
        """Enforce minimum interval between STAC requests."""
        now = time.monotonic()
        elapsed = now - self._last_request_time
        if elapsed < self._min_request_interval:
            time.sleep(self._min_request_interval - elapsed)
        self._last_request_time = time.monotonic()


def _parse_dt(val: str) -> Optional[datetime]:
    """Parse ISO-8601 datetime string to timezone-aware UTC datetime."""
    try:
        dt = datetime.fromisoformat(val)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt
    except (ValueError, TypeError):
        return None
