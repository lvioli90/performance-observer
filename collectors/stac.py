"""
collectors/stac.py
==================
STAC catalog collector.

Polls a STAC API (OGC API - Features / STAC API spec) to check whether
expected products have been published.

API assumptions
---------------
Standard STAC API item search:
  GET /collections/{collection_id}/items?ids={item_id}
  or
  POST /search with {"ids": [...]}

Item response (abbreviated):
  {
    "type": "Feature",
    "stac_version": "1.0.0",
    "id": "S2A_MSIL2A_20230101T100000_...",
    "bbox": [-10, 40, 10, 60],
    "geometry": {...},
    "properties": {
      "datetime": "2023-01-01T10:00:00Z",
      ...
    },
    "links": [...]
  }

Strategy
--------
1. For each product_id in the RunContext that does not yet have a stac_seen_at,
   derive the expected STAC item id using the configured template.
2. Query GET /collections/{collection}/items/{item_id}.
3. If found, record the first_seen_at timestamp and update the ProductRecord.
4. As a fallback, if the direct lookup fails and there are many unresolved
   products, do a batch search by time window (configurable).

Plug-in points
--------------
- Replace the ``_fetch_item()`` implementation if your STAC endpoint requires
  different auth or uses a non-standard path.
- Override ``_derive_item_id()`` for custom id derivation logic.
"""

from __future__ import annotations

import logging
import time
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

import requests

from core.models import StacRecord
from core.run_context import RunContext

logger = logging.getLogger(__name__)


class StacCollector:
    """
    Polls the STAC API to detect when products become visible.

    Maintains a set of product_ids that are still pending STAC visibility
    so each poll only queries for unresolved products.
    """

    def __init__(self, ctx: RunContext):
        self.ctx = ctx
        self._session = requests.Session()
        if ctx.stac_token:
            self._session.headers["Authorization"] = f"Bearer {ctx.stac_token}"
        self._session.verify = ctx.stac_verify_tls
        # Respect rate limits: minimum seconds between requests
        self._min_request_interval = 0.5
        self._last_request_time = 0.0

    # ------------------------------------------------------------------
    # Main poll
    # ------------------------------------------------------------------

    def poll(self) -> List[StacRecord]:
        """
        Check STAC visibility for all products not yet seen.

        Returns list of StacRecord objects where items were newly found.
        """
        if not self.ctx.stac_endpoint:
            logger.debug("STAC endpoint not configured; skipping STAC poll")
            return []

        # Find product_ids still pending STAC visibility
        pending_products = [
            p for p in self.ctx.snapshot_products()
            if p.stac_seen_at is None
            and p.final_status in ("succeeded", "in_progress")
        ]

        if not pending_products:
            return []

        newly_found: List[StacRecord] = []
        seen_at = datetime.now(timezone.utc)

        for product in pending_products:
            item_id = self.ctx.derive_stac_item_id(product.product_id)
            try:
                item = self._fetch_item(self.ctx.stac_collection, item_id)
                if item:
                    stac_record = StacRecord(
                        run_id=self.ctx.run_id,
                        product_id=product.product_id,
                        stac_item_id=item_id,
                        collection_id=self.ctx.stac_collection,
                        first_seen_at=seen_at,
                        verification_status="found",
                        stac_datetime=item.get("properties", {}).get("datetime"),
                        stac_bbox=item.get("bbox"),
                    )
                    self.ctx.add_or_update_stac(stac_record)
                    # Update the ProductRecord
                    with self.ctx._lock:
                        prod_record = self.ctx.products.get(product.product_id)
                        if prod_record and prod_record.stac_seen_at is None:
                            prod_record.stac_seen_at = seen_at
                            self.ctx._products_dirty.append(product.product_id)

                    newly_found.append(stac_record)
                    logger.debug(
                        "STAC item found: product_id=%s item_id=%s",
                        product.product_id,
                        item_id,
                    )
            except Exception as exc:
                logger.warning(
                    "STAC lookup failed for product_id=%s: %s",
                    product.product_id,
                    exc,
                )

        if newly_found:
            logger.info("STAC poll: %d new items found", len(newly_found))

        return newly_found

    # ------------------------------------------------------------------
    # Item fetching
    # ------------------------------------------------------------------

    def _fetch_item(self, collection_id: str, item_id: str) -> Optional[dict]:
        """
        Fetch a single STAC item by collection and item id.

        Returns the item dict if found, None if not found (404), raises on
        other errors.

        PLUG-IN: Replace with your STAC endpoint's actual path structure.
        Some STAC APIs use /search instead of /collections/{id}/items/{id}.
        """
        self._rate_limit()

        url = (
            f"{self.ctx.stac_endpoint.rstrip('/')}"
            f"/collections/{collection_id}/items/{item_id}"
        )
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
        This implements the STAC API /search endpoint.
        """
        if not self.ctx.stac_endpoint:
            return {}

        found: Dict[str, dict] = {}
        url = f"{self.ctx.stac_endpoint.rstrip('/')}/search"

        # Process in batches to avoid query string length limits
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
