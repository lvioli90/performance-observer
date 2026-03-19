#!/usr/bin/env python3
"""
scripts/drop_product.py
=======================
Upload a single product file to the MinIO drop-bucket to trigger the
ingestion-dispatcher workflow.

Use this for dry-run validation before a full load test.

Usage
-----
  python scripts/drop_product.py \\
      --config config/dryrun.config.yaml \\
      --file /path/to/IMH01_0_ST__OPT8_20251219T....zip

  # Upload to a specific prefix inside the bucket
  python scripts/drop_product.py \\
      --config config/dryrun.config.yaml \\
      --file /path/to/product.zip \\
      --prefix some/subdir/

  # Dry-run: print what would happen without actually uploading
  python scripts/drop_product.py \\
      --config config/dryrun.config.yaml \\
      --file /path/to/product.zip \\
      --dry-run

The script prints the expected product_id and the MinIO object key so you
can cross-check with the observer log.
"""

from __future__ import annotations

import argparse
import logging
import os
import re
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from core.run_context import RunContext, load_config

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)-8s %(message)s",
    stream=sys.stdout,
)
logger = logging.getLogger("drop_product")


def _extract_product_id(filename: str, regex: str) -> str | None:
    """Apply the configured regex to the filename to extract product_id."""
    try:
        m = re.search(regex, filename)
        if m:
            return m.group("product_id")
    except (re.error, IndexError):
        pass
    return None


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Upload a product file to the MinIO drop-bucket"
    )
    parser.add_argument(
        "--config", required=True,
        help="Path to observer YAML config (e.g. config/dryrun.config.yaml)"
    )
    parser.add_argument(
        "--file", required=True,
        help="Local path to the product file to upload"
    )
    parser.add_argument(
        "--prefix", default="",
        help="Optional object key prefix inside the drop-bucket (default: root)"
    )
    parser.add_argument(
        "--dry-run", action="store_true",
        help="Print what would happen without actually uploading"
    )
    args = parser.parse_args()

    # Load config
    cfg = load_config(args.config)
    ctx = RunContext(cfg)

    local_path = Path(args.file)
    if not local_path.exists():
        logger.error("File not found: %s", local_path)
        sys.exit(1)

    filename = local_path.name
    prefix = args.prefix.strip("/")
    object_key = f"{prefix}/{filename}" if prefix else filename
    bucket = ctx.minio_drop_bucket

    # Derive expected product_id
    product_id = _extract_product_id(filename, ctx.corr_product_id_s3_key_regex)
    stac_item_id = ctx.derive_stac_item_id(product_id) if product_id else "unknown"

    print()
    print("=" * 60)
    print("  Drop-product dry run" if args.dry_run else "  Drop-product upload")
    print("=" * 60)
    print(f"  Local file   : {local_path} ({local_path.stat().st_size / 1e6:.1f} MB)")
    print(f"  Bucket       : {bucket}")
    print(f"  Object key   : {object_key}")
    print(f"  S3 URL       : s3://{bucket}/{object_key}")
    print(f"  product_id   : {product_id or '(could not extract)'}")
    print(f"  STAC item id : {stac_item_id}")
    print(f"  MinIO endpt  : {'https' if ctx.minio_secure else 'http'}://{ctx.minio_endpoint}")
    print("=" * 60)

    if args.dry_run:
        print("\n[DRY RUN] No file uploaded. Remove --dry-run to upload.")
        return

    # Upload
    try:
        from minio import Minio
        from minio.error import S3Error
    except ImportError:
        logger.error("minio package not installed. Run: pip install minio")
        sys.exit(1)

    endpoint = (
        ctx.minio_endpoint
        .replace("http://", "")
        .replace("https://", "")
    )
    client = Minio(
        endpoint=endpoint,
        access_key=ctx.minio_access_key or None,
        secret_key=ctx.minio_secret_key or None,
        secure=ctx.minio_secure,
    )

    file_size = local_path.stat().st_size
    logger.info("Uploading %s → s3://%s/%s (%d bytes)...", filename, bucket, object_key, file_size)

    try:
        with open(local_path, "rb") as fh:
            client.put_object(
                bucket_name=bucket,
                object_name=object_key,
                data=fh,
                length=file_size,
            )
    except S3Error as exc:
        logger.error("Upload failed: %s", exc)
        sys.exit(1)

    logger.info("Upload complete.")
    print()
    print("Next steps:")
    print(f"  Watch the observer log for: product_id={product_id}")
    print(f"  Expected dispatcher param:  s3-key={object_key}")
    print(f"  Expected omnipass param:    reference=s3://{bucket}/{object_key}")
    print()
    print("The observer will confirm STAC publication when it logs:")
    print(f"  STAC published: product_id={product_id} ... updated=<timestamp>")
    print()


if __name__ == "__main__":
    main()
