#!/usr/bin/env python3
"""
scripts/drop_product.py
=======================
Upload one or more product files to the MinIO drop-bucket to trigger the
ingestion-dispatcher workflow.

Usage — single file
-------------------
  python scripts/drop_product.py \\
      --config config/dryrun.config.yaml \\
      --file /path/to/IMH01_0_ST__OPT8_20251219T....zip

Usage — batch (directory of products)
--------------------------------------
  python scripts/drop_product.py \\
      --config config/uat.config.yaml \\
      --dir /path/to/products/ \\
      --interval 5          # seconds between drops (default: 0)
      --pattern "*.zip"     # glob filter (default: *.zip)

  # Dry-run first to check what would be dropped:
  python scripts/drop_product.py \\
      --config config/uat.config.yaml \\
      --dir /path/to/products/ \\
      --dry-run

Options
-------
  --prefix   Object key prefix inside the bucket (default: root)
  --dry-run  Print what would happen without uploading

The script prints the expected product_id and the MinIO object key so you
can cross-check with the observer log.
"""

from __future__ import annotations

import argparse
import logging
import re
import sys
import time
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


def _upload_one(client, ctx: "RunContext", local_path: Path, prefix: str, dry_run: bool) -> bool:
    """Upload a single file. Returns True on success."""
    from minio.error import S3Error

    filename   = local_path.name
    object_key = f"{prefix}/{filename}" if prefix else filename
    bucket     = ctx.minio_drop_bucket

    product_id   = _extract_product_id(filename, ctx.corr_product_id_s3_key_regex)
    stac_item_id = ctx.derive_stac_item_id(product_id) if product_id else "unknown"

    print()
    print(f"  {'[DRY RUN] ' if dry_run else ''}Uploading: {filename}")
    print(f"    S3 URL      : s3://{bucket}/{object_key}")
    print(f"    product_id  : {product_id or '(could not extract)'}")
    print(f"    STAC item   : {stac_item_id}")

    if dry_run:
        return True

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
        logger.info("  ✓ Uploaded: %s", filename)
        return True
    except S3Error as exc:
        logger.error("  ✗ Upload failed for %s: %s", filename, exc)
        return False


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Upload product file(s) to the MinIO drop-bucket"
    )
    parser.add_argument(
        "--config", required=True,
        help="Path to observer YAML config (e.g. config/dryrun.config.yaml)"
    )

    src = parser.add_mutually_exclusive_group(required=True)
    src.add_argument(
        "--file", metavar="FILE",
        help="Single product file to upload"
    )
    src.add_argument(
        "--dir", metavar="DIR",
        help="Directory of product files to upload (batch mode)"
    )

    parser.add_argument(
        "--pattern", default="*.zip",
        help="Glob pattern when using --dir (default: *.zip)"
    )
    parser.add_argument(
        "--interval", type=float, default=0.0, metavar="SEC",
        help="Seconds to wait between drops in batch mode (default: 0)"
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

    # Collect files to drop
    if args.file:
        files = [Path(args.file)]
        if not files[0].exists():
            logger.error("File not found: %s", files[0])
            sys.exit(1)
    else:
        dir_path = Path(args.dir)
        if not dir_path.is_dir():
            logger.error("Directory not found: %s", dir_path)
            sys.exit(1)
        files = sorted(dir_path.glob(args.pattern))
        if not files:
            logger.error("No files matching '%s' in %s", args.pattern, dir_path)
            sys.exit(1)

    prefix = args.prefix.strip("/")

    print()
    print("=" * 60)
    print(f"  Drop-product {'DRY RUN' if args.dry_run else 'UPLOAD'}")
    print("=" * 60)
    print(f"  Bucket      : {ctx.minio_drop_bucket}")
    print(f"  MinIO       : {'https' if ctx.minio_secure else 'http'}://{ctx.minio_endpoint}")
    print(f"  Files       : {len(files)}")
    if args.interval > 0:
        print(f"  Interval    : {args.interval}s between drops")
    print("=" * 60)

    if args.dry_run:
        for f in files:
            _upload_one(None, ctx, f, prefix, dry_run=True)
        print("\n[DRY RUN] No files uploaded. Remove --dry-run to upload.")
        return

    # Build MinIO client
    try:
        from minio import Minio
    except ImportError:
        logger.error("minio package not installed. Run: pip install minio")
        sys.exit(1)

    endpoint = ctx.minio_endpoint.replace("http://", "").replace("https://", "")
    client = Minio(
        endpoint=endpoint,
        access_key=ctx.minio_access_key or None,
        secret_key=ctx.minio_secret_key or None,
        secure=ctx.minio_secure,
    )

    # Upload
    ok = failed = 0
    for i, local_path in enumerate(files):
        success = _upload_one(client, ctx, local_path, prefix, dry_run=False)
        if success:
            ok += 1
        else:
            failed += 1
        if args.interval > 0 and i < len(files) - 1:
            logger.info("Waiting %.1fs before next drop...", args.interval)
            time.sleep(args.interval)

    print()
    print(f"Done: {ok} uploaded, {failed} failed.")
    if ok > 0:
        print()
        print("Observer will log product_id for each file and stop automatically")
        print("when all products reach a terminal state (STAC published or failed).")


if __name__ == "__main__":
    main()
