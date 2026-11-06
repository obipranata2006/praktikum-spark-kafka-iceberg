"""Langkah 08 - Validasi landing raw di MinIO.

Contoh:
  python scripts/03_inspect_raw.py --run-id smoke-a [--count-lines]
"""
import argparse
import os
import re
import sys
from collections import Counter

import boto3
from botocore.client import Config

ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://minio:9000")
ACCESS = os.getenv("MINIO_ACCESS_KEY", "labadmin")
SECRET = os.getenv("MINIO_SECRET_KEY", "labpassword123")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--run-id", required=True)
    ap.add_argument("--count-lines", action="store_true", help="hitung total baris (lebih lambat)")
    args = ap.parse_args()

    s3 = boto3.client(
        "s3", endpoint_url=ENDPOINT, aws_access_key_id=ACCESS, aws_secret_access_key=SECRET,
        region_name="us-east-1", config=Config(signature_version="s3v4", s3={"addressing_style": "path"}),
    )
    prefix = f"events/run_id={args.run_id}/"
    keys, total_bytes = [], 0
    for page in s3.get_paginator("list_objects_v2").paginate(Bucket="raw", Prefix=prefix):
        for obj in page.get("Contents", []):
            keys.append(obj["Key"])
            total_bytes += obj["Size"]

    print(f"OBJECT_COUNT={len(keys)}")
    print(f"TOTAL_BYTES={total_bytes}")
    if not keys:
        print("Tidak ada objek. Jalankan producer + consumer dengan run-id yang sama.")
        sys.exit(1)

    per_part = Counter(re.search(r"partition=(\d+)", k).group(1) for k in keys)
    for p in sorted(per_part):
        print(f"  partition={p}: {per_part[p]} objek")

    print(f"\nContoh isi {keys[0]}:")
    body = s3.get_object(Bucket="raw", Key=keys[0])["Body"].read().decode("utf-8")
    for line in body.splitlines()[:3]:
        print("  ", line)

    if args.count_lines:
        n = 0
        for k in keys:
            n += s3.get_object(Bucket="raw", Key=k)["Body"].read().count(b"\n")
        print(f"LINE_COUNT={n}")


if __name__ == "__main__":
    main()
