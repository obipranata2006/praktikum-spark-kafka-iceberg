"""Langkah 03 - Membuat bucket MinIO: raw (JSONL dari Kafka) dan warehouse (Iceberg)."""
import os
import time

import boto3
from botocore.client import Config
from botocore.exceptions import ClientError

ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://minio:9000")
ACCESS = os.getenv("MINIO_ACCESS_KEY", "labadmin")
SECRET = os.getenv("MINIO_SECRET_KEY", "labpassword123")


def make_client():
    # endpoint_url menunjuk ke MinIO di jaringan internal Docker (bukan ke AWS)
    return boto3.client(
        "s3",
        endpoint_url=ENDPOINT,
        aws_access_key_id=ACCESS,
        aws_secret_access_key=SECRET,
        region_name="us-east-1",
        config=Config(signature_version="s3v4", s3={"addressing_style": "path"}),
    )


def wait_ready(s3, retries=30):
    for i in range(1, retries + 1):
        try:
            s3.list_buckets()
            return
        except Exception as exc:  # MinIO belum siap
            print(f"Menunggu MinIO siap ({i}/{retries}): {type(exc).__name__}")
            time.sleep(2)
    raise SystemExit("MinIO tidak siap. Cek: docker compose logs --tail=80 minio")


def main():
    s3 = make_client()
    wait_ready(s3)
    for bucket in ("raw", "warehouse"):
        try:
            s3.head_bucket(Bucket=bucket)
            print(f"Bucket sudah ada: {bucket}")
        except ClientError:
            s3.create_bucket(Bucket=bucket)
            print(f"Bucket dibuat: {bucket}")


if __name__ == "__main__":
    main()
