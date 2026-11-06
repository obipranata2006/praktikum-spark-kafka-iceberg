"""Langkah 07 - Consumer at-least-once: Kafka -> JSON Lines -> bucket raw MinIO.

Urutan: baca -> unggah ke MinIO -> pastikan sukses -> commit offset.
Bila proses mati sesudah upload sebelum commit, batch terbaca ulang (at-least-once);
duplikat dibuang di Spark lewat dropDuplicates(run_id, order_id).

Contoh:
  python scripts/02_consumer_raw_minio.py --expected 20000 --run-id smoke-a
"""
import argparse
import json
import os
import sys
import time

import boto3
from botocore.client import Config
from kafka import KafkaConsumer
from kafka.errors import NoBrokersAvailable
from kafka.structs import OffsetAndMetadata

BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "redpanda:9092")
ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://minio:9000")
ACCESS = os.getenv("MINIO_ACCESS_KEY", "labadmin")
SECRET = os.getenv("MINIO_SECRET_KEY", "labpassword123")


def s3_client():
    return boto3.client(
        "s3", endpoint_url=ENDPOINT, aws_access_key_id=ACCESS, aws_secret_access_key=SECRET,
        region_name="us-east-1", config=Config(signature_version="s3v4", s3={"addressing_style": "path"}),
    )


def connect(topic, group, retries=20):
    for i in range(1, retries + 1):
        try:
            return KafkaConsumer(
                topic,
                bootstrap_servers=BOOTSTRAP,
                group_id=group,
                enable_auto_commit=False,        # commit manual SETELAH upload berhasil
                auto_offset_reset="earliest",    # group baru mulai dari pesan tertua
            )
        except NoBrokersAvailable:
            print(f"Broker belum siap ({i}/{retries}), mencoba lagi ...")
            time.sleep(3)
    raise SystemExit("NoBrokersAvailable: cek 'docker compose logs --tail=80 redpanda'")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--expected", type=int, required=True)
    ap.add_argument("--run-id", required=True)
    ap.add_argument("--topic", default="ecommerce.orders")
    ap.add_argument("--group-id", default=None, help="default: lab-raw-<run-id>")
    ap.add_argument("--max-records", type=int, default=10000)
    ap.add_argument("--idle-timeout", type=int, default=60,
                    help="detik tanpa pesan baru sebelum menyerah")
    args = ap.parse_args()

    group = args.group_id or f"lab-raw-{args.run_id}"
    consumer = connect(args.topic, group)
    s3 = s3_client()

    received, objects = 0, 0
    last_progress = time.time()
    print(f"Consumer group={group}, menunggu {args.expected:,} pesan run_id={args.run_id} ...")

    while received < args.expected:
        batches = consumer.poll(timeout_ms=2000, max_records=args.max_records)
        if not batches:
            if time.time() - last_progress > args.idle_timeout:
                break
            continue

        for tp, records in batches.items():
            lines, first, last = [], None, None
            for rec in records:
                text = rec.value.decode("utf-8")
                try:
                    obj = json.loads(text)
                except ValueError:
                    continue
                if obj.get("run_id") != args.run_id:     # abaikan pesan run lain
                    continue
                lines.append(text)
                first = rec.offset if first is None else first
                last = rec.offset

            if lines:
                key = (f"events/run_id={args.run_id}/partition={tp.partition}/"
                       f"offset-{first:012d}-{last:012d}.jsonl")
                body = "\n".join(lines) + "\n"
                s3.put_object(Bucket="raw", Key=key, Body=body.encode("utf-8"))   # upload dulu
                objects += 1
                received += len(lines)
                last_progress = time.time()
                print(f"MINIO_OK key={key} pesan={len(lines)} total={received:,}")

            # commit offset hanya setelah upload (atau tidak ada yang perlu diunggah)
            next_offset = records[-1].offset + 1
            consumer.commit(offsets={tp: OffsetAndMetadata(next_offset, "")})

    consumer.close()
    if received < args.expected:
        print(f"ERROR: timeout. diterima={received:,} < expected={args.expected:,}.")
        print("Periksa: --run-id/--expected sama dengan producer? Jika consumer sudah pernah selesai "
              "untuk run-id ini, offset group sudah ter-commit: pakai --group-id baru.")
        sys.exit(1)
    print(f"CONSUMER_OK jumlah_diterima={received} objek={objects}")


if __name__ == "__main__":
    main()
