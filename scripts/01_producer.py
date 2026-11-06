"""Langkah 05 - Producer: mengirim event transaksi sintetik (reproducible) ke topik Kafka.

Contoh:
  python scripts/01_producer.py --rows 20000 --run-id smoke-a

Karakteristik data:
  * ~80% Denpasar, ~10% Jakarta, ~10% Surabaya  -> hot key (skew)
  * order_id = 1..rows; event_time di 2026-09-01 .. 2026-09-07
  * Baris tidak valid: order_id kelipatan 500 (kecuali kelipatan 200000);
    separuhnya city kosong, separuhnya amount null.
    -> 20.000 event: 40 invalid -> 19.960 bersih;  1.000.000 event: 1.995 invalid -> 998.005 bersih.
  * Sebagian kota sengaja ditulis kapital/berspasi untuk menguji standardisasi (trim + initcap).
"""
import argparse
import json
import os
import random
import sys
import time
from datetime import datetime, timedelta

BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "redpanda:9092")


def make_event(i, rng, run_id):
    r = rng.random()
    city = "Denpasar" if r < 0.80 else ("Jakarta" if r < 0.90 else "Surabaya")
    day = rng.randint(0, 6)
    sec = rng.randint(0, 86399)
    ts = datetime(2026, 9, 1) + timedelta(days=day, seconds=sec)
    product_id = rng.randint(1, 200)
    quantity = rng.randint(1, 5)
    amount = rng.randint(10, 500)

    # noise penulisan pada baris valid (diperbaiki oleh trim + initcap)
    variant = i % 3
    if variant == 1:
        city_out = city.upper()
    elif variant == 2:
        city_out = "  " + city.lower() + " "
    else:
        city_out = city

    # baris tidak valid untuk menguji data quality
    if i % 500 == 0 and i % 200000 != 0:
        if (i // 500) % 2 == 0:
            city_out = ""
        else:
            amount = None

    return {
        "run_id": run_id,
        "order_id": i,
        "event_time": ts.strftime("%Y-%m-%dT%H:%M:%S"),
        "city": city_out,
        "product_id": product_id,
        "quantity": quantity,
        "amount": amount,
    }


def connect(retries=20):
    from kafka import KafkaProducer
    from kafka.errors import NoBrokersAvailable

    for i in range(1, retries + 1):
        try:
            return KafkaProducer(
                bootstrap_servers=BOOTSTRAP,
                key_serializer=lambda x: x.encode("utf-8"),
                value_serializer=lambda obj: json.dumps(obj).encode("utf-8"),
                acks="all",
                retries=5,
                linger_ms=20,
                batch_size=65536,
            )
        except NoBrokersAvailable:
            print(f"Broker belum siap ({i}/{retries}), mencoba lagi ...")
            time.sleep(3)
    raise SystemExit("NoBrokersAvailable: cek 'docker compose logs --tail=80 redpanda'")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--rows", type=int, default=20000)
    ap.add_argument("--run-id", required=True)
    ap.add_argument("--topic", default="ecommerce.orders")
    ap.add_argument("--seed", type=int, default=42)
    args = ap.parse_args()

    rng = random.Random(args.seed)
    producer = connect()
    errors = []

    def on_error(exc):
        errors.append(exc)

    t0 = time.time()
    for i in range(1, args.rows + 1):
        event = make_event(i, rng, args.run_id)
        producer.send(args.topic, key=str(event["product_id"]), value=event).add_errback(on_error)
        if i % 100000 == 0:
            print(f"... terkirim {i:,} event")
    producer.flush()          # tunggu antrean produksi selesai
    dt = time.time() - t0
    producer.close()

    if errors:
        print(f"ERROR: {len(errors)} pesan gagal terkirim, contoh: {errors[0]}")
        sys.exit(1)
    print(f"Throughput: {args.rows / max(dt, 1e-9):,.0f} event/detik ({dt:.1f} detik)")
    print(f"PRODUCER_OK run_id={args.run_id} jumlah={args.rows}")


if __name__ == "__main__":
    main()
