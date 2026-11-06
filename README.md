# Paket Praktikum MG5–MG7 (End-to-End)

Kafka/Redpanda → MinIO (raw) → Spark (cleaning, DAG, skew, join) → Apache Iceberg (curated).

## Jalan cepat
```bash
docker compose up -d --build
docker compose ps                                   # tunggu 10–30 detik setelah Up
docker compose exec tools python scripts/00_setup.py
docker compose exec redpanda rpk topic create ecommerce.orders -p 3 -r 1
docker compose exec tools python scripts/01_producer.py --rows 20000 --run-id smoke-a
docker compose exec tools python scripts/02_consumer_raw_minio.py --expected 20000 --run-id smoke-a
docker compose exec tools python scripts/03_inspect_raw.py --run-id smoke-a
docker compose exec spark bash scripts/run_spark.sh --run-id smoke-a --shuffle-partitions 8 --salt-buckets 16 --pause
docker compose exec tools python scripts/05_audit_hasil.py --run-id smoke-a
```

## Alamat
| Layanan | URL |
|---|---|
| Redpanda Console | http://localhost:8088 |
| MinIO Console | http://localhost:9001  (labadmin / labpassword123) |
| Spark Web UI | http://localhost:4040 (hanya saat Spark berjalan / [PAUSE]) |
| Iceberg REST Catalog | http://localhost:8181 |

Kredensial adalah dummy untuk lab lokal. Satu eksperimen = satu `--run-id` yang konsisten di producer, consumer, dan Spark.
