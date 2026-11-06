# Laporan Milestone 2a — Analisis & Optimasi Eksekusi Apache Spark

**Mata kuliah:** Analitik dan Skalabilitas Data (MK35) — Sub-CPMK6.1.3
**Nama / NIM / Kelompok:** …
**Tanggal:** …  **Run-id yang dilaporkan:** …  (jumlah baris: …)

## 1. Lingkungan percobaan
| Item | Nilai |
|---|---|
| Perangkat / OS | … |
| CPU / RAM / RAM dialokasikan ke Docker | … |
| Spark / Iceberg / Redpanda / MinIO | 3.5.3 / 1.7.1 / v26.2.5 / RELEASE.2025-02-07 |
| Mode Spark | local[4] (bukan cluster multi-worker) |
| AQE / autoBroadcastJoinThreshold | false / -1 |
| shuffle.partitions / salt buckets / ulangan | 8 / 16 / 3 |

## 2. Arsitektur alur data
Jelaskan (diagram boleh): Producer → Redpanda (topik `ecommerce.orders`, 3 partisi) → Consumer at-least-once → MinIO `raw` → Spark (cleaning) → Iceberg `warehouse` (REST Catalog).

## 3. Ingestion (MG5)
- Bukti log: `PRODUCER_OK …`, `CONSUMER_OK …`  *(tempel)*
- Screenshot Redpanda Console (partisi, offset, payload) *(sisipkan)*
- Screenshot MinIO `raw/events/run_id=…/` *(sisipkan)*
- Jelaskan: mengapa offset di-commit **setelah** upload? Apa risikonya jika sebelum?

## 4. Data quality (MG6)
| raw_count | clean_count | rejected | city kosong | amount null | duplikat |
|---|---|---|---|---|---|
| … | … | … | … | … | … |

Jelaskan aturan cleaning (isi `quality.json`) dan mengapa angkanya demikian.

## 5. DAG, shuffle, dan Spark Web UI
- Screenshot Jobs, Stages, SQL *(sisipkan)*
- Percobaan A: tunjukkan operator **Exchange** & **HashAggregate** pada `plan_A_groupby_baseline.txt`. Apa penyebab shuffle? Bedakan narrow vs wide.

## 6. Data skew & key salting
| Skenario | Partisi terbesar | Terkecil | Non-kosong | Skew ratio |
|---|---|---|---|---|
| Sebelum (repartition city) | … | … | … | … |
| Sesudah (city + salt) | … | … | … | … |

Benchmark (median dari ≥3 ulangan):
| Percobaan | Median (s) | Min–Maks (s) |
|---|---|---|
| B skew sebelum | … | … |
| C setelah salting | … | … |

Perubahan waktu = (B − C) ÷ B × 100% = …  ; Speedup = B ÷ C = …
Analisis sebab-akibat: apakah partisi lebih seimbang? Apakah waktu ikut membaik? Jika tidak, jelaskan overhead (dua tahap groupBy, shuffle tambahan, data kecil, local[4]). Bukti: `VALIDASI_SALTING_OK`.

## 7. SortMergeJoin vs BroadcastHashJoin
| Percobaan | Operator di plan | Median (s) |
|---|---|---|
| D sort-merge | SortMergeJoin, Sort, Exchange | … |
| E broadcast | BroadcastHashJoin, BroadcastExchange | … |

Perubahan waktu & speedup: … Mengapa `groupBy(category)` tetap memicu shuffle? Kapan broadcast tidak layak (dimensi besar)? Bukti: `VALIDASI_JOIN_OK`.

## 8. Iceberg & partition pruning
- `ICEBERG_OK`, baris tabel = CLEAN_COUNT, jumlah snapshot *(tempel)*
- Screenshot MinIO bucket `warehouse` (data + metadata) *(sisipkan)*
- `plan_partition_pruning.txt`: tunjukkan predicate `sale_date`; file data relevan = … dari … (lihat `quality.json`).

## 9. Hasil audit
Tempel keluaran `05_audit_hasil.py` (harus berakhir `AUDIT_OK`).

## 10. Jawaban pertanyaan refleksi (Bagian J modul)
1. … 2. … (jawab semua dengan kalimat sendiri)

## 11. Keterbatasan & kesimpulan
- Ukuran data, spesifikasi laptop, mode `local[4]`, efek cache, jumlah ulangan.
- Batas kesimpulan: hasil benchmark lokal tidak otomatis berlaku di cluster.
- Kesimpulan akhir: …

## Lampiran
Tautan repositori Git, daftar berkas `hasil/<run-id>/`, screenshot tambahan.
