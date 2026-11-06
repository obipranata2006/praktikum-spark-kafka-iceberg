"""Langkah 19 - Audit otomatis artefak hasil (tidak menjalankan Spark).

Contoh:  python scripts/05_audit_hasil.py --run-id smoke-a
"""
import argparse
import csv
import json
import os
import statistics
import sys

ap = argparse.ArgumentParser()
ap.add_argument("--run-id", required=True)
ap.add_argument("--dir", default="hasil")
args = ap.parse_args()
D = os.path.join(args.dir, args.run_id)
failed = []


def fail(msg):
    print("FAIL:", msg)
    failed.append(msg)


def read(name):
    path = os.path.join(D, name)
    if not os.path.exists(path):
        fail(f"berkas tidak ditemukan: {path}")
        return None
    with open(path, encoding="utf-8") as fh:
        return fh.read()


def median_s(rows, exp):
    v = [float(r["seconds"]) for r in rows if r["experiment"] == exp]
    return statistics.median(v) if v else None


def report_change(label, base, opt):
    if not base or not opt:
        fail(f"{label}: data waktu tidak lengkap")
        return
    pct = (base - opt) / base * 100
    sp = base / opt
    note = "  <-- LEBIH LAMBAT (speedup < 1)" if sp < 1 else ""
    print(f"{label} baseline={base:.3f}s optimasi={opt:.3f}s perubahan={pct:+.1f}% speedup={sp:.2f}x{note}")


# 1. quality.json
txt = read("quality.json")
if txt:
    q = json.loads(txt)
    ok = (q["raw_count"] - q["rejected_count"] == q["clean_count"]
          and 0 < q["clean_count"] <= q["raw_count"]
          and q.get("validations", {}).get("salting_ok") is True
          and q.get("validations", {}).get("join_ok") is True
          and q.get("validations", {}).get("iceberg_ok") is True)
    if ok:
        print(f"QUALITY_OK raw={q['raw_count']} clean={q['clean_count']} rejected={q['rejected_count']}")
    else:
        fail("quality.json tidak konsisten atau ada validasi yang belum lolos")
    clean = q["clean_count"]
else:
    clean = None

# 2. benchmarks.csv
rows = []
txt = read("benchmarks.csv")
if txt:
    rows = list(csv.DictReader(txt.splitlines()))
    need = ["A_groupby_baseline", "B_skew_before", "C_skew_after_salting", "D_sort_merge_join", "E_broadcast_join"]
    missing = [e for e in need if median_s(rows, e) is None]
    if missing or any(float(r["seconds"]) <= 0 for r in rows):
        fail(f"benchmarks.csv tidak lengkap/valid. Hilang: {missing}")
    else:
        reps = len([r for r in rows if r["experiment"] == "A_groupby_baseline"])
        print(f"BENCHMARKS_OK percobaan={len(need)} ulangan_per_percobaan={reps}")

# 3. partition_sizes.csv
txt = read("partition_sizes.csv")
if txt:
    ps = list(csv.DictReader(txt.splitlines()))
    stats = {}
    for scen, tag in (("skew_before", "PARTITIONS_SKEW"), ("salted_after", "PARTITIONS_SALTED")):
        sizes = [int(r["rows"]) for r in ps if r["scenario"] == scen]
        if not sizes:
            fail(f"skenario {scen} kosong di partition_sizes.csv")
            continue
        total = sum(sizes)
        ratio = max(sizes) / (total / len(sizes)) if total else 0
        stats[scen] = ratio
        print(f"{tag} partisi={len(sizes)} non_kosong={sum(1 for s in sizes if s)} "
              f"terbesar={max(sizes)} terkecil={min(sizes)} skew_ratio={ratio:.2f}")
        if clean is not None and total != clean:
            fail(f"{scen}: jumlah baris {total} != clean_count {clean}")
    if len(stats) == 2:
        print("   keseimbangan partisi membaik setelah salting:", stats["salted_after"] < stats["skew_before"])

# 4. plan join
pd, pe = read("plan_D_sort_merge_join.txt"), read("plan_E_broadcast_join.txt")
if pd is not None and pe is not None:
    if "SortMergeJoin" in pd and "BroadcastHashJoin" in pe:
        print("JOIN_PLAN_OK  D=SortMergeJoin, E=BroadcastHashJoin")
    else:
        fail("plan join tidak memuat operator yang diharapkan "
             f"(D SortMergeJoin={'SortMergeJoin' in pd}, E BroadcastHashJoin={'BroadcastHashJoin' in pe})")

# 5. speedup
if rows:
    report_change("JOIN_SPEEDUP ", median_s(rows, "D_sort_merge_join"), median_s(rows, "E_broadcast_join"))
    report_change("SKEW_SPEEDUP ", median_s(rows, "B_skew_before"), median_s(rows, "C_skew_after_salting"))

# 6. plan lain
for name in ("plan_A_groupby_baseline.txt", "plan_B_skew_before.txt", "plan_C_skew_after_salting.txt"):
    t = read(name)
    if t is not None:
        print(f"{name}: Exchange={'Exchange' in t}")
pp = read("plan_partition_pruning.txt")
if pp is not None:
    print("PRUNING predicate sale_date pada plan:", "sale_date" in pp)

print()
if failed:
    print(f"AUDIT_GAGAL ({len(failed)} masalah)")
    sys.exit(1)
print("AUDIT_OK")
