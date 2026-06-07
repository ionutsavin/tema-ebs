import csv
import os
import statistics

EXPERIMENT_DIR = os.path.dirname(os.path.abspath(__file__))
TOTAL_PUBLICATIONS = 90000
SUBSCRIBERS = ["client_1", "client_2", "client_3"]


def load_matches(subscriber_id):
    path = os.path.join(EXPERIMENT_DIR, f"{subscriber_id}_matches.csv")
    if not os.path.exists(path):
        return []
    with open(path) as f:
        return list(csv.DictReader(f))


def main():
    all_ts = set()
    all_latencies = []
    per_sub = {}

    for sid in SUBSCRIBERS:
        rows = load_matches(sid)
        per_sub[sid] = len(rows)
        for r in rows:
            all_ts.add(int(r["ts"]))
            all_latencies.append(int(r["latency_ms"]))

    total_unique = len(all_ts)
    total_matches = sum(per_sub.values())

    print(f"\n{'=' * 50}")
    print(f"  Rezultate experiment")
    print(f"{'=' * 50}")
    print(f"\n  a) Publicații livrate:")
    for sid, count in per_sub.items():
        print(f"     {sid}: {count}")
    print(f"     Total match-uri primite: {total_matches}")
    print(f"     Publicații unice livrate: {total_unique} / {TOTAL_PUBLICATIONS}")
    print(f"     Rată de livrare: {total_unique / TOTAL_PUBLICATIONS * 100:.2f}%")

    if all_latencies:
        all_latencies.sort()
        mean = statistics.mean(all_latencies)
        p50 = all_latencies[len(all_latencies) // 2]
        p95 = all_latencies[int(len(all_latencies) * 0.95)]
        p99 = all_latencies[int(len(all_latencies) * 0.99)]
        print(f"\n  b) Latență end-to-end (ms):")
        print(f"     Medie: {mean:.2f}")
        print(f"     Minim: {min(all_latencies)}")
        print(f"     Maxim: {max(all_latencies)}")
        print(f"     P50:   {p50}")
        print(f"     P95:   {p95}")
        print(f"     P99:   {p99}")

    print(f"\n  c) Rată de matching:")
    print(f"     {total_unique} / {TOTAL_PUBLICATIONS} = {total_unique / TOTAL_PUBLICATIONS * 100:.2f}%")
    print(f"{'=' * 50}\n")


if __name__ == "__main__":
    main()
