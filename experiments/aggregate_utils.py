import json
import os
import re
import statistics

MATCH_RE = re.compile(r"match_received ts=(\d+) latency_ms=(\d+) broker_id=(\S+)")
VERIFY_SUMMARY_RE = re.compile(
    r"verify_summary verified=(\d+) failed=(\d+) total=(\d+) rate=([\d.]+)"
)

SUBSCRIBERS = ["client_1", "client_2", "client_3"]
TOTAL_PUBLICATIONS = 90000
IN_WINDOW_LATENCY_MS = 10_000


def load_matches(log_path: str) -> list[dict]:
    if not os.path.exists(log_path):
        return []

    rows = []
    with open(log_path) as f:
        for line in f:
            match = MATCH_RE.search(line)
            if match:
                rows.append({
                    "ts": match.group(1),
                    "latency_ms": int(match.group(2)),
                    "broker_id": match.group(3),
                })
    return rows


def load_verify_summary(log_path: str) -> dict:
    if not os.path.exists(log_path):
        return {"verified": 0, "failed": 0, "total": 0}

    with open(log_path) as f:
        for line in f:
            match = VERIFY_SUMMARY_RE.search(line)
            if match:
                return {
                    "verified": int(match.group(1)),
                    "failed": int(match.group(2)),
                    "total": int(match.group(3)),
                }
    return {"verified": 0, "failed": 0, "total": 0}


def load_publisher_metrics(scenario_dir: str) -> dict:
    path = os.path.join(scenario_dir, "publisher_metrics.json")
    if not os.path.exists(path):
        return {"pubs_sent": None, "duration_s": None, "feed_duration_min": None}
    with open(path, encoding="utf-8") as f:
        return json.load(f)


def _latency_stats(latencies: list[int]) -> dict:
    if not latencies:
        return {}
    sorted_lat = sorted(latencies)
    return {
        "mean": statistics.mean(sorted_lat),
        "min": min(sorted_lat),
        "max": max(sorted_lat),
        "p50": sorted_lat[len(sorted_lat) // 2],
        "p95": sorted_lat[int(len(sorted_lat) * 0.95)],
        "p99": sorted_lat[int(len(sorted_lat) * 0.99)],
        "count": len(sorted_lat),
    }


def collect_results(scenario_dir: str) -> dict:
    all_ts = set()
    all_latencies = []
    in_window_latencies = []
    per_sub = {}
    verify = {"verified": 0, "failed": 0, "total": 0}

    for sid in SUBSCRIBERS:
        log_path = os.path.join(scenario_dir, f"{sid}.log")
        rows = load_matches(log_path)
        per_sub[sid] = len(rows)
        for row in rows:
            all_ts.add(int(row["ts"]))
            all_latencies.append(row["latency_ms"])
            if row["latency_ms"] <= IN_WINDOW_LATENCY_MS:
                in_window_latencies.append(row["latency_ms"])

        summary = load_verify_summary(log_path)
        verify["verified"] += summary["verified"]
        verify["failed"] += summary["failed"]
        verify["total"] += summary["total"]

    metrics = load_publisher_metrics(scenario_dir)
    pubs_sent = metrics.get("pubs_sent") or len(all_ts)

    total_unique = len(all_ts)
    total_matches = sum(per_sub.values())
    delivery_rate_sent = total_unique / pubs_sent * 100 if pubs_sent else 0.0
    delivery_rate_corpus = total_unique / TOTAL_PUBLICATIONS * 100

    return {
        "scenario": os.path.basename(scenario_dir),
        "per_subscriber": per_sub,
        "total_matches": total_matches,
        "unique_delivered": total_unique,
        "pubs_sent": pubs_sent,
        "total_publications": TOTAL_PUBLICATIONS,
        "delivery_rate": delivery_rate_sent,
        "delivery_rate_corpus": delivery_rate_corpus,
        "latency": _latency_stats(in_window_latencies),
        "latency_all": _latency_stats(all_latencies),
        "verify": verify,
        "publisher_metrics": metrics,
    }


def print_results(results: dict) -> None:
    print(f"\n{'=' * 50}")
    print("  Rezultate experiment")
    print(f"{'=' * 50}")
    print("\n  a) Publicații livrate:")
    for sid, count in results["per_subscriber"].items():
        print(f"     {sid}: {count}")
    print(f"     Total match-uri primite: {results['total_matches']}")
    print(f"     Publicații trimise (publisher): {results['pubs_sent']}")
    print(f"     Publicații unice livrate: {results['unique_delivered']}")
    print(f"     Rată de livrare (trimise): {results['delivery_rate']:.2f}%")
    print(f"     Rată de livrare (corpus 90k): {results['delivery_rate_corpus']:.2f}%")

    latency = results["latency"]
    if latency:
        print(f"\n  b) Latență end-to-end (ms, matches cu latency ≤ {IN_WINDOW_LATENCY_MS} ms):")
        print(f"     Esantion: {latency['count']} notificări")
        print(f"     Medie: {latency['mean']:.2f}")
        print(f"     Minim: {latency['min']}")
        print(f"     Maxim: {latency['max']}")
        print(f"     P50:   {latency['p50']}")
        print(f"     P95:   {latency['p95']}")
        print(f"     P99:   {latency['p99']}")

    verify = results["verify"]
    if verify["total"]:
        rate = verify["verified"] / verify["total"] * 100
        print("\n  c) Corectitudine matching:")
        print(f"     Verificate: {verify['verified']}/{verify['total']} ({rate:.1f}%)")
        print(f"     Eșecuri:    {verify['failed']}/{verify['total']}")

    print(f"\n  d) Rată de matching (trimise): {results['delivery_rate']:.2f}%")
    print(f"{'=' * 50}\n")
