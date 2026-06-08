#!/usr/bin/env python3
"""Generate raport_evaluare.md from experiment results."""

import os

from aggregate_utils import IN_WINDOW_LATENCY_MS, collect_results

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
REPORT_PATH = os.path.join(ROOT, "raport_evaluare.md")
SCENARIOS = {
    "100pct_eq": "100% equality",
    "25pct_eq": "25% equality",
}


def _fmt_int(n: int) -> str:
    return f"{n:,}"


def _latency_cell(results: dict, key: str) -> str:
    latency = results.get("latency", {})
    if not latency:
        return "N/A"
    value = latency[key]
    if key == "mean":
        return f"{value:.2f} ms"
    return f"{int(value)} ms"


def generate_report(results_by_scenario: dict) -> str:
    r100 = results_by_scenario["100pct_eq"]
    r25 = results_by_scenario["25pct_eq"]

    v100 = r100["verify"]
    v25 = r25["verify"]
    pass100 = v100["verified"] / v100["total"] * 100 if v100["total"] else 0
    pass25 = v25["verified"] / v25["total"] * 100 if v25["total"] else 0
    total_verified = v100["verified"] + v25["verified"]
    total_failures = v100["failed"] + v25["failed"]

    avg_per_sub_100 = r100["total_matches"] // 3
    avg_per_sub_25 = r25["total_matches"] // 3

    lines = [
        "# Evaluation Report — Encrypted Matching with OPE and AES-ECB",
        "",
        "## Experimental Setup",
        "",
        "- **3,333 subscriptions** per subscriber (3 subscribers, ~10,000 total registrations)",
        "- **90,000 publications** in corpus; publisher streams for **3 minutes** at 2ms target interval",
        "- **3 brokers** in chain: `broker_0` (Kafka consumer) → `broker_1` → `broker_2`",
        "- Fields: `company` (text, AES-ECB), `value` (numeric, OPE), `drop` (numeric, OPE), `variation` (numeric, OPE)",
        "- Matching performed entirely on ciphertext (broker never sees plaintext)",
        "- All notifications decrypted and verified offline by subscribers",
        "",
        "## Scenarios",
        "",
        "| Scenario | `equalityWeights.company` | `company` constraints |",
        "|----------|--------------------------|-----------------------|",
        "| 100% eq  | 1.0                      | 100% `=`, 0% `!=`    |",
        "| 25% eq   | 0.25                     | 25% `=`, 75% `!=`    |",
        "",
        "## Results",
        "",
        "### a) Publication Delivery",
        "",
        "| Metric | 100% equality | 25% equality |",
        "|--------|--------------|--------------|",
        f"| Publications sent (3 min feed) | {_fmt_int(r100['pubs_sent'])} | {_fmt_int(r25['pubs_sent'])} |",
        f"| Unique pubs delivered | {_fmt_int(r100['unique_delivered'])} | {_fmt_int(r25['unique_delivered'])} |",
        f"| **Delivery rate (sent / received)** | **{r100['delivery_rate']:.2f}%** | **{r25['delivery_rate']:.2f}%** |",
        f"| Delivery rate (vs 90k corpus) | {r100['delivery_rate_corpus']:.2f}% | {r25['delivery_rate_corpus']:.2f}% |",
        f"| Matches per subscriber (avg) | {_fmt_int(avg_per_sub_100)} | {_fmt_int(avg_per_sub_25)} |",
        f"| Total matches (all subs) | {_fmt_int(r100['total_matches'])} | {_fmt_int(r25['total_matches'])} |",
        "",
        "> **Delivery rate** = unique publications that produced ≥1 match / publications actually sent.",
        "> A 20s drain period after publishing allows the broker pipeline to flush before measurement stops.",
        "",
        "### b) End-to-End Latency",
        "",
        f"Measured on match notifications with latency ≤ {IN_WINDOW_LATENCY_MS // 1000}s "
        "(excludes backlog-inflated outliers from the previous metric).",
        "",
        "| Metric | 100% equality | 25% equality |",
        "|--------|--------------|--------------|",
        f"| Mean | {_latency_cell(r100, 'mean')} | {_latency_cell(r25, 'mean')} |",
        f"| Min | {_latency_cell(r100, 'min')} | {_latency_cell(r25, 'min')} |",
        f"| Max | {_latency_cell(r100, 'max')} | {_latency_cell(r25, 'max')} |",
        f"| P50 | {_latency_cell(r100, 'p50')} | {_latency_cell(r25, 'p50')} |",
        f"| P95 | {_latency_cell(r100, 'p95')} | {_latency_cell(r25, 'p95')} |",
        f"| P99 | {_latency_cell(r100, 'p99')} | {_latency_cell(r25, 'p99')} |",
        "",
        "### c) Matching Correctness",
        "",
        "| Scenario | Matches verified | Failures | Pass rate |",
        "|----------|-----------------|----------|-----------|",
        f"| 100% eq | {_fmt_int(v100['verified'])} | {v100['failed']} | **{pass100:.0f}%** |",
        f"| 25% eq | {_fmt_int(v25['verified'])} | {v25['failed']} | **{pass25:.0f}%** |",
        "",
        "Every match notification was decrypted by the subscriber and verified against the "
        "plaintext subscriptions using `MatchingEngine.matches()`.",
        "",
        "## Interpretation",
        "",
        "### Delivery Rate",
        "",
    ]

    delta = r25["delivery_rate"] - r100["delivery_rate"]
    unique_delta = r25["unique_delivered"] - r100["unique_delivered"]
    match_delta = r25["total_matches"] - r100["total_matches"]

    lines.extend([
        f"The primary delivery rate compares unique matched publications against those **actually sent** "
        f"during the 3-minute feed ({r100['delivery_rate']:.2f}% vs {r25['delivery_rate']:.2f}%).",
        "",
        f"The 25% equality scenario shows a **{abs(delta):.2f} percentage point "
        f"{'higher' if delta >= 0 else 'lower'}** delivery rate. "
        "With ~75% `!=` constraints, each publication matches more subscriptions (~4/5 companies), "
        "producing more notifications per unique publication.",
        "",
        "Supporting evidence:",
        f"- Publications sent: {_fmt_int(r100['pubs_sent'])} (100% eq) vs {_fmt_int(r25['pubs_sent'])} (25% eq)",
        f"- Unique publications delivered: {_fmt_int(r100['unique_delivered'])} → {_fmt_int(r25['unique_delivered'])} "
        f"({'+' if unique_delta >= 0 else ''}{_fmt_int(unique_delta)})",
        f"- Total matches: {_fmt_int(r100['total_matches'])} → {_fmt_int(r25['total_matches'])} "
        f"({'+' if match_delta >= 0 else ''}{_fmt_int(match_delta)})",
        "",
        "### Latency",
        "",
    ])

    if r100["latency"] and r25["latency"]:
        lines.extend([
            f"End-to-end latency is computed as `receive_time − publication._ts` for matches "
            f"delivered within {IN_WINDOW_LATENCY_MS // 1000}s. This reflects true in-feed delivery; "
            "matches processed from backlog after longer delays are excluded.",
            "",
            f"| | 100% equality | 25% equality |",
            f"|--|--|--|",
            f"| Mean | {r100['latency']['mean']:.2f} ms | {r25['latency']['mean']:.2f} ms |",
            f"| P50 | {int(r100['latency']['p50'])} ms | {int(r25['latency']['p50'])} ms |",
            f"| P99 | {int(r100['latency']['p99'])} ms | {int(r25['latency']['p99'])} ms |",
        ])
    else:
        lines.append("In-window latency data was not available from subscriber logs.")

    lines.extend([
        "",
        "### Correctness",
        "",
        f"AES-ECB deterministic encryption guarantees correct equality matching on ciphertext. "
        f"OPE guarantees correct inequality matching for numeric fields. "
        f"Across both experiments, {_fmt_int(total_verified)} matches were verified with "
        f"{total_failures} failures.",
        "",
        "## Limitations",
        "",
        "1. **Publisher throughput**: Serialization and Kafka I/O cap how many of the 90k corpus publications are sent in 3 minutes.",
        "2. **Single Kafka partition**: Only `broker_0` consumes from Kafka.",
        "3. **Corpus denominator**: The 90k figure is the generated dataset size; the sent-based rate is the fair operational metric.",
        "",
    ])

    return "\n".join(lines)


def main():
    experiments_dir = os.path.dirname(os.path.abspath(__file__))
    results = {}
    for scenario in SCENARIOS:
        results[scenario] = collect_results(os.path.join(experiments_dir, scenario))

    report = generate_report(results)
    with open(REPORT_PATH, "w", encoding="utf-8") as f:
        f.write(report)
    print(f"Report written to {REPORT_PATH}")


if __name__ == "__main__":
    main()
