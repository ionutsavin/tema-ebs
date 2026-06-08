# Evaluation Report — Encrypted Matching with OPE and AES-ECB

## Experimental Setup

- **3,333 subscriptions** per subscriber (3 subscribers, ~10,000 total registrations)
- **90,000 publications** in corpus; publisher streams for **3 minutes** at 2ms target interval
- **3 brokers** in chain: `broker_0` (Kafka consumer) → `broker_1` → `broker_2`
- Fields: `company` (text, AES-ECB), `value` (numeric, OPE), `drop` (numeric, OPE), `variation` (numeric, OPE)
- Matching performed entirely on ciphertext (broker never sees plaintext)
- All notifications decrypted and verified offline by subscribers

## Scenarios

| Scenario | `equalityWeights.company` | `company` constraints |
|----------|--------------------------|-----------------------|
| 100% eq  | 1.0                      | 100% `=`, 0% `!=`    |
| 25% eq   | 0.25                     | 25% `=`, 75% `!=`    |

## Results

### a) Publication Delivery

| Metric | 100% equality | 25% equality |
|--------|--------------|--------------|
| Publications sent (3 min feed) | 70,543 | 66,633 |
| Unique pubs delivered | 68,904 | 65,173 |
| **Delivery rate (sent / received)** | **97.68%** | **97.81%** |
| Delivery rate (vs 90k corpus) | 76.56% | 72.41% |
| Matches per subscriber (avg) | 68,904 | 182,373 |
| Total matches (all subs) | 206,712 | 547,119 |

> **Delivery rate** = unique publications that produced ≥1 match / publications actually sent.
> A 20s drain period after publishing allows the broker pipeline to flush before measurement stops.

### b) End-to-End Latency

Measured on match notifications with latency ≤ 10s (excludes backlog-inflated outliers from the previous metric).

| Metric | 100% equality | 25% equality |
|--------|--------------|--------------|
| Mean | 5.55 ms | 6.28 ms |
| Min | 0 ms | 1 ms |
| Max | 33 ms | 87 ms |
| P50 | 6 ms | 6 ms |
| P95 | 8 ms | 9 ms |
| P99 | 9 ms | 10 ms |

### c) Matching Correctness

| Scenario | Matches verified | Failures | Pass rate |
|----------|-----------------|----------|-----------|
| 100% eq | 206,712 | 0 | **100%** |
| 25% eq | 547,119 | 0 | **100%** |

Every match notification was decrypted by the subscriber and verified against the plaintext subscriptions using `MatchingEngine.matches()`.

## Interpretation

### Delivery Rate

The primary delivery rate compares unique matched publications against those **actually sent** during the 3-minute feed (97.68% vs 97.81%).

The 25% equality scenario shows a **0.13 percentage point higher** delivery rate. With ~75% `!=` constraints, each publication matches more subscriptions (~4/5 companies), producing more notifications per unique publication.

Supporting evidence:
- Publications sent: 70,543 (100% eq) vs 66,633 (25% eq)
- Unique publications delivered: 68,904 → 65,173 (-3,731)
- Total matches: 206,712 → 547,119 (+340,407)

### Latency

End-to-end latency is computed as `receive_time − publication._ts` for matches delivered within 10s. This reflects true in-feed delivery; matches processed from backlog after longer delays are excluded.

| | 100% equality | 25% equality |
|--|--|--|
| Mean | 5.55 ms | 6.28 ms |
| P50 | 6 ms | 6 ms |
| P99 | 9 ms | 10 ms |

### Correctness

AES-ECB deterministic encryption guarantees correct equality matching on ciphertext. OPE guarantees correct inequality matching for numeric fields. Across both experiments, 753,831 matches were verified with 0 failures.

## Limitations

1. **Publisher throughput**: Serialization and Kafka I/O cap how many of the 90k corpus publications are sent in 3 minutes.
2. **Single Kafka partition**: Only `broker_0` consumes from Kafka.
3. **Corpus denominator**: The 90k figure is the generated dataset size; the sent-based rate is the fair operational metric.
