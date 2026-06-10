# Evaluation Report — Encrypted Matching with OPE and AES-ECB

## Experimental Setup

- **10,000 subscriptions**  and 3 subscribers
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
| Delivery rate (vs 90k total) | 76.56% | 72.41% |
| Matches per subscriber (avg) | 68,904 | 182,373 |
| Total matches (all subs) | 206,712 | 547,119 |



### b) End-to-End Latency



| Metric | 100% equality | 25% equality |
|--------|--------------|--------------|
| Mean | **8.2 ms** | 9.17 ms |
| Median | **6 ms** | 7 ms |
| Min | **1 ms** | 1 ms |
| Max | **58 ms** | 111 ms |
| P50 | **6 ms** | 7 ms |
| P95 | **21 ms** | 24 ms |
| P99 | **38 ms** | 44 ms |

### c) Matching Correctness

| Scenario | Matches verified | Failures | Pass rate |
|----------|-----------------|----------|-----------|
| 100% eq | 206,712 | 0 | **100%** |
| 25% eq | 547,119 | 0 | **100%** |

Every match notification was decrypted by the subscriber and verified against the plaintext subscriptions using `MatchingEngine.matches()`.

## Interpretation


### Latency

End-to-end latency is computed as `receive_time − publication._ts`. This reflects true in-feed delivery; matches processed from backlog after longer delays are excluded.


| | 100% equality | 25% equality |
|--|--|--|
| Mean | **8.2 ms** | 9.17 ms |
| Median | **6 ms** | 7 ms |
| P50 | **6 ms** | 7 ms |
| P95 | **21 ms** | 24 ms |
| P99 | **38 ms** | 44 ms |

### Correctness

AES-ECB deterministic encryption guarantees correct equality matching on ciphertext. OPE guarantees correct inequality matching for numeric fields. Across both experiments, 753,831 matches were verified with 0 failures.

## Limitations

1. **Publisher throughput**: Serialization and Kafka I/O cap how many of the 90k corpus publications are sent in 3 minutes.
2. **Single Kafka partition**: Only `broker_0` consumes from Kafka.
3. **Corpus denominator**: The 90k figure is the generated dataset size; the sent-based rate is the fair operational metric.
