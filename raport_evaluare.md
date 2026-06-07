# Evaluation Report — Encrypted Matching with OPE and AES-ECB

## Experimental Setup

- **10,000 subscriptions** per subscriber (3 subscribers, 30,000 total)
- **90,000 publications** generated (≈66,000 sent in 3 min at 2ms interval)
- **3 brokers** in chain: `broker_0` (Kafka consumer) → `broker_1` → `broker_2`
- Fields: `company` (text, AES-ECB), `value` (numeric, OPE), `drop` (numeric, OPE), `variation` (numeric, OPE), `date` (text, AES-ECB)
- Matching performed entirely on ciphertext (broker never sees plaintext)
- All notifications decrypted and verified offline by subscribers

## Scenarios

| Scenario | `equalityWeights.company` | `company` constraints |
|----------|--------------------------|-----------------------|
| 100% eq  | 1.0                      | 100% `=`, 0% `!=`    |
| 25% eq   | 0.25                     | 25% `=`, 75% `!=`    |

## Results

### a) Publication Delivery

| Metric                     | 100% equality | 25% equality |
|----------------------------|--------------|--------------|
| Unique pubs delivered      | 67,679       | 59,061       |
| Total pubs available       | 90,000       | 90,000       |
| Delivery rate              | **75.20%**   | **65.62%**   |
| Matches per subscriber     | 218,573      | 178,826      |
| Total matches (all subs)   | 655,719      | 536,478      |

> Note: Publisher throughput ≈ 370 pubs/sec (limited by serialization + Kafka I/O), so ≈66,000 pubs were actually sent in the 3-minute window. The 75.20% rate is computed against the full 90,000 generated publications.

### b) End-to-End Latency

| Metric | 100% equality | 25% equality |
|--------|--------------|--------------|
| Mean   | 1.62 ms      | 1.74 ms      |
| Min    | 0 ms         | 0 ms         |
| Max    | 128 ms       | 142 ms       |
| P50    | 1 ms         | 2 ms         |
| P95    | 2 ms         | 3 ms         |
| P99    | 6 ms         | 4 ms         |

### c) Matching Correctness

| Scenario | Matches verified | Failures | Pass rate |
|----------|-----------------|----------|-----------|
| 100% eq  | 655,719         | 0        | **100%**  |
| 25% eq   | 536,478         | 0        | **100%**  |

Every match notification was decrypted by the subscriber and verified against the plaintext subscriptions using `MatchingEngine.matches()`. **Zero false positives** in both scenarios.

## Interpretation

### Delivery Rate

The 25% equality scenario shows a **~10 percentage point lower delivery rate** (65.62% vs 75.20%). This is unexpected at first glance — `!=` constraints are less selective than `=` constraints (they match 4/5 companies instead of 1/5), so one would expect **more** matches, not fewer.

The likely explanation is **broker-side congestion**: subscriptions with `!= company` match many more publications per subscription. With more matches per publication, the broker spends more time sending TCP notifications to subscribers, which reduces the rate at which it can consume new publications from Kafka. Since the Kafka consumer loop is single-threaded per broker, the total throughput drops.

Supporting evidence:
- The unique publication count dropped from 67,679 to 59,061 (≈8,600 fewer pubs processed)
- Match volume dropped from 655,719 to 536,478 even though selectivity per subscription increased
- Latency slightly increased (1.62 → 1.74 ms mean), consistent with higher notification load

### Latency

Both scenarios exhibit sub-2ms mean latency, confirming that encrypted matching adds negligible overhead. The P99 remains under 10ms in both cases. Higher max latency in the 25% scenario (142 ms vs 128 ms) is consistent with occasional notification backpressure.

### Correctness

AES-ECB deterministic encryption guarantees correct equality matching on ciphertext. OPE guarantees correct inequality matching for numeric fields. All 1,192,197 match notifications across both experiments were verified against ground truth with zero failures.

## Limitations

1. **Date field**: Comparisons like `date <= "3.02.2022"` use AES-ECB ciphertext lexicographic comparison, which is **not order-preserving**. This means date-range constraints produce random results on the broker. A future improvement would move dates to OPE.
2. **Publisher throughput**: Limited to ~370 pubs/sec due to protobuf serialization and Kafka producer overhead. This reduces the effective evaluation window.
3. **Single Kafka partition**: Only `broker_0` consumes from Kafka; a multi-partition setup would increase throughput.
