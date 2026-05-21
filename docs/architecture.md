# Architecture & Design Decisions — TP5

## 1. Overview

The system implements a **lambda-style real-time path**: events are generated continuously, ingested by Kafka, processed by Spark Structured Streaming, and exposed to analysts through a Jupyter dashboard reading Parquet snapshots.

## 2. Component Diagram

```
┌─────────────────────┐     ┌──────────────┐     ┌─────────────────────────┐
│ transaction_        │     │    Kafka     │     │   spark_processor.py    │
│ generator.py        │────▶│  (3 parts)   │────▶│  • 4 sliding windows    │
│  • N+M individuals  │     │ bank-trans.  │     │  • lifetime aggregates  │
│  • 1 tick / second  │     └──────┬───────┘     │  • recent 10s batch     │
└─────────────────────┘            │             └───────────┬─────────────┘
                                   │                         │
                            ┌──────▼──────┐           ┌──────▼──────┐
                            │  Kafka UI   │           │ data/output │
                            └─────────────┘           │  (Parquet)  │
                                                      └──────┬──────┘
                                                             │
                                                      ┌──────▼──────┐
                                                      │  Jupyter    │
                                                      │  dashboard  │
                                                      └─────────────┘
```

## 3. Design Decisions

### 3.1 Message format

Strict JSON field names from the TP specification (`send_id`, `receive_id`, `amount`, ISO-8601 `date`, etc.).

### 3.2 Generator performance

- **Vectorized NumPy** Bernoulli trials per second instead of Python loops over 300k users.
- **Batched Kafka produce** per tick with LZ4 compression.
- **Peak multiplier** during hours 9–18 UTC to simulate business-hour load without changing core formulas.

### 3.3 Spark: multiple queries vs. single job

We run **separate Structured Streaming queries** with independent Kafka consumer groups:

| Query | Output | Mode |
|-------|--------|------|
| Window 3h / 7d / 3w / 3mo | `windowed_*` | `update` |
| Lifetime | `lifetime` | `complete` |
| Recent activity | `recent_*` | `foreachBatch` |

**Why:** Different window lengths need different watermark delays; isolating queries avoids state explosion in one DAG.

### 3.4 Distinct counterparties

`approx_count_distinct` (HyperLogLog++) is used instead of exact `count(distinct)` to meet memory constraints at scale.

### 3.5 Dashboard storage

Parquet under `data/output/` is a **simple, notebook-friendly sink**. Alternatives (Delta Lake, JDBC) were avoided to minimize moving parts for the course demo.

### 3.6 Fault tolerance

- Kafka: single broker (dev), replication factor 1
- Spark: checkpoint directories under `checkpoints/`
- Services: `restart: unless-stopped` for generator and processor

## 4. Window Specification Mapping

| TP requirement | Implementation |
|----------------|----------------|
| Last 3 hours | `window(event_time, "3 hours", "30 minutes")` |
| Last 7 days | `window(..., "7 days", "1 hour")` |
| Last 3 weeks | `window(..., "21 days", "6 hours")` |
| Last 3 months | `window(..., "90 days", "1 day")` |

Watermarks: 20 min (3h), 2h (7d), 12h (3w), 2d (3mo).

## 5. Security & Operations (demo scope)

- No authentication on Kafka/Jupyter (local lab only).
- Jupyter token set via `JUPYTER_TOKEN` environment variable.
- Not production-hardened; documented intentionally.

## 6. Known Limitations

1. **Long-window state** for 90-day windows is heavy at full 300k-user scale — use dev population or increase cluster RAM.
2. **Distinct-set deltas** across windows are computed in the dashboard from consecutive snapshots (not a separate Spark output).
3. **Starting offset `latest`** on processor — cold start misses history; appropriate for live demo.

## 7. Future Improvements

- Delta Lake for ACID upserts on lifetime metrics
- Spark job metrics → Prometheus/Grafana
- ML-based fraud scoring on top of aggregated features
- Kubernetes deployment for true 1000+ tx/s soak tests
