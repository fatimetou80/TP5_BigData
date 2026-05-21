# Getting Started — TP5 Fraud Detection

Step-by-step guide to run the full pipeline locally with Docker Compose.

## Prerequisites

- Docker Desktop (Windows/macOS) or Docker Engine (Linux)
- 8 GB+ RAM recommended for dev defaults (`N=2000`, `M=4000`)
- Ports free: `7077`, `8080`, `8081`, `8888`, `9092`, `9094`

## 1. Clone and enter the project

```bash
cd TP5_BigData
```

## 2. Start infrastructure

```bash
docker compose up -d --build
```

Wait until all services are healthy (2–5 minutes on first build).

### Verify containers

```bash
docker compose ps
```

Expected running services:

- `tp5-kafka`, `tp5-kafka-ui`
- `tp5-spark-master`, `tp5-spark-worker-{1,2,3}`
- `tp5-transaction-generator`
- `tp5-spark-processor`
- `tp5-jupyter`

## 3. Execution traces

### Kafka topic creation (`kafka-init`)

```
Topic bank-transactions ready.
```

### Transaction generator (every ~10s)

```
2026-05-21 12:00:10 [INFO] tick=10 batch_size=42 total_sent=380
```

### Spark processor

```
All streaming queries started. Awaiting termination...
```

### Check Kafka messages (optional)

```bash
docker exec tp5-kafka /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 \
  --topic bank-transactions \
  --max-messages 3
```

Sample message:

```json
{
  "msg_entity": "bank_X",
  "app_type": "mobile_app",
  "send_entity": "bank_X",
  "receive_entity": "bank_A",
  "send_id": "client_000042",
  "receive_id": "user_a_000117",
  "amount": 245.33,
  "date": "2026-05-21T14:32:15Z",
  "tx_type": "transfer",
  "tx_id": "a1b2c3d4-..."
}
```

### Verify Spark output (after ~30s)

```bash
docker exec tp5-jupyter ls -la /workspace/data/output/
```

You should see directories such as:

- `recent_activity/`
- `recent_users/`
- `lifetime/`
- `windowed_3h/`, `windowed_7d/`, `windowed_3w/`, `windowed_3mo/`

## 4. Open the dashboard

1. Browser: http://localhost:8888
2. Token: `tp5fraud2026`
3. Open `work/fraud_dashboard.ipynb`
4. Run all cells — the dashboard auto-refreshes every 5 seconds (configurable)

## 5. Monitoring UIs

| UI | URL |
|----|-----|
| Kafka UI | http://localhost:8080 |
| Spark Master | http://localhost:8081 |

## 6. Tune simulation size

**Small smoke test:**

```bash
N_CLIENTS=500 M_EXTERNAL=1000 docker compose up -d --build
```

**Closer to specification:**

```bash
N_CLIENTS=10000 M_EXTERNAL=20000 PEAK_MULTIPLIER=8 docker compose up -d --build
```

## 7. Stop and clean

```bash
docker compose down
```

Remove volumes (reset Kafka + checkpoints):

```bash
docker compose down -v
rm -rf data/output/* checkpoints/*
```

## Troubleshooting

| Issue | Fix |
|-------|-----|
| Dashboard empty | Wait 30–60s; confirm `tp5-spark-processor` logs show active queries |
| Kafka connection refused | `docker compose restart kafka` then `kafka-init` |
| Spark OOM | Lower `N_CLIENTS` / `M_EXTERNAL` or increase memory in compose |
| Port conflict | Change host ports in `docker-compose.yml` |

## Submission checklist

- [ ] README.md and getting_started.md updated
- [ ] Group members listed in README
- [ ] Public Git repo link + commit hash sent to instructor
- [ ] Presentation prepared (architecture + demo)
