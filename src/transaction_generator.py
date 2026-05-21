"""
Bank X transaction generator for TP5 — Real-time Fraud Detection.
Simulates N + M individuals and publishes JSON batches to Kafka every second.
"""

from __future__ import annotations

import json
import logging
import os
import signal
import sys
import time
import uuid
from dataclasses import dataclass
from datetime import datetime, timezone

import numpy as np
from confluent_kafka import Producer

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    stream=sys.stdout,
)
log = logging.getLogger("transaction_generator")

BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
TOPIC = os.getenv("KAFKA_TOPIC", "bank-transactions")
N_CLIENTS = int(os.getenv("N_CLIENTS", "2000"))
M_EXTERNAL = int(os.getenv("M_EXTERNAL", "4000"))
PEAK_MULTIPLIER = float(os.getenv("PEAK_MULTIPLIER", "5"))
FRAUD_RATE = float(os.getenv("FRAUD_RATE", "0.001"))
TICK_SECONDS = float(os.getenv("TICK_SECONDS", "1"))

INCOME_MIN = 1_000.0
INCOME_MAX = 1_000_000.0
SECONDS_PER_MONTH = 30 * 24 * 3600

_running = True


def _handle_signal(signum, frame):
    global _running
    log.info("Shutdown signal %s received, stopping...", signum)
    _running = False


signal.signal(signal.SIGINT, _handle_signal)
signal.signal(signal.SIGTERM, _handle_signal)


@dataclass
class Population:
    ids: np.ndarray
    banks: np.ndarray
    income: np.ndarray
    spending: np.ndarray
    balance: np.ndarray
    tx_prob: np.ndarray


def sample_power_law_income(n: int, rng: np.random.Generator) -> np.ndarray:
    """P(I) ∝ I^-2 on [INCOME_MIN, INCOME_MAX] via inverse CDF on 1/I."""
    u = rng.random(n)
    inv = (1.0 / INCOME_MAX) + (1.0 / INCOME_MIN - 1.0 / INCOME_MAX) * (1.0 - u)
    return 1.0 / inv


def build_population(n_clients: int, m_external: int, rng: np.random.Generator) -> Population:
    total = n_clients + m_external
    ids = np.empty(total, dtype=object)
    banks = np.empty(total, dtype=object)

    for i in range(n_clients):
        ids[i] = f"client_{i:06d}"
        banks[i] = "bank_X"

    half = m_external // 2
    for i in range(m_external):
        idx = n_clients + i
        if i < half:
            ids[idx] = f"user_a_{i:06d}"
            banks[idx] = "bank_A"
        else:
            ids[idx] = f"user_b_{i - half:06d}"
            banks[idx] = "bank_B"

    income = sample_power_law_income(total, rng)
    low = income / 1000.0
    high = income / 100.0
    spending = rng.uniform(low, high)
    balance = rng.uniform(0.0, 3.0 * income)
    freq_month = income / spending
    tx_prob = freq_month / SECONDS_PER_MONTH

    return Population(ids=ids, banks=banks, income=income, spending=spending, balance=balance, tx_prob=tx_prob)


def peak_factor(now: datetime) -> float:
    """Higher activity during business hours (UTC+0 demo)."""
    hour = now.hour
    if 9 <= hour <= 18:
        return PEAK_MULTIPLIER
    return 1.0


def make_message(
    send_idx: int,
    recv_idx: int,
    amount: float,
    pop: Population,
    tx_type: str = "transfer",
) -> dict:
    now = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    return {
        "msg_entity": "bank_X",
        "app_type": "mobile_app",
        "send_entity": str(pop.banks[send_idx]),
        "receive_entity": str(pop.banks[recv_idx]),
        "send_id": str(pop.ids[send_idx]),
        "receive_id": str(pop.ids[recv_idx]),
        "amount": round(float(amount), 2),
        "date": now,
        "tx_type": tx_type,
        "tx_id": str(uuid.uuid4()),
    }


def inject_fraud(pop: Population, rng: np.random.Generator) -> list[dict]:
    """Abnormal high-value burst from a random sender."""
    sender = int(rng.integers(0, len(pop.ids)))
    receiver = int(rng.integers(0, len(pop.ids)))
    while receiver == sender:
        receiver = int(rng.integers(0, len(pop.ids)))
    amount = min(float(pop.balance[sender]), float(pop.income[sender] * rng.uniform(2.0, 5.0)))
    if amount <= 0:
        return []
    pop.balance[sender] -= amount
    pop.balance[receiver] += amount
    return [make_message(sender, receiver, amount, pop, tx_type="fraud_alert")]


def simulate_tick(pop: Population, rng: np.random.Generator, now: datetime) -> list[dict]:
    multiplier = peak_factor(now)
    probs = np.clip(pop.tx_prob * multiplier, 0.0, 1.0)
    active = rng.random(len(pop.ids)) < probs
    active_idx = np.flatnonzero(active)
    messages: list[dict] = []

    for send_idx in active_idx:
        balance = float(pop.balance[send_idx])
        if balance <= 0:
            continue
        recv_idx = int(rng.integers(0, len(pop.ids)))
        if recv_idx == send_idx:
            recv_idx = (recv_idx + 1) % len(pop.ids)

        si = float(pop.spending[send_idx])
        sigma = si / 2.0
        amount = float(rng.uniform(si - 2 * sigma, si + 2 * sigma))
        amount = max(1.0, amount)
        if amount > balance:
            continue

        pop.balance[send_idx] -= amount
        pop.balance[recv_idx] += amount
        messages.append(make_message(send_idx, recv_idx, amount, pop))

    if rng.random() < FRAUD_RATE:
        messages.extend(inject_fraud(pop, rng))

    return messages


def main() -> None:
    log.info(
        "Starting generator N=%d M=%d total=%d topic=%s bootstrap=%s",
        N_CLIENTS,
        M_EXTERNAL,
        N_CLIENTS + M_EXTERNAL,
        TOPIC,
        BOOTSTRAP,
    )

    rng = np.random.default_rng(42)
    pop = build_population(N_CLIENTS, M_EXTERNAL, rng)

    producer = Producer(
        {
            "bootstrap.servers": BOOTSTRAP,
            "linger.ms": 50,
            "batch.num.messages": 10000,
            "compression.type": "snappy",
        }
    )

    tick = 0
    total_sent = 0
    while _running:
        t0 = time.perf_counter()
        now = datetime.now(timezone.utc)
        batch = simulate_tick(pop, rng, now)

        for msg in batch:
            producer.produce(TOPIC, value=json.dumps(msg).encode("utf-8"))
        producer.poll(0)
        producer.flush(timeout=5)

        total_sent += len(batch)
        tick += 1
        if tick % 10 == 0:
            log.info("tick=%d batch_size=%d total_sent=%d", tick, len(batch), total_sent)

        elapsed = time.perf_counter() - t0
        sleep_time = max(0.0, TICK_SECONDS - elapsed)
        if sleep_time > 0:
            time.sleep(sleep_time)

    producer.flush()
    log.info("Generator stopped. total_messages=%d", total_sent)


if __name__ == "__main__":
    main()
