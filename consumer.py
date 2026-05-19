"""
consumer.py  –  Output Consumer
--------------------------------
Reads from the 'predictions' topic and pretty-prints each result to the
console in real time.

Usage:
    python consumer.py [--topic predictions]
"""

import argparse
import json
from datetime import datetime

from kafka import KafkaConsumer
from kafka.errors import KafkaError

import config

# ── CLI ───────────────────────────────────────────────────────────────────────
parser = argparse.ArgumentParser(description="Fraud Predictions – Console Consumer")
parser.add_argument("--topic", type=str, default="predictions", help="Topic to read from")
args = parser.parse_args()

# ── ANSI colours ──────────────────────────────────────────────────────────────
RED    = "\033[91m"
GREEN  = "\033[92m"
YELLOW = "\033[93m"
CYAN   = "\033[96m"
RESET  = "\033[0m"
BOLD   = "\033[1m"

BANNER = f"""
{CYAN}{BOLD}╔══════════════════════════════════════════════════╗
║   Real-Time Credit Card Fraud Detection            ║
║   Consuming from: {args.topic:<29} ║
╚══════════════════════════════════════════════════╝{RESET}
"""
print(BANNER)

# ── Kafka consumer ────────────────────────────────────────────────────────────
print(f"[Consumer] Connecting to {config.BOOTSTRAP_SERVERS} …")
consumer = KafkaConsumer(
    args.topic,
    bootstrap_servers=config.BOOTSTRAP_SERVERS,
    security_protocol="SASL_SSL",
    sasl_mechanism="PLAIN",
    sasl_plain_username=config.API_KEY,
    sasl_plain_password=config.API_SECRET,
    value_deserializer=lambda v: json.loads(v.decode("utf-8")),
    auto_offset_reset="earliest",
    enable_auto_commit=True,
    group_id="fraud-output-consumer",
)
print(f"[Consumer] Connected. Waiting for predictions …\n")

# ── Running counters ──────────────────────────────────────────────────────────
total = 0
fraud = 0

try:
    for message in consumer:
        data = message.value
        total += 1

        pred  = data.get("prediction", 0)
        proba = data.get("probability_fraud", 0.0)
        label = data.get("label", "?")

        if pred == 1:
            fraud += 1
            colour = RED
            icon   = "🚨"
        else:
            colour = GREEN
            icon   = "✅"

        # ── Pretty print ──────────────────────────────────────────────────
        print(
            f"{colour}{BOLD}{icon}  [{label}]{RESET}  "
            f"event_id={data.get('event_id','?')[:12]}…  "
            f"prob_fraud={proba:.4f}  "
            f"│  total={total}  fraud={fraud} ({fraud/total*100:.1f}%)"
        )

except KeyboardInterrupt:
    print(f"\n[Consumer] Stopped. Total: {total}  Fraud: {fraud}")
finally:
    consumer.close()
