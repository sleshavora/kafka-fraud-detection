"""
producer.py
-----------
Reads rows from creditcard.csv and publishes each one as a JSON message
to the Kafka topic  'raw-data'  at ~1 row/second.

Usage:
    python producer.py [--speed 1.0] [--topic raw-data] [--rows 500]

Environment variables (or edit config.py):
    KAFKA_BOOTSTRAP_SERVERS  e.g. pkc-xxxxx.us-east-1.aws.confluent.cloud:9092
    KAFKA_API_KEY
    KAFKA_API_SECRET
"""

import argparse
import csv
import json
import os
import sys
import time
import uuid
from datetime import datetime

from kafka import KafkaProducer
from kafka.errors import KafkaError

import config

# ── CLI ───────────────────────────────────────────────────────────────────────
parser = argparse.ArgumentParser(description="Credit Card Fraud – Kafka Producer")
parser.add_argument("--speed",  type=float, default=1.0,    help="Seconds between messages (default 1.0)")
parser.add_argument("--topic",  type=str,   default="raw-data", help="Kafka topic name")
parser.add_argument("--rows",   type=int,   default=500,    help="Max rows to send (0 = all)")
parser.add_argument("--data",   type=str,   default=os.path.join("data", "creditcard.csv"), help="Path to CSV")
args = parser.parse_args()

# ── Kafka producer ────────────────────────────────────────────────────────────
print(f"[Producer] Connecting to {config.BOOTSTRAP_SERVERS} …")
producer = KafkaProducer(
    bootstrap_servers=config.BOOTSTRAP_SERVERS,
    security_protocol="SASL_SSL",
    sasl_mechanism="PLAIN",
    sasl_plain_username=config.API_KEY,
    sasl_plain_password=config.API_SECRET,
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    key_serializer=lambda k: k.encode("utf-8") if k else None,
    acks="all",
    retries=3,
)
print(f"[Producer] Connected. Streaming to topic '{args.topic}' …\n")

# ── Stream rows ───────────────────────────────────────────────────────────────
def on_send_error(exc):
    print(f"[Producer] ERROR: {exc}", file=sys.stderr)

sent = 0
try:
    with open(args.data, newline="") as csvfile:
        reader = csv.DictReader(csvfile)
        for row in reader:
            if args.rows and sent >= args.rows:
                break

            # Build message payload
            payload = {
                "event_id":  str(uuid.uuid4()),
                "timestamp": datetime.utcnow().isoformat(),
                "features":  {k: float(v) for k, v in row.items() if k != "Class"},
                "label":     int(row["Class"]),   # kept for evaluation; processor ignores it
            }

            future = producer.send(
                args.topic,
                key=payload["event_id"],
                value=payload,
            )
            future.add_errback(on_send_error)

            sent += 1
            fraud_tag = " ⚠️  FRAUD" if payload["label"] == 1 else ""
            print(f"[Producer] Sent #{sent:>5}  event_id={payload['event_id'][:8]}…{fraud_tag}")
            time.sleep(args.speed)

except KeyboardInterrupt:
    print("\n[Producer] Interrupted by user.")
finally:
    producer.flush()
    producer.close()
    print(f"\n[Producer] Done. {sent} messages sent.")
