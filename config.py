"""
config.py  –  Kafka / Confluent Cloud credentials
--------------------------------------------------
Fill in your own values before running any component.

Option 1 – edit this file directly (never commit real secrets to GitHub).
Option 2 – set environment variables; this file reads them automatically.

How to get these values:
  1. Log in to Confluent Cloud → select your cluster.
  2. Click  API Keys  → Create Key (Cluster scope).
  3. Copy the key and secret here.
  4. Copy the Bootstrap server URL from  Cluster Settings → Endpoints.
"""

import os

# ── Confluent Cloud settings ──────────────────────────────────────────────────
BOOTSTRAP_SERVERS = os.getenv(
    "KAFKA_BOOTSTRAP_SERVERS",
    "pkc-XXXXXX.us-east-1.aws.confluent.cloud:9092",   # ← replace
)

API_KEY = os.getenv(
    "KAFKA_API_KEY",
    "YOUR_API_KEY",                                     # ← replace
)

API_SECRET = os.getenv(
    "KAFKA_API_SECRET",
    "YOUR_API_SECRET",                                  # ← replace
)

# ── Topic names (change only if you rename them) ──────────────────────────────
RAW_TOPIC         = "raw-data"
PREDICTIONS_TOPIC = "predictions"
