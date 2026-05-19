"""
app.py  –  Faust Streams Processor
------------------------------------
Consumes 'raw-data', runs the pre-trained Random Forest model on every
record, and produces a prediction message to the 'predictions' topic.

Start with:
    faust -A app worker -l info

Environment / config.py must have KAFKA_* credentials set.
"""

import json
import os
from datetime import datetime
from typing import Optional

import faust
import joblib
import numpy as np

import config

# ── Load model artefacts (once, at startup) ───────────────────────────────────
MODEL_DIR   = os.path.join(os.path.dirname(__file__), "model")
MODEL_PATH  = os.path.join(MODEL_DIR, "fraud_model.joblib")
FEATURE_PATH = os.path.join(MODEL_DIR, "feature_cols.txt")

print("[Processor] Loading model …")
clf = joblib.load(MODEL_PATH)

with open(FEATURE_PATH) as f:
    FEATURE_COLS = [line.strip() for line in f if line.strip()]

print(f"[Processor] Model loaded. Features: {len(FEATURE_COLS)}")

# ── Faust app ─────────────────────────────────────────────────────────────────
app = faust.App(
    "fraud-detector",
    broker=f"kafka://{config.BOOTSTRAP_SERVERS}",
    broker_credentials=faust.SASLCredentials(
        username=config.API_KEY,
        password=config.API_SECRET,
        ssl_context=None,          # Faust uses SSL for SASL_SSL automatically
        mechanism="PLAIN",
    ),
    value_serializer="json",
    consumer_auto_offset_reset="earliest",
)

# ── Topic definitions ─────────────────────────────────────────────────────────
class RawEvent(faust.Record, serializer="json"):
    event_id: str
    timestamp: str
    features: dict
    label: Optional[int] = None   # ground truth, ignored by model


class Prediction(faust.Record, serializer="json"):
    event_id: str
    received_at: str
    predicted_at: str
    prediction: int          # 0 = Legit, 1 = Fraud
    probability_fraud: float
    label: str               # human-readable


raw_topic         = app.topic("raw-data",   value_type=RawEvent)
predictions_topic = app.topic("predictions", value_type=Prediction)

# ── Running stats table (Faust stateful table) ────────────────────────────────
stats = app.Table("stats", default=int)

# ── Agent (Kafka Streams equivalent) ─────────────────────────────────────────
@app.agent(raw_topic)
async def detect_fraud(events):
    async for event in events:
        # ── Build feature vector in the correct column order ──────────────
        feature_vec = np.array(
            [event.features.get(col, 0.0) for col in FEATURE_COLS],
            dtype=np.float64,
        ).reshape(1, -1)

        # ── Predict ───────────────────────────────────────────────────────
        pred  = int(clf.predict(feature_vec)[0])
        proba = float(clf.predict_proba(feature_vec)[0][1])

        # ── Update counters ───────────────────────────────────────────────
        stats["total"]      += 1
        stats["fraud"]      += pred
        stats["legit"]      += (1 - pred)

        # ── Build output message ──────────────────────────────────────────
        result = Prediction(
            event_id         = event.event_id,
            received_at      = event.timestamp,
            predicted_at     = datetime.utcnow().isoformat(),
            prediction       = pred,
            probability_fraud = proba,
            label            = "FRAUD" if pred == 1 else "Legit",
        )

        await predictions_topic.send(key=event.event_id, value=result)

        flag = "🚨 FRAUD DETECTED" if pred == 1 else "✅ Legit"
        print(
            f"[Processor] {flag} | "
            f"event={event.event_id[:8]}… | "
            f"prob={proba:.4f} | "
            f"total={stats['total']} fraud={stats['fraud']}"
        )


# ── Optional: periodic stats log ─────────────────────────────────────────────
@app.timer(interval=30.0)
async def log_stats():
    total = stats["total"]
    fraud = stats["fraud"]
    if total:
        print(
            f"\n[Processor] ── 30s Summary ── "
            f"Total: {total} | Fraud: {fraud} ({fraud/total*100:.1f}%) | "
            f"Legit: {stats['legit']}\n"
        )
