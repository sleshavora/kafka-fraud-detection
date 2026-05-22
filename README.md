# Real-Time Credit Card Fraud Detection with Apache Kafka + Faust

> **ENGR 5785G – Assignment 1 · Real-Time Streaming**

A complete end-to-end streaming pipeline that ingests credit card transactions one row at a second, runs a pre-trained Random Forest classifier in real time using **Faust** (Python Kafka Streams), and prints live predictions to an output console.

---

## Architecture

```
creditcard.csv
     │  (1 row/sec)
     ▼
┌─────────────┐   raw-data topic   ┌──────────────────────┐   predictions topic   ┌──────────────────┐
│  producer.py │ ─────────────────▶ │      app.py (Faust)   │ ────────────────────▶ │  consumer.py      │
│  KafkaProducer│                   │  @agent → RF model    │                       │  KafkaConsumer   │
└─────────────┘                    └──────────────────────┘                       └──────────────────┘
```

---

## Dataset

| Field | Value |
|-------|-------|
| **Name** | Credit Card Fraud Detection |
| **Source** | [Kaggle – mlg-ulb/creditcardfraud](https://www.kaggle.com/datasets/mlg-ulb/creditcardfraud) |
| **ML Task** | Binary classification – flag fraudulent transactions |
| **Size** | 284,807 transactions, 492 fraud (0.17%) |
| **Features** | V1–V28 (PCA), Time, Amount |

Download `creditcard.csv` from Kaggle and place it in the `data/` folder.

---

## ML Model

| Metric | Value |
|--------|-------|
| **Algorithm** | Random Forest (100 trees, max_depth=12, class_weight=balanced) |
| **Accuracy** | 99.94% |
| **F1 Score (Fraud class)** | 0.8187 |
| **ROC-AUC** | 0.9753 |

> Class imbalance is handled via `class_weight="balanced"` in scikit-learn.  
> Model artefacts are saved to `model/fraud_model.joblib` and `model/scaler.joblib`.

---

## Project Structure

```
kafka-fraud-detection/
├── producer.py          # Reads CSV → publishes to 'raw-data'
├── app.py               # Faust agent: consumes 'raw-data' → predicts → 'predictions'
├── consumer.py          # Reads 'predictions' → pretty-prints to console
├── config.py            # Kafka credentials (edit before running)
├── requirements.txt
├── data/
│   └── creditcard.csv   # ← download from Kaggle (not tracked in git)
└── model/
    ├── train_model.py       # Offline training script
    ├── fraud_model.joblib   # Trained Random Forest
    ├── scaler.joblib        # StandardScaler for Amount/Time
    └── feature_cols.txt     # Ordered feature names
```

---

## Setup

### 1. Prerequisites

- Python 3.10+
- A [Confluent Cloud](https://confluent.cloud) account (free tier works)
- The `creditcard.csv` dataset downloaded from Kaggle

### 2. Install dependencies

```bash
pip install -r requirements.txt
```

### 3. Create Kafka topics in Confluent Cloud

Log in to Confluent Cloud and create two topics:

| Topic | Partitions |
|-------|-----------|
| `raw-data` | 1 |
| `predictions` | 1 |

### 4. Configure credentials

Edit `config.py` and fill in your values:

```python
BOOTSTRAP_SERVERS = "pkc-XXXXXX.us-east-1.aws.confluent.cloud:9092"
API_KEY           = "YOUR_API_KEY"
API_SECRET        = "YOUR_API_SECRET"
```

Or export environment variables:

```bash
export KAFKA_BOOTSTRAP_SERVERS="pkc-XXXXXX..."
export KAFKA_API_KEY="..."
export KAFKA_API_SECRET="..."
```

### 5. Place the dataset

```bash
mkdir -p data
cp ~/Downloads/creditcard.csv data/
```

### 6. Train the model (one-time)

```bash
python model/train_model.py
```

This creates `model/fraud_model.joblib`, `model/scaler.joblib`, and `model/feature_cols.txt`.

---

## Running the Pipeline

Open **three separate terminals** side by side.

### Terminal 1 – Streams Processor (start first)

```bash
faust -A app worker -l info
```

Faust connects to Confluent Cloud and waits for messages.

### Terminal 2 – Output Consumer

```bash
python consumer.py
```

Connects and waits for predictions to arrive.

### Terminal 3 – Producer

```bash
python producer.py --speed 1.0 --rows 300
```

Streams 300 rows at 1 row/second. Watch Terminals 1 and 2 light up!

#### Optional producer flags

| Flag | Default | Description |
|------|---------|-------------|
| `--speed` | `1.0` | Seconds between messages |
| `--rows` | `500` | Max rows to send (0 = all) |
| `--topic` | `raw-data` | Override topic name |
| `--data` | `data/creditcard.csv` | Path to CSV |

---

## Streams API Usage

This project uses **Faust** (the Python equivalent of Kafka Streams).  
The processor defines a Faust *agent* (analogous to a Kafka Streams `KStream.mapValues()` topology):

```python
@app.agent(raw_topic)
async def detect_fraud(events):
    async for event in events:
        # Load features → predict → publish to predictions topic
        ...
```

A Faust *table* tracks running fraud/legit counts as stateful in-memory state (equivalent to a Kafka Streams `KTable`).

---

## Video Demo

📹 **(https://drive.google.com/file/d/10HxEmoI4ui6gJsA-45Vp0DpI4pjMa81v/view?usp=sharing)*


---

## Notes

- `creditcard.csv` is excluded from this repo via `.gitignore` (Kaggle terms of service). Download it directly from Kaggle.
- Model files (`.joblib`) **are** committed so the processor works without re-training.
- Real secrets should never be committed — use environment variables in production.
