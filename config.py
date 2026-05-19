import os

BOOTSTRAP_SERVERS = os.getenv(
    "KAFKA_BOOTSTRAP_SERVERS",
    "pkc-921jm.us-east-2.aws.confluent.cloud:9092",
)

API_KEY = os.getenv(
    "KAFKA_API_KEY",
    "I2SROFHVALAHQESH",
)

API_SECRET = os.getenv(
    "KAFKA_API_SECRET",
    "cfltbftmn5UIXgyGe8rgf/gpCi3uv1dK96PtWeLAZ4wsZ4lvHA4xzJhUZlOYI01g",
)

RAW_TOPIC         = "raw-data"
PREDICTIONS_TOPIC = "predictions"
