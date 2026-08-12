import json
import time
from kafka import KafkaProducer

KAFKA_BROKER = "localhost:9092"
TOPIC = "socialdata"
DATA_FILE = r"..\data\raw\ai_tweets.json"

producer = KafkaProducer(
    bootstrap_servers=KAFKA_BROKER,
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

with open(DATA_FILE, "r", encoding="utf-8") as f:
    tweets = json.load(f)

print(f"Loaded {len(tweets)} tweets.")

for i, tweet in enumerate(tweets, start=1):
    producer.send(TOPIC, value=tweet)

    if i % 100 == 0:
        producer.flush()
        print(f"Sent {i} tweets")

    time.sleep(0.01)

producer.flush()
producer.close()

print("All tweets sent successfully.")