import json
import psycopg2
from kafka import KafkaConsumer
import os
from dotenv import load_dotenv

load_dotenv()

KAFKA_BROKER = "localhost:9092"
TOPIC = "socialdata"

DB_HOST = os.getenv("POSTGRES_HOST")
DB_PORT = os.getenv("POSTGRES_PORT")
DB_NAME = os.getenv("POSTGRES_DB")
DB_USER = os.getenv("POSTGRES_USER")
DB_PASSWORD = os.getenv("POSTGRES_PASSWORD")

consumer = KafkaConsumer(
    TOPIC,
    bootstrap_servers=KAFKA_BROKER,
    auto_offset_reset="earliest",
    enable_auto_commit=True,
    group_id="socialmedia-consumer",
    value_deserializer=lambda x: json.loads(x.decode("utf-8"))
)

conn = psycopg2.connect(
    host=DB_HOST,
    port=DB_PORT,
    database=DB_NAME,
    user=DB_USER,
    password=DB_PASSWORD
)

cursor = conn.cursor()

print("Consumer started...")
print("Waiting for tweets from Kafka...")

count = 0

for message in consumer:
    tweet = message.value

    try:
        cursor.execute(
            """
            INSERT INTO tweets
            (tweet_id, username, text, timestamp, language, hashtags)
            VALUES (%s, %s, %s, %s, %s, %s)
            ON CONFLICT (tweet_id) DO NOTHING
            """,
            (
                tweet["tweet_id"],
                tweet["username"],
                tweet["text"],
                tweet["timestamp"],
                tweet["language"],
                tweet["hashtags"]
            )
        )

        conn.commit()

        count += 1

        if count % 100 == 0:
            print(f"Inserted {count} tweets into PostgreSQL")

    except Exception as e:
        conn.rollback()
        print("Error:", e)