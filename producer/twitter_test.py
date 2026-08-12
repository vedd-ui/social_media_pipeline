import os
import tweepy
from dotenv import load_dotenv

load_dotenv()

bearer_token = os.getenv("X_BEARER_TOKEN")

if not bearer_token:
    raise ValueError("X_BEARER_TOKEN not found in .env")

client = tweepy.Client(bearer_token=bearer_token)

response = client.search_recent_tweets(
    query="AI -is:retweet lang:en",
    max_results=10
)

if response.data:
    print(f"Tweets received: {len(response.data)}")

    for tweet in response.data:
        print("\n---")
        print(tweet.text)
else:
    print("No tweets received.")