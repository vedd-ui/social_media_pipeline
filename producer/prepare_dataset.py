import pandas as pd
import json
import os

# Input dataset
input_file = r"C:\Users\Ved\Downloads\chatgpt1.csv"

# Output location
output_file = r"D:\SocialMediaPipeline\data\raw\ai_tweets.json"

# Read dataset
df = pd.read_csv(input_file)

# Keep English tweets with actual text
df = df[
    (df["Language"] == "en") &
    (df["Text"].notna()) &
    (df["Text"].str.strip() != "")
]

# Select exactly 1,000 records
df = df.head(1000)

# Convert required fields to our pipeline format
tweets = []

for _, row in df.iterrows():
    tweets.append({
        "tweet_id": str(row["Tweet Id"]),
        "username": str(row["Username"]),
        "text": str(row["Text"]),
        "timestamp": str(row["Datetime"]),
        "language": str(row["Language"]),
        "hashtags": row["hashtag"] if isinstance(row["hashtag"], list) else str(row["hashtag"])
    })

# Save as JSON
os.makedirs(os.path.dirname(output_file), exist_ok=True)

with open(output_file, "w", encoding="utf-8") as f:
    json.dump(tweets, f, ensure_ascii=False, indent=2)

print(f"Successfully created {len(tweets)} tweets")
print(f"Output: {output_file}")