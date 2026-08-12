from pyspark.sql.functions import regexp_extract_all
from pyspark.sql.functions import col, sum, when
from pyspark.sql.functions import regexp_replace, trim, from_json, lower, expr, concat_ws
from pyspark.sql.types import ArrayType, StringType
from pyspark.sql.functions import explode, count
from pyspark.sql import SparkSession
from pyspark.sql.functions import hour, to_date, count, length, avg, min, max, when, col, size, udf, round, explode, hour
from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer
import sys
import os

os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

JDBC_DRIVER = r"D:\SocialMediaPipeline\spark\jars\postgresql-42.7.13.jar"

python_path = sys.executable

spark = (
    SparkSession.builder
    .appName("PostgreSQLConnectionTest")
    .master("local[*]")
    .config("spark.driver.host", "127.0.0.1")
.config("spark.driver.bindAddress", "127.0.0.1")
    .config("spark.python.worker.reuse", "true")
    .config("spark.python.worker.timeout", "120")
    .config(
        "spark.jars",
        r"D:\SocialMediaPipeline\spark\jars\postgresql-42.7.13.jar"
    )
    .config(
        "spark.driver.extraClassPath",
        r"D:\SocialMediaPipeline\spark\jars\postgresql-42.7.13.jar"
    )
    .config(
        "spark.driver.extraJavaOptions",
        "-Duser.timezone=Asia/Kolkata"
    )
    .getOrCreate()
)

jdbc_url = "jdbc:postgresql://127.0.0.1:5432/socialmedia"
properties = {
    "user": "postgres",
    "password": "postgres",
    "driver": "org.postgresql.Driver"
}

df = spark.read.jdbc(
    url=jdbc_url,
    table="tweets",
    properties=properties
)

print("Total records:", df.count())

print("\nSchema:")
df.printSchema()

print("\nFirst 5 records:")
df.show(5, truncate=False)

print("\nMissing values:")
df.select(
    *[
        sum(when(col(c).isNull(), 1).otherwise(0)).alias(c)
        for c in df.columns
    ]
).show()

print("\nDuplicate tweet IDs:")
print(df.count() - df.dropDuplicates(["tweet_id"]).count())

print("\nLanguages:")
df.groupBy("language").count().show()

print("\nEmpty text:")
df.filter(
    col("text").isNull() | (col("text") == "")
).count()

print("\nColumn names:")
print(df.columns)

clean_df = df.withColumn(
    "clean_text",
    regexp_replace("text", r"https?://\S+|www\.\S+", "")
)

clean_df = clean_df.withColumn(
    "clean_text",
    trim(regexp_replace("clean_text", r"\s+", " "))
)

print("\nOriginal vs Cleaned:")
clean_df.select(
    "text",
    "clean_text"
).show(5, truncate=False)

print("\nTweets containing Markdown links:")
print(
    df.filter(
        col("text").contains("](")
    ).count()
)

print("\nURLs remaining after cleaning:")
clean_df.filter(
    col("clean_text").rlike(r"https?://|www\.")
).select(
    "text",
    "clean_text"
).show(10, truncate=False)

print("\nCleaned records:", clean_df.count())


print("\nTweets containing mentions:")
print(
    clean_df.filter(
        col("clean_text").rlike(r"@\w+")
    ).count()
)

print("\nTweets containing hashtags:")
print(
    clean_df.filter(
        col("clean_text").rlike(r"#\w+")
    ).count()
)

print("\nHashtag examples:")
clean_df.select("hashtags").filter(
    col("hashtags") != "[]"
).show(10, truncate=False)


clean_df = clean_df.withColumn(
    "hashtags_array",
    from_json(
        regexp_replace(col("hashtags"), "'", '"'),
        ArrayType(StringType())
    )
)

clean_df = clean_df.withColumn(
    "hashtags_array",
    expr("""
        transform(
            hashtags_array,
            x -> lower(regexp_replace(x, '[^a-zA-Z0-9#]', ''))
        )
    """)
)
hashtag_counts = (
    clean_df
    .select(explode("hashtags_array").alias("hashtag"))
    .groupBy("hashtag")
    .count()
    .orderBy("count", ascending=False)
)

print("\nTop 20 hashtags:")
hashtag_counts.show(20, truncate=False)

hourly_tweets = (
    clean_df
    .withColumn("hour", hour("timestamp"))
    .groupBy("hour")
    .count()
    .orderBy("hour")
)

print("\nTweets by hour:")
hourly_tweets.show(24)

daily_tweets = (
    clean_df
    .withColumn("date", to_date("timestamp"))
    .groupBy("date")
    .count()
    .orderBy("date")
)

print("\nTweets by date:")
daily_tweets.show()

top_users = (
    clean_df
    .groupBy("username")
    .count()
    .orderBy("count", ascending=False)
)

print("\nTop 20 users by tweet count:")
top_users.show(20, truncate=False)

unique_users = clean_df.select("username").distinct().count()
print("\nUnique users:", unique_users)

tweet_length_stats = clean_df.select(
    avg(length("clean_text")).alias("average_length"),
    min(length("clean_text")).alias("minimum_length"),
    max(length("clean_text")).alias("maximum_length")
)

print("\nTweet length statistics:")
tweet_length_stats.show()
tweet_length_categories = (
    clean_df
    .withColumn(
        "length_category",
        when(length("clean_text") < 50, "Short")
        .when(length("clean_text") <= 150, "Medium")
        .otherwise("Long")
    )
    .groupBy("length_category")
    .count()
    .orderBy("count", ascending=False)
)

print("\nTweet length categories:")
tweet_length_categories.show()
mention_analysis = (
    clean_df
    .withColumn(
        "has_mention",
        when(col("text").contains("@"), "With Mention")
        .otherwise("Without Mention")
    )
    .groupBy("has_mention")
    .count()
    .orderBy("count", ascending=False)
)

print("\nTweets with vs without mentions:")
mention_analysis.show()

hashtag_analysis = (
    clean_df
    .withColumn(
        "has_hashtag",
        when(col("hashtags_array").isNotNull() & (size("hashtags_array") > 0),
             "With Hashtag")
        .otherwise("Without Hashtag")
    )
    .groupBy("has_hashtag")
    .count()
    .orderBy("count", ascending=False)
)

print("\nTweets with vs without hashtags:")
hashtag_analysis.show()

analyzer = SentimentIntensityAnalyzer()

def get_sentiment(text):
    score = analyzer.polarity_scores(text)["compound"]

    if score >= 0.05:
        return "Positive"
    elif score <= -0.05:
        return "Negative"
    else:
        return "Neutral"

sentiment_udf = udf(get_sentiment, StringType())

sentiment_df = clean_df.withColumn(
    "sentiment",
    sentiment_udf("clean_text")
)

print("\nSentiment examples:")
sentiment_df.select(
    "clean_text",
    "sentiment"
).show(10, truncate=False)

print("Sentiment distribution:")

sentiment_df.groupBy("sentiment") \
    .count() \
    .orderBy("count", ascending=False) \
    .show()

total = sentiment_df.count()

print("\nSentiment distribution with percentages:")

sentiment_df.groupBy("sentiment") \
    .count() \
    .withColumn(
        "percentage",
        round(col("count") / total * 100, 2)
    ) \
    .orderBy(col("count").desc()) \
    .show()

print("\nSentiment by hashtag:")

sentiment_df \
    .select(explode("hashtags_array").alias("hashtag"), "sentiment") \
    .groupBy("hashtag", "sentiment") \
    .count() \
    .orderBy(col("count").desc()) \
    .show(20, truncate=False)

print("\nSentiment by hour:")

sentiment_df \
    .withColumn("hour", hour("timestamp")) \
    .groupBy("hour", "sentiment") \
    .count() \
    .orderBy("hour", col("count").desc()) \
    .show(20)

final_df = sentiment_df.select(
    "tweet_id",
    "username",
    "text",
    "clean_text",
    "timestamp",
    "language",
    "hashtags_array",
    "sentiment"
)

print("\nFinal processed DataFrame:")
final_df.printSchema()
final_df.show(5, truncate=False)

# Convert hashtag array to a PostgreSQL-friendly string
final_df = final_df.withColumn(
    "hashtags",
    concat_ws(",", "hashtags_array")
).drop("hashtags_array")

print("\nFinal DataFrame before PostgreSQL:")
final_df.printSchema()
print("\nWriting processed data to PostgreSQL...")

final_df.write \
    .jdbc(
        url=jdbc_url,
        table="processed_tweets",
        mode="overwrite",
        properties=properties
    )

print("Successfully written to PostgreSQL!")
spark.stop()