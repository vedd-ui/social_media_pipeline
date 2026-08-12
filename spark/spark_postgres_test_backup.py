# ============================================================
# SOCIAL MEDIA DATA PIPELINE USING PYSPARK + POSTGRESQL
# ============================================================

# ------------------------------------------------------------
# 1. IMPORTS
# ------------------------------------------------------------

import os
import sys

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    sum,
    when,
    regexp_replace,
    trim,
    from_json,
    lower,
    expr,
    concat_ws,
    explode,
    count,
    hour,
    to_date,
    length,
    avg,
    min,
    max,
    size,
    udf,
    round
)
from pyspark.sql.types import ArrayType, StringType

from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer


# ------------------------------------------------------------
# 2. PYTHON CONFIGURATION FOR SPARK WORKERS
# ------------------------------------------------------------

os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable


# ------------------------------------------------------------
# 3. SPARK SESSION CONFIGURATION
# ------------------------------------------------------------

JDBC_DRIVER = r"D:\SocialMediaPipeline\spark\jars\postgresql-42.7.13.jar"

spark = (
    SparkSession.builder
    .appName("PostgreSQLConnectionTest")
    .master("local[*]")
    .config("spark.driver.host", "127.0.0.1")
    .config("spark.driver.bindAddress", "127.0.0.1")
    .config("spark.python.worker.reuse", "true")
    .config("spark.python.worker.timeout", "120")
    .config("spark.jars", JDBC_DRIVER)
    .config("spark.driver.extraClassPath", JDBC_DRIVER)
    .config(
        "spark.driver.extraJavaOptions",
        "-Duser.timezone=Asia/Kolkata"
    )
    .getOrCreate()
)


# ------------------------------------------------------------
# 4. POSTGRESQL CONNECTION
# ------------------------------------------------------------

jdbc_url = "jdbc:postgresql://127.0.0.1:5432/socialmedia"

properties = {
    "user": "postgres",
    "password": "postgres",
    "driver": "org.postgresql.Driver"
}


# ------------------------------------------------------------
# 5. LOAD DATA FROM POSTGRESQL
# ------------------------------------------------------------

df = spark.read.jdbc(
    url=jdbc_url,
    table="tweets",
    properties=properties
)


# ------------------------------------------------------------
# 6. BASIC DATA VALIDATION
# ------------------------------------------------------------

print("Total records:", df.count())

print("\nSchema:")
df.printSchema()

print("\nFirst 5 records:")
df.show(5, truncate=False)


# ------------------------------------------------------------
# 7. MISSING VALUES
# ------------------------------------------------------------

print("\nMissing values:")

df.select(
    *[
        sum(
            when(col(c).isNull(), 1).otherwise(0)
        ).alias(c)
        for c in df.columns
    ]
).show()


# ------------------------------------------------------------
# 8. DUPLICATE TWEET IDs
# ------------------------------------------------------------

print("\nDuplicate tweet IDs:")

duplicate_count = (
    df.count()
    - df.dropDuplicates(["tweet_id"]).count()
)

print(duplicate_count)


# ------------------------------------------------------------
# 9. LANGUAGE DISTRIBUTION
# ------------------------------------------------------------

print("\nLanguages:")

df.groupBy("language") \
    .count() \
    .show()


# ------------------------------------------------------------
# 10. EMPTY TEXT CHECK
# ------------------------------------------------------------

print("\nEmpty text:")

empty_text_count = df.filter(
    col("text").isNull() | (col("text") == "")
).count()

print(empty_text_count)


# ------------------------------------------------------------
# 11. COLUMN NAMES
# ------------------------------------------------------------

print("\nColumn names:")
print(df.columns)


# ============================================================
# TEXT CLEANING
# ============================================================

# ------------------------------------------------------------
# 12. REMOVE URLs
# ------------------------------------------------------------

clean_df = df.withColumn(
    "clean_text",
    regexp_replace(
        "text",
        r"https?://\S+|www\.\S+",
        ""
    )
)


# ------------------------------------------------------------
# 13. NORMALIZE WHITESPACE
# ------------------------------------------------------------

clean_df = clean_df.withColumn(
    "clean_text",
    trim(
        regexp_replace(
            "clean_text",
            r"\s+",
            " "
        )
    )
)


# ------------------------------------------------------------
# 14. ORIGINAL VS CLEANED TEXT
# ------------------------------------------------------------

print("\nOriginal vs Cleaned:")

clean_df.select(
    "text",
    "clean_text"
).show(5, truncate=False)


# ------------------------------------------------------------
# 15. MARKDOWN LINK CHECK
# ------------------------------------------------------------

print("\nTweets containing Markdown links:")

markdown_links = df.filter(
    col("text").contains("](")
).count()

print(markdown_links)


# ------------------------------------------------------------
# 16. CHECK FOR REMAINING URLs
# ------------------------------------------------------------

print("\nURLs remaining after cleaning:")

clean_df.filter(
    col("clean_text").rlike(r"https?://|www\.")
).select(
    "text",
    "clean_text"
).show(10, truncate=False)


print("\nCleaned records:", clean_df.count())


# ============================================================
# MENTIONS AND HASHTAGS
# ============================================================

# ------------------------------------------------------------
# 17. MENTION ANALYSIS
# ------------------------------------------------------------

print("\nTweets containing mentions:")

mention_count = clean_df.filter(
    col("clean_text").rlike(r"@\w+")
).count()

print(mention_count)


# ------------------------------------------------------------
# 18. HASHTAG ANALYSIS
# ------------------------------------------------------------

print("\nTweets containing hashtags:")

hashtag_count = clean_df.filter(
    col("clean_text").rlike(r"#\w+")
).count()

print(hashtag_count)


# ------------------------------------------------------------
# 19. HASHTAG EXAMPLES
# ------------------------------------------------------------

print("\nHashtag examples:")

clean_df.select("hashtags") \
    .filter(col("hashtags") != "[]") \
    .show(10, truncate=False)


# ------------------------------------------------------------
# 20. CONVERT HASHTAGS TO SPARK ARRAY
# ------------------------------------------------------------

clean_df = clean_df.withColumn(
    "hashtags_array",
    from_json(
        regexp_replace(
            col("hashtags"),
            "'",
            '"'
        ),
        ArrayType(StringType())
    )
)


# ------------------------------------------------------------
# 21. NORMALIZE HASHTAGS
# ------------------------------------------------------------

clean_df = clean_df.withColumn(
    "hashtags_array",
    expr("""
        transform(
            hashtags_array,
            x -> lower(
                regexp_replace(
                    x,
                    '[^a-zA-Z0-9#]',
                    ''
                )
            )
        )
    """)
)


# ------------------------------------------------------------
# 22. TOP HASHTAGS
# ------------------------------------------------------------

hashtag_counts = (
    clean_df
    .select(
        explode("hashtags_array").alias("hashtag")
    )
    .groupBy("hashtag")
    .count()
    .orderBy("count", ascending=False)
)

print("\nTop 20 hashtags:")
hashtag_counts.show(20, truncate=False)


# ============================================================
# TIME ANALYSIS
# ============================================================

# ------------------------------------------------------------
# 23. TWEETS BY HOUR
# ------------------------------------------------------------

hourly_tweets = (
    clean_df
    .withColumn("hour", hour("timestamp"))
    .groupBy("hour")
    .count()
    .orderBy("hour")
)

print("\nTweets by hour:")
hourly_tweets.show(24)


# ------------------------------------------------------------
# 24. TWEETS BY DATE
# ------------------------------------------------------------

daily_tweets = (
    clean_df
    .withColumn("date", to_date("timestamp"))
    .groupBy("date")
    .count()
    .orderBy("date")
)

print("\nTweets by date:")
daily_tweets.show()


# ============================================================
# USER ANALYSIS
# ============================================================

# ------------------------------------------------------------
# 25. TOP USERS
# ------------------------------------------------------------

top_users = (
    clean_df
    .groupBy("username")
    .count()
    .orderBy("count", ascending=False)
)

print("\nTop 20 users by tweet count:")
top_users.show(20, truncate=False)


# ------------------------------------------------------------
# 26. UNIQUE USERS
# ------------------------------------------------------------

unique_users = (
    clean_df
    .select("username")
    .distinct()
    .count()
)

print("\nUnique users:", unique_users)


# ============================================================
# TWEET LENGTH ANALYSIS
# ============================================================

# ------------------------------------------------------------
# 27. TWEET LENGTH STATISTICS
# ------------------------------------------------------------

tweet_length_stats = clean_df.select(
    avg(length("clean_text")).alias("average_length"),
    min(length("clean_text")).alias("minimum_length"),
    max(length("clean_text")).alias("maximum_length")
)

print("\nTweet length statistics:")
tweet_length_stats.show()


# ------------------------------------------------------------
# 28. TWEET LENGTH CATEGORIES
# ------------------------------------------------------------

tweet_length_categories = (
    clean_df
    .withColumn(
        "length_category",
        when(
            length("clean_text") < 50,
            "Short"
        )
        .when(
            length("clean_text") <= 150,
            "Medium"
        )
        .otherwise("Long")
    )
    .groupBy("length_category")
    .count()
    .orderBy("count", ascending=False)
)

print("\nTweet length categories:")
tweet_length_categories.show()


# ============================================================
# MENTION AND HASHTAG PRESENCE
# ============================================================

# ------------------------------------------------------------
# 29. WITH VS WITHOUT MENTIONS
# ------------------------------------------------------------

mention_analysis = (
    clean_df
    .withColumn(
        "has_mention",
        when(
            col("text").contains("@"),
            "With Mention"
        )
        .otherwise("Without Mention")
    )
    .groupBy("has_mention")
    .count()
    .orderBy("count", ascending=False)
)

print("\nTweets with vs without mentions:")
mention_analysis.show()


# ------------------------------------------------------------
# 30. WITH VS WITHOUT HASHTAGS
# ------------------------------------------------------------

hashtag_analysis = (
    clean_df
    .withColumn(
        "has_hashtag",
        when(
            col("hashtags_array").isNotNull()
            & (size("hashtags_array") > 0),
            "With Hashtag"
        )
        .otherwise("Without Hashtag")
    )
    .groupBy("has_hashtag")
    .count()
    .orderBy("count", ascending=False)
)

print("\nTweets with vs without hashtags:")
hashtag_analysis.show()


# ============================================================
# SENTIMENT ANALYSIS
# ============================================================

# ------------------------------------------------------------
# 31. INITIALIZE VADER
# ------------------------------------------------------------

analyzer = SentimentIntensityAnalyzer()


# ------------------------------------------------------------
# 32. SENTIMENT FUNCTION
# ------------------------------------------------------------

def get_sentiment(text):
    score = analyzer.polarity_scores(text)["compound"]

    if score >= 0.05:
        return "Positive"

    elif score <= -0.05:
        return "Negative"

    else:
        return "Neutral"


# ------------------------------------------------------------
# 33. CREATE SENTIMENT UDF
# ------------------------------------------------------------

sentiment_udf = udf(
    get_sentiment,
    StringType()
)


# ------------------------------------------------------------
# 34. APPLY SENTIMENT ANALYSIS
# ------------------------------------------------------------

sentiment_df = clean_df.withColumn(
    "sentiment",
    sentiment_udf("clean_text")
)


# ------------------------------------------------------------
# 35. SENTIMENT EXAMPLES
# ------------------------------------------------------------

print("\nSentiment examples:")

sentiment_df.select(
    "clean_text",
    "sentiment"
).show(10, truncate=False)


# ------------------------------------------------------------
# 36. SENTIMENT DISTRIBUTION
# ------------------------------------------------------------

print("\nSentiment distribution:")

sentiment_df \
    .groupBy("sentiment") \
    .count() \
    .orderBy("count", ascending=False) \
    .show()


# ------------------------------------------------------------
# 37. SENTIMENT PERCENTAGES
# ------------------------------------------------------------

total = sentiment_df.count()

print("\nSentiment distribution with percentages:")

sentiment_df \
    .groupBy("sentiment") \
    .count() \
    .withColumn(
        "percentage",
        round(
            col("count") / total * 100,
            2
        )
    ) \
    .orderBy(col("count").desc()) \
    .show()


# ------------------------------------------------------------
# 38. SENTIMENT BY HASHTAG
# ------------------------------------------------------------

print("\nSentiment by hashtag:")

sentiment_df \
    .select(
        explode("hashtags_array").alias("hashtag"),
        "sentiment"
    ) \
    .groupBy(
        "hashtag",
        "sentiment"
    ) \
    .count() \
    .orderBy(
        col("count").desc()
    ) \
    .show(20, truncate=False)


# ------------------------------------------------------------
# 39. SENTIMENT BY HOUR
# ------------------------------------------------------------

print("\nSentiment by hour:")

sentiment_df \
    .withColumn(
        "hour",
        hour("timestamp")
    ) \
    .groupBy(
        "hour",
        "sentiment"
    ) \
    .count() \
    .orderBy(
        "hour",
        col("count").desc()
    ) \
    .show(20)


# ============================================================
# FINAL PROCESSED DATAFRAME
# ============================================================

# ------------------------------------------------------------
# 40. SELECT FINAL COLUMNS
# ------------------------------------------------------------

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


# ============================================================
# WRITE PROCESSED DATA TO POSTGRESQL
# ============================================================

# ------------------------------------------------------------
# 41. CONVERT HASHTAG ARRAY TO STRING
# ------------------------------------------------------------

final_df = final_df.withColumn(
    "hashtags",
    concat_ws(
        ",",
        "hashtags_array"
    )
).drop("hashtags_array")


# ------------------------------------------------------------
# 42. FINAL DATAFRAME BEFORE DATABASE WRITE
# ------------------------------------------------------------

print("\nFinal DataFrame before PostgreSQL:")
final_df.printSchema()


# ------------------------------------------------------------
# 43. WRITE TO POSTGRESQL
# ------------------------------------------------------------

print("\nWriting processed data to PostgreSQL...")

final_df.write \
    .jdbc(
        url=jdbc_url,
        table="processed_tweets",
        mode="overwrite",
        properties=properties
    )

print("Successfully written to PostgreSQL!")


# ------------------------------------------------------------
# 44. STOP SPARK
# ------------------------------------------------------------

spark.stop()