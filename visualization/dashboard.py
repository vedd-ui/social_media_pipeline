import psycopg2
import pandas as pd
import matplotlib.pyplot as plt
from matplotlib.patches import FancyBboxPatch


# ============================================================
# 1. POSTGRESQL CONFIGURATION
# ============================================================

DB_CONFIG = {
    "host": "127.0.0.1",
    "port": 5432,
    "database": "socialmedia",
    "user": "postgres",
    "password": "postgres"
}


# ============================================================
# 2. CONNECT TO POSTGRESQL
# ============================================================

print("Connecting to PostgreSQL...")

conn = psycopg2.connect(**DB_CONFIG)

print("Connected successfully!")


# ============================================================
# 3. LOAD PROCESSED DATA
# ============================================================

query = """
SELECT
    tweet_id,
    username,
    clean_text,
    timestamp,
    sentiment,
    hashtags
FROM processed_tweets
"""

df = pd.read_sql(query, conn)

conn.close()

print(f"Loaded {len(df)} processed tweets.")


if df.empty:
    print("No data found in processed_tweets.")
    exit()


# ============================================================
# 4. PREPARE TIMESTAMP
# ============================================================

# PostgreSQL timestamps -> UTC -> India Standard Time
df["timestamp"] = pd.to_datetime(
    df["timestamp"],
    utc=True
).dt.tz_convert("Asia/Kolkata")


# ============================================================
# 5. SENTIMENT DATA
# ============================================================

sentiment_counts = (
    df["sentiment"]
    .value_counts()
    .reindex(
        ["Positive", "Neutral", "Negative"],
        fill_value=0
    )
)


# ============================================================
# 6. TWEETS BY HOUR
# ============================================================

df["hour"] = df["timestamp"].dt.hour

hour_counts = (
    df["hour"]
    .value_counts()
    .sort_index()
)


# ============================================================
# 7. TWEET LENGTH CATEGORIES
# ============================================================

df["tweet_length"] = (
    df["clean_text"]
    .fillna("")
    .str.len()
)

df["length_category"] = pd.cut(
    df["tweet_length"],
    bins=[-1, 49, 150, float("inf")],
    labels=["Short", "Medium", "Long"]
)

length_counts = (
    df["length_category"]
    .value_counts()
    .reindex(
        ["Short", "Medium", "Long"],
        fill_value=0
    )
)


# ============================================================
# 8. HASHTAG DATA
# ============================================================

hashtag_list = []

for hashtags in df["hashtags"].fillna(""):
    if hashtags:

        for hashtag in str(hashtags).split(","):

            hashtag = hashtag.strip().lower()

            if hashtag:
                hashtag_list.append(hashtag)


hashtag_counts = (
    pd.Series(hashtag_list)
    .value_counts()
    .head(10)
)


# ============================================================
# 9. SUMMARY STATISTICS
# ============================================================

total_tweets = len(df)

positive_count = sentiment_counts["Positive"]
neutral_count = sentiment_counts["Neutral"]
negative_count = sentiment_counts["Negative"]

positive_percentage = (
    positive_count / total_tweets * 100
)

neutral_percentage = (
    neutral_count / total_tweets * 100
)

negative_percentage = (
    negative_count / total_tweets * 100
)


# ============================================================
# 10. DASHBOARD COLORS
# ============================================================

dark_blue = "#123F73"
blue = "#1769D1"

positive_color = "#2CA02C"
neutral_color = "#1769D1"
negative_color = "#E52B20"

purple = "#7B3FB5"
orange = "#FF8C00"
teal = "#159A9C"

light_blue = "#EEF5FC"


# ============================================================
# 11. CREATE FIGURE
# ============================================================

fig = plt.figure(
    figsize=(18, 12),
    facecolor="white"
)

fig.suptitle(
    "Social Media Analytics Dashboard",
    fontsize=27,
    fontweight="bold",
    color=dark_blue,
    y=0.975
)


# ============================================================
# 12. CREATE 2 × 2 GRID
# ============================================================

axes = fig.subplots(2, 2)

fig.subplots_adjust(
    left=0.055,
    right=0.965,
    top=0.885,
    bottom=0.225,
    wspace=0.16,
    hspace=0.28
)


# ============================================================
# 13. FUNCTION FOR CARD HEADER
# ============================================================

def add_card_header(ax, title):

    header = FancyBboxPatch(
        (0, 1.045),
        1,
        0.12,
        transform=ax.transAxes,
        boxstyle="round,pad=0.01,rounding_size=0.025",
        facecolor=dark_blue,
        edgecolor=dark_blue,
        linewidth=1.5,
        clip_on=False
    )

    ax.add_patch(header)

    ax.text(
        0.5,
        1.105,
        title,
        transform=ax.transAxes,
        ha="center",
        va="center",
        fontsize=15,
        fontweight="bold",
        color="white"
    )


# ============================================================
# 14. SENTIMENT DISTRIBUTION
# ============================================================

ax = axes[0, 0]

sentiment_colors = [
    positive_color,
    neutral_color,
    negative_color
]

bars = ax.bar(
    sentiment_counts.index,
    sentiment_counts.values,
    color=sentiment_colors,
    width=0.65
)

ax.set_title("")
ax.set_xlabel("Sentiment", fontsize=11)
ax.set_ylabel("Number of Tweets", fontsize=11)

ax.set_ylim(
    0,
    sentiment_counts.max() * 1.20
)

ax.grid(
    axis="y",
    linestyle="--",
    alpha=0.25
)

ax.set_axisbelow(True)

for bar, value in zip(
    bars,
    sentiment_counts.values
):

    ax.text(
        bar.get_x() + bar.get_width() / 2,
        value + sentiment_counts.max() * 0.025,
        str(value),
        ha="center",
        va="bottom",
        fontsize=13,
        fontweight="bold"
    )

add_card_header(
    ax,
    "1. SENTIMENT DISTRIBUTION"
)


# ============================================================
# 15. TOP 10 HASHTAGS
# ============================================================

ax = axes[0, 1]

if not hashtag_counts.empty:

    hashtags = hashtag_counts.index[::-1]
    values = hashtag_counts.values[::-1]

    bars = ax.barh(
        hashtags,
        values,
        color=purple,
        height=0.65
    )

    ax.set_xlabel(
        "Number of Occurrences",
        fontsize=11
    )

    ax.set_ylabel(
        "Hashtag",
        fontsize=11
    )

    ax.set_xlim(
        0,
        hashtag_counts.max() * 1.13
    )

    ax.grid(
        axis="x",
        linestyle="--",
        alpha=0.25
    )

    ax.set_axisbelow(True)

    for bar, value in zip(
        bars,
        values
    ):

        ax.text(
            value + hashtag_counts.max() * 0.012,
            bar.get_y() + bar.get_height() / 2,
            str(value),
            va="center",
            fontsize=11,
            fontweight="bold"
        )

else:

    ax.text(
        0.5,
        0.5,
        "No hashtag data available",
        ha="center",
        va="center",
        transform=ax.transAxes
    )


add_card_header(
    ax,
    "2. TOP 10 HASHTAGS"
)


# ============================================================
# 16. TWEETS BY HOUR
# ============================================================

ax = axes[1, 0]

hours = hour_counts.index.astype(str)
hour_values = hour_counts.values

bars = ax.bar(
    hours,
    hour_values,
    color=[orange, teal][:len(hours)]
)

ax.set_xlabel(
    "Hour (24-Hour Format)",
    fontsize=11
)

ax.set_ylabel(
    "Number of Tweets",
    fontsize=11
)

ax.set_ylim(
    0,
    hour_counts.max() * 1.20
)

ax.grid(
    axis="y",
    linestyle="--",
    alpha=0.25
)

ax.set_axisbelow(True)

for bar, value in zip(
    bars,
    hour_values
):

    ax.text(
        bar.get_x() + bar.get_width() / 2,
        value + hour_counts.max() * 0.025,
        str(value),
        ha="center",
        va="bottom",
        fontsize=13,
        fontweight="bold"
    )

add_card_header(
    ax,
    "3. TWEETS BY HOUR"
)


# ============================================================
# 17. TWEET LENGTH CATEGORIES
# ============================================================

ax = axes[1, 1]

length_colors = [
    purple,
    positive_color,
    blue
]

bars = ax.bar(
    length_counts.index,
    length_counts.values,
    color=length_colors,
    width=0.65
)

ax.set_xlabel(
    "Length Category",
    fontsize=11
)

ax.set_ylabel(
    "Number of Tweets",
    fontsize=11
)

ax.set_ylim(
    0,
    length_counts.max() * 1.20
)

ax.grid(
    axis="y",
    linestyle="--",
    alpha=0.25
)

ax.set_axisbelow(True)

for bar, value in zip(
    bars,
    length_counts.values
):

    ax.text(
        bar.get_x() + bar.get_width() / 2,
        value + length_counts.max() * 0.025,
        str(value),
        ha="center",
        va="bottom",
        fontsize=13,
        fontweight="bold"
    )

add_card_header(
    ax,
    "4. TWEET LENGTH CATEGORIES"
)


# ============================================================
# 18. SUMMARY PANEL
# ============================================================

summary_y = 0.075
summary_height = 0.095

summary_panel = FancyBboxPatch(
    (0.055, summary_y),
    0.91,
    summary_height,
    transform=fig.transFigure,
    boxstyle="round,pad=0.008,rounding_size=0.02",
    facecolor=light_blue,
    edgecolor=dark_blue,
    linewidth=2
)

fig.add_artist(summary_panel)


# ------------------------------------------------------------
# Vertical separators
# ------------------------------------------------------------

separator_positions = [
    0.25,
    0.50,
    0.75
]

for x in separator_positions:

    fig.lines.append(
        plt.Line2D(
            [x, x],
            [
                summary_y + 0.015,
                summary_y + summary_height - 0.015
            ],
            transform=fig.transFigure,
            color=blue,
            linewidth=1.5
        )
    )


# ============================================================
# 19. SUMMARY TEXT
# ============================================================

# Total Tweets

fig.text(
    0.15,
    summary_y + 0.057,
    "TOTAL TWEETS",
    ha="center",
    va="center",
    fontsize=12,
    fontweight="bold",
    color=dark_blue
)

fig.text(
    0.15,
    summary_y + 0.027,
    f"{total_tweets:,}",
    ha="center",
    va="center",
    fontsize=23,
    fontweight="bold",
    color=dark_blue
)


# Positive

fig.text(
    0.375,
    summary_y + 0.057,
    "POSITIVE",
    ha="center",
    va="center",
    fontsize=12,
    fontweight="bold",
    color=positive_color
)

fig.text(
    0.375,
    summary_y + 0.027,
    f"{positive_count:,} ({positive_percentage:.1f}%)",
    ha="center",
    va="center",
    fontsize=18,
    fontweight="bold",
    color=positive_color
)


# Neutral

fig.text(
    0.625,
    summary_y + 0.057,
    "NEUTRAL",
    ha="center",
    va="center",
    fontsize=12,
    fontweight="bold",
    color=blue
)

fig.text(
    0.625,
    summary_y + 0.027,
    f"{neutral_count:,} ({neutral_percentage:.1f}%)",
    ha="center",
    va="center",
    fontsize=18,
    fontweight="bold",
    color=blue
)


# Negative

fig.text(
    0.85,
    summary_y + 0.057,
    "NEGATIVE",
    ha="center",
    va="center",
    fontsize=12,
    fontweight="bold",
    color=negative_color
)

fig.text(
    0.85,
    summary_y + 0.027,
    f"{negative_count:,} ({negative_percentage:.1f}%)",
    ha="center",
    va="center",
    fontsize=18,
    fontweight="bold",
    color=negative_color
)


# ============================================================
# 20. TIMEZONE NOTE
# ============================================================

fig.text(
    0.5,
    0.035,
    "Note: All timestamps are in Asia/Kolkata time (IST)",
    ha="center",
    va="center",
    fontsize=11,
    fontstyle="italic",
    color="black"
)


# ============================================================
# 21. SAVE DASHBOARD
# ============================================================

output_file = "social_media_dashboard.png"

plt.savefig(
    output_file,
    dpi=300,
    bbox_inches="tight",
    facecolor="white"
)

print(
    f"\nDashboard saved as: {output_file}"
)


# ============================================================
# 22. DISPLAY DASHBOARD
# ============================================================

plt.show()