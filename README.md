**Social Media Data Engineering \& Sentiment Analytics Pipeline**

* An end-to-end social media data processing and analytics pipeline built using Python, Apache Kafka, PySpark, PostgreSQL, Docker, and VADER sentiment analysis.
* The project demonstrates how social media data can be collected, streamed, processed, analyzed, stored, and visualized using a data engineering pipeline.



**Project Overview**

* This project processes social media tweet data and performs text cleaning, hashtag extraction, timestamp analysis, tweet-length analysis, and sentiment classification.
* The processed data is stored in PostgreSQL and used to generate an analytics dashboard.
* The pipeline is designed with a streaming architecture using Apache Kafka, while Apache Spark acts as the main data processing engine.



**Architecture**



&#x20;                   Social Media Dataset

&#x20;                           |

&#x20;                           v

&#x20;                   +---------------+

&#x20;                   |     Kafka     |

&#x20;                   | Streaming     |

&#x20;                   |    Layer      |

&#x20;                   +-------+-------+

&#x20;                           |

&#x20;                           v

&#x20;                   +---------------+

&#x20;                   |    PySpark    |

&#x20;                   |   Processing  |

&#x20;                   +-------+-------+

&#x20;                           |

&#x20;             +-------------+-------------+

&#x20;             |             |             |

&#x20;             v             v             v

&#x20;       Data Cleaning   Hashtags      Timestamp

&#x20;             |         Extraction     Analysis

&#x20;             |             |             |

&#x20;             +-------------+-------------+

&#x20;                           |

&#x20;                           v

&#x20;                   Sentiment Analysis

&#x20;                      (VADER)

&#x20;                           |

&#x20;                           v

&#x20;                +---------------------+

&#x20;                |    PostgreSQL       |

&#x20;                | processed\_tweets    |

&#x20;                +----------+----------+

&#x20;                           |

&#x20;                           v

&#x20;                   Python Dashboard

&#x20;                 Pandas + Matplotlib





**Technologies Used:**

Python			-	Main programming language

Apache Kafka		-	Streaming/message-broker layer

Apache Spark / PySpark	-	Distributed data processing

VADER			-	Sentiment analysis

PostgreSQL		-	Persistent data storage

Docker			-	Containerization

Docker Compose		-	Multi-container management

Pandas			-	Data manipulation

Matplotlib		-	Data visualization

psycopg2		-	PostgreSQL connection

kafka-python		-	Kafka producer/consumer

python-dotenv		-	Environment configuration

Tweepy			-	Twitter/X API experimentation



The project follows these major stages:

Dataset

&#x20;  ↓

Kafka

&#x20;  ↓

PySpark

&#x20;  ↓

Cleaning \& Transformation

&#x20;  ↓

Sentiment Analysis

&#x20;  ↓

PostgreSQL

&#x20;  ↓

Visualization Dashboard



**Dataset:**

The project uses a dataset containing social media tweets.



The raw data contains fields such as:

Tweet ID

Username

Tweet text

Timestamp

Language

Hashtags

The dataset is stored in PostgreSQL in the tweets table.



**Kafka:**

* Apache Kafka is included as the streaming layer of the architecture.
* Kafka provides a mechanism for handling continuously arriving social media events and making them available to downstream processing components.
* The project contains Kafka producer and consumer components.
* Kafka is containerized using Docker.



**PySpark Processing**

Apache Spark is the primary data processing engine.

PySpark is used to:

* Read data
* Clean tweet text
* Remove URLs
* Extract hashtags
* Process timestamps
* Analyze tweet length
* Apply sentiment analysis
* Perform aggregations
* Write processed results to PostgreSQL
* Spark allows the same processing approach to scale to substantially larger datasets.



**Data Cleaning**

The original tweet text is transformed into a cleaned text field called: clean\_text

The cleaning process removes unwanted URL content and prepares the text for sentiment analysis and further processing.



**Hashtag Extraction**

Hashtags are extracted from tweets and stored as an array: hashtags\_array

This allows hashtag frequency and sentiment-by-hashtag analysis.



**Sentiment Analysis**

VADER (Valence Aware Dictionary and sEntiment Reasoner) is used to perform sentiment analysis.



Each tweet is classified into one of three categories:

Positive

Neutral

Negative



For the analyzed dataset of 1,000 tweets:

Sentiment	Tweets	Percentage

Positive	460	46.0%

Neutral	364	36.4%

Negative	176	17.6%



**PostgreSQL Storage:**

After processing, Spark stores the resulting DataFrame in PostgreSQL.

The main processed table is: processed\_tweets



The processed data contains fields such as:

tweet\_id

username

text

clean\_text

timestamp

language

hashtags\_array

sentiment



The processed table can then be queried using SQL.

Example:

SELECT sentiment, COUNT(\*) AS count

FROM processed\_tweets

GROUP BY sentiment

ORDER BY count DESC;



**Visualization Dashboard:**

The processed data is retrieved from PostgreSQL using Python.

The visualization layer uses:

psycopg2 for database connectivity

Pandas for data manipulation

Matplotlib for visualization



**The dashboard currently provides:**

* Sentiment distribution
* Top hashtags
* Tweets by hour
* Tweet length categories
* Overall sentiment summary

All timestamps displayed by the dashboard are converted to India Standard Time (IST).



**Project Structure**

SocialMediaPipeline/

│

├── consumer/

│   └── consumer.py

│

├── producer/

│   ├── producer.py

│   ├── prepare\_dataset.py

│   └── twitter\_test.py

│

├── spark/

│   ├── spark\_postgres\_test.py

│   ├── python\_worker\_test.py

│   ├── jdbc\_debug.py

│   └── jars/

│

├── visualization/

│   ├── dashboard.py

│   └── social\_media\_dashboard.png

│

├── data/

│   └── ...

│

├── docker-compose.yml

├── requirements.txt

├── .gitignore

└── README.md

Running the Project



**Create and activate the Python environment**

Python 3.12 is used for the project.

python -m venv venv312

Activate it on Windows:

venv312\\Scripts\\activate



**Install dependencies**

pip install -r requirements.txt



**Start Docker services**

Make sure Docker Desktop is running.



Start the required services using Docker Compose:

docker compose up -d



The project uses containerized services including:

PostgreSQL

Apache Kafka



**Verify PostgreSQL**

Check running containers:

docker ps

PostgreSQL should expose port:

5432



**Run the Spark processing pipeline**

Run the PySpark processing script: python spark/spark\_postgres\_test.py

The script processes the tweet data and writes the processed results to PostgreSQL.



**Generate the dashboard**

Run:

python visualization/dashboard.py

The dashboard image will be generated as:

\[Social Media Analytics Dashboard](visualization/social\_media\_dashboard.png)



**The project can answer questions such as:**

* What percentage of tweets are positive, neutral, or negative?
* Which hashtags occur most frequently?
* What sentiment is associated with particular hashtags?
* During which hours are tweets most frequent?
* How are tweets distributed by length?
* How many processed tweets are stored in PostgreSQL?
* Future Improvements



**Possible future extensions include:**

* Real-time Spark Structured Streaming
* Direct Kafka-to-Spark streaming
* Live Twitter/X API ingestion
* Real-time dashboard updates
* Additional NLP techniques
* Topic modelling
* Named entity recognition
* Sentiment trends over time
* Interactive dashboards using Plotly or Streamlit
* Deployment to cloud infrastructure
* Key Learning Outcomes



**This project demonstrates practical experience with:**

* Data engineering pipelines
* Distributed data processing
* Stream processing architecture
* ETL/data transformation
* Natural language processing
* Sentiment analysis
* Relational databases
* Containerization
* Data visualization
* Python-based analytics



**Author - Ved Dodwadkar, MTECH Data Science, COEP Technological University**



**License** - This project is intended for academic and educational purposes.

