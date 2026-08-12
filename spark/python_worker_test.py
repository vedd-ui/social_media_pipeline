from pyspark.sql import SparkSession

spark = (
    SparkSession.builder
    .appName("PythonWorkerTest")
    .master("local[1]")
    .config("spark.driver.host", "127.0.0.1")
    .config("spark.driver.bindAddress", "127.0.0.1")
    .config("spark.python.worker.reuse", "false")
    .getOrCreate()
)

df = spark.createDataFrame([(1,), (2,), (3,), (4,), (5,)], ["number"])

result = df.rdd.map(lambda x: x[0] * 2).collect()

print("Result:", result)

spark.stop()