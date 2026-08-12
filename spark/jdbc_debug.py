from pyspark.sql import SparkSession

JDBC_DRIVER = r"D:\SocialMediaPipeline\spark\jars\postgresql-42.7.13.jar"

spark = (
    SparkSession.builder
    .appName("JDBC_Debug")
    .master("local[*]")
    .config("spark.jars", JDBC_DRIVER)
    .config("spark.driver.extraClassPath", JDBC_DRIVER)
    .config("spark.driver.extraJavaOptions", "-Duser.timezone=Asia/Kolkata")
    .getOrCreate()
)

url = "jdbc:postgresql://127.0.0.1:5432/socialmedia"
try:
    print("Testing direct JDBC connection...")

    conn = spark._jvm.java.sql.DriverManager.getConnection(
        url,
        "postgres",
        "postgres"
    )

    print("SUCCESS: JDBC connection established!")
    print("Database:", conn.getMetaData().getDatabaseProductName())
    print("Version:", conn.getMetaData().getDatabaseProductVersion())

    conn.close()

except Exception as e:
    print("JDBC CONNECTION FAILED")
    print(e)

finally:
    spark.stop()