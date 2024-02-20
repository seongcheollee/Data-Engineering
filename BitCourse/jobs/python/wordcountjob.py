from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("PythonWordCount")\
    .config("spark.jars", "/opt/spark/jars/postgresql-42.2.5.jar") \
    .getOrCreate()

jdbc_url = "jdbc:postgresql://localhost:5432/coin"
connection_properties = {
    "user": "postgres",
    "password": "postgres",
    "driver": "org.postgresql.Driver"
}


spark.stop()