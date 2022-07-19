from pyspark.sql import SparkSession
from pyspark.sql import Row
spark = SparkSession.builder.config("spark.jars", "/home/prasoon/Downloads/postgresql-42.2.5.jar") \
    .master("local").appName("PySpark_Postgres_test").getOrCreate()
df = spark.read.format("jdbc").option("url", "jdbc:postgresql://localhost:5432/its") \
    .option("driver", "org.postgresql.Driver").option("dbtable", "twitter.twittertweet") \
    .option("user", "prasoon").option("password", "9473").load()
df.show()