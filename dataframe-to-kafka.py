from pyspark.sql.functions import to_json, col, struct
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType,StructField, StringType, IntegerType
import os
spark = SparkSession.builder.appName('SparkByExamples.com').getOrCreate()
spark_version = '3.2.1'
os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages org.apache.spark:spark-sql-kafka-0-10_2.12:{}'.format(spark_version)

#spark = SparkSession.builder.appName('SparkByExamples.com').getOrCreate()
data2 = [("James", "", "Smith", "36636", "M", 3000),
         ("Michael", "Rose", "", "40288", "M", 4000),
         ("Robert", "", "Williams", "42114", "M", 4000),
         ("Maria", "Anne", "Jones", "39192", "F", 4000),
         ("Jen", "Mary", "Brown", "", "F", -1)
         ]

schema = StructType([ \
    StructField("firstname", StringType(), True), \
    StructField("middlename", StringType(), True), \
    StructField("lastname", StringType(), True), \
    StructField("id", StringType(), True), \
    StructField("gender", StringType(), True), \
    StructField("salary", IntegerType(), True) \
    ])

df = spark.createDataFrame(data=data2, schema=schema)
bootstrap_servers=["ec2-135.compute-1.amazonaws.com:9092","ec2--238.compute-1.amazonaws.com:9092","ec2-135.compute-1.amazonaws.com:9092"]

# df.selectExpr("CAST(age AS STRING) as key", "CAST(name AS STRING) as value")
# .write
# .format("kafka")
# .option("kafka.bootstrap.servers", "localhost:9092")
# .option("topic", "test-topic")
# .save(
#################################
# spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.13:3.2.1 dataframe-to-kafka.py

df2 = df.select(to_json(struct("firstname","middlename","lastname","id","gender","salary"))).toDF("value")
df2.write\
    .format("kafka")\
    .option("kafka.bootstrap.servers", bootstrap_servers[1])\
    .option("topic", "third_topic")\
    .save()