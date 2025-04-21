from pyspark.sql import SparkSession
from pyspark.sql.functions import explode,split
from pyspark.sql.types import *
from pyspark.sql.functions import col ,to_json,struct

spark = SparkSession.builder.appName("stream from csv").getOrCreate()
schema = StructType([
    StructField("id",IntegerType(),True),
    StructField("name",StringType(),True),
    StructField("dept",StringType(),True),
    StructField("salary",IntegerType(),True)
])
df = spark.readStream\
    .format("csv")\
    .option("header","True")\
    .schema(schema)\
    .load("/home/hdoop/notebooks/data/stream_csv/*")
# Convert DataFrame to JSON format for Kafka
df_kafka = df.select(
    # You can use a column as key or a constant
    col("id").cast("string").alias("key"),
    # Convert the entire row to JSON for the value
    to_json(struct("*")).alias("value")
)
table_write = df.writeStream\
    .format("kafka")\
    .option("kafka.bootstrap.servers","localhost:9092")\
    .option("topic","listen")\
    .option("checkpointLocation","hdfs://127.0.0.1:9000/checkpoint/stream_csv")\
    .start()

table_write.awaitTermination()

spark.stop()