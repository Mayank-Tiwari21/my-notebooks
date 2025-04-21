from pyspark.sql import SparkSession
from pyspark.sql.functions import explode,split
from pyspark.sql.types import *
from pyspark.sql.functions import col ,to_json,struct

spark = SparkSession.builder.appName("stream from csv").getOrCreate()
schema = StructType([
    StructField("id", IntegerType(), True),
    StructField("location_id", IntegerType(), True),
    StructField("address_1", StringType(), True),
    StructField("address_2", StringType(), True),
    StructField("city", StringType(), True),
    StructField("state_province", StringType(), True),
    StructField("postal_code", StringType(), True),
    StructField("country", StringType(), True)
])

df = spark.readStream\
    .format("csv")\
    .option("header","True")\
    .schema(schema)\
    .load("/home/hdoop/notebooks/data/stream_csv/*")

table_write = df.writeStream\
    .format("console")\
    .option("truncate","false")\
    .start()

table_write.awaitTermination()

spark.stop()