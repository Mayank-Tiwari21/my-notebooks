from pyspark.sql import SparkSession
from pyspark.sql.functions import explode, split

#initializing teh spark session
spark = SparkSession.builder.appName("kafkaWordCount")\
        .config("saprk.jars.packages",
                "org.apache.spark-sql-kafka-0-10_2.12:3.5.0,org.apache.spark:spark-avro_2.12:3.5.0")\
        .getOrCreate()

#set log level to Error to reduce verbosity
spark.sparkContext.setLogLevel("ERROR")

#read from kafka topic 
kafka_df = spark.readStream\
    .format("kafka")\
    .option("kafka.bootstrap.servers","localhost:9092")\
    .option("subscribe","listen")\
    .load()

words_df = kafka_df.selectExpr("CAST(value as STRING) as message")

#split the message column into words
words = words_df.select(explode(split(words_df["message"],' ')).alias("word"))

#perform word count
word__counts = words.groupBy("word").count()


#write the results to the console (or other sinks like kafka , hdfs, etc)
query = word__counts.writeStream \
    .outputMode("complete")\
    .format("console")\
    .start()

#waiting for the termination of the query
query.awaitTermination()