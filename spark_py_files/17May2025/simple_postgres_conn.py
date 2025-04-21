from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("ReadPostgres") \
    .master("local[*]") \
    .getOrCreate()

url = "jdbc:postgresql://localhost:5432/my_database"
table = "emp" #or <table> if no schema
properties = {
    "user": "postgres",
    "password": "8308",
    "driver": "org.postgresql.Driver"
}

try:
    df = spark.read.jdbc(url=url, table=table, properties=properties)
    df.show()
    df.printSchema()
except Exception as e:
    print(f"Error while reading PostgreSQL table: {e}")
finally:
    spark.stop()
