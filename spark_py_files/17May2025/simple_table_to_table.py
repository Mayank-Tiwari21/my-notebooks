from pyspark.sql import SparkSession
from pyspark.sql.functions import lit
spark = SparkSession.builder \
    .appName("PostgresTableToTable") \
    .master("local[*]") \
    .getOrCreate()

url = "jdbc:postgresql://localhost:5432/my_database"
source_table = "emp"
destination_table = "emp_old"

properties = {
    "user": "postgres",
    "password": "8308",
    "driver": "org.postgresql.Driver"
}

try:
    # Read from source table
    source_df = spark.read.jdbc(url=url, table=source_table, properties=properties)

    # Perform transformations (optional)
    transformed_df = source_df.withColumn("salary",lit(55000.0))  # Example: No transformation
    #printing the transformed df
    transformed_df.show()
    transformed_df.select("dept_id").distinct().orderBy("dept_id").show()
    # Write to destination table
    transformed_df.write \
        .mode("overwrite") \
        .jdbc(url=url, table=destination_table, properties=properties)

    print("Data written successfully!")

except Exception as e:
    print(f"Error: {e}")

finally:
    spark.stop()
