from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession

from orders_write import output_json, output_parquet, output_delta

builder = SparkSession.builder.appName("OrdersRead")
spark = configure_spark_with_delta_pip(builder).getOrCreate()

json_df = spark.read.json(output_json)
print("=== JSON ===")
json_df.printSchema()
json_df.show(truncate=False)

parquet_df = spark.read.parquet(output_parquet)
print("=== Parquet ===")
parquet_df.printSchema()
parquet_df.show(truncate=False)

delta_df = spark.read.format("delta").load(output_delta)
print("=== Delta Lake ===")
delta_df.printSchema()
delta_df.show(truncate=False)

