from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = SparkSession.builder.appName("OrdersUnionFiles").getOrCreate()

orders_csv = (
    spark.read
    .option("header", "true")
    .option("inferSchema", "true")
    .csv("examples/orders_group1.csv")
)

orders_json = (
    spark.read
    .json("examples/orders_group2.json")
)

print("=== Group 1 schema (CSV) ===")
orders_csv.printSchema()

print("=== Group 2 schema (JSON) ===")
orders_json.printSchema()

orders_json_aligned = (
    orders_json
    .withColumnRenamed("user_first_name", "first_name")
    .withColumnRenamed("user_last_name",  "last_name")
    .withColumnRenamed("order_amount",    "amount")
)

result = orders_csv.unionByName(orders_json_aligned, allowMissingColumns=False)

result.show(truncate=False)
