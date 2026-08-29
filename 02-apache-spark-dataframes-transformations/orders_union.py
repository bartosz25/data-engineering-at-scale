from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import DoubleType, StringType, StructField, StructType

spark = SparkSession.builder.appName("Unions example").getOrCreate()

orders_group1 = spark.createDataFrame([
    ("Alice", "Smith", 120.50),
    ("Bob",   "Jones",  45.00),
    ("Carol", "White", 200.75),
], schema=StructType([
    StructField("first_name", StringType()),
    StructField("last_name",  StringType()),
    StructField("amount",     DoubleType()),
])).withColumn("source", F.lit("orders_group1"))

orders_group2 = spark.createDataFrame([
    ("Dave", "Brown", 89.99),
    ("Eve",  "Davis", 310.00),
], schema=StructType([
    StructField("user_first_name", StringType()),
    StructField("user_last_name",  StringType()),
    StructField("order_amount",    DoubleType()),
])).withColumn("source", F.lit("orders_group2"))

orders_group2_aligned = (
    orders_group2
    .withColumnRenamed("user_first_name", "first_name")
    .withColumnRenamed("user_last_name",  "last_name")
    .withColumnRenamed("order_amount",    "amount")
)

result = orders_group1.unionByName(orders_group2_aligned)

result.show(truncate=False)
