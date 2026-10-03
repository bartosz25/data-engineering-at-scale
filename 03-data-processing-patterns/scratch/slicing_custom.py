import datetime

from pyspark.sql import SparkSession
from pyspark.sql.types import (
    DateType,
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
)

JDBC_URL = "jdbc:postgresql://localhost:5432/orders_db"
JDBC_PROPS = {
    "user": "spark",
    "password": "spark",
    "driver": "org.postgresql.Driver",
}

builder = (
    SparkSession.builder.appName("slicing_custom")
    .config("spark.jars.packages", "org.postgresql:postgresql:42.7.3")
)

spark = builder.getOrCreate()

# --- Setup: write orders to PostgreSQL via JDBC ---
schema = StructType(
    [
        StructField("order_id", IntegerType()),
        StructField("customer_id", IntegerType()),
        StructField("order_date", DateType()),
        StructField("amount", DoubleType()),
        StructField("status", StringType()),
    ]
)

orders = [
    (
        1001 + i,
        (i % 5) + 1,
        datetime.date(2024, 1, 1 + (i // 5)),
        float((i + 1) * 50),
        "delivered",
    )
    for i in range(20)
]

df_setup = spark.createDataFrame(orders, schema)
df_setup.write.jdbc(url=JDBC_URL, table="orders", mode="overwrite", properties=JDBC_PROPS)

print("Orders written to PostgreSQL.")

# --- Demo: custom partitioning via value-range predicates ---
# Each predicate is a SQL WHERE clause that defines the slice of data
# one Spark task is responsible for. Tasks run in parallel, each issuing
# its own JDBC query against the database.
# This gives full control over the boundaries and size of each slice,
# which is useful when the column is not uniformly distributed.
predicates = [
    "order_id BETWEEN 1001 AND 1005",
    "order_id BETWEEN 1006 AND 1010",
    "order_id BETWEEN 1011 AND 1015",
    "order_id BETWEEN 1016 AND 1020",
]

df = spark.read.jdbc(
    url=JDBC_URL,
    table="orders",
    predicates=predicates,
    properties=JDBC_PROPS,
)

print(f"Predicates defined: {len(predicates)} → Spark tasks: {df.rdd.getNumPartitions()}")
df.orderBy("order_id").show()
