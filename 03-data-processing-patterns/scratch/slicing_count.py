import datetime
from math import ceil

from delta import configure_spark_with_delta_pip
from pyspark.sql import Row, SparkSession
from pyspark.sql.types import (
    DateType,
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
)

builder = (
    SparkSession.builder.appName("slicing_count")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config(
        "spark.sql.catalog.spark_catalog",
        "org.apache.spark.sql.delta.catalog.DeltaCatalog",
    )
)

spark = configure_spark_with_delta_pip(builder).getOrCreate()

# --- Setup: write orders to a Delta table ---
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
    Row(
        order_id=1001 + i,
        customer_id=(i % 5) + 1,
        order_date=datetime.date(2024, 1, 1 + (i // 5)),
        amount=float((i + 1) * 50),
        status="delivered",
    )
    for i in range(20)
]

df_setup = spark.createDataFrame(orders, schema)
df_setup.write.format("delta").mode("overwrite").saveAsTable("orders_count_demo")

# --- Demo: count-based slicing ---
# Delta Lake does not natively enforce a row-count per task.
# The pattern is: read the table, compute the required number of partitions
# so that each partition holds at most ROWS_PER_TASK rows, then repartition.
ROWS_PER_TASK = 5

df = spark.read.format("delta").table("orders_count_demo")
total_rows = df.count()
num_partitions = max(1, ceil(total_rows / ROWS_PER_TASK))
df = df.repartition(num_partitions)

print(f"Total rows:       {total_rows}")
print(f"Rows per task:    {ROWS_PER_TASK}")
print(f"Partitions/tasks: {df.rdd.getNumPartitions()}")
df.show()
