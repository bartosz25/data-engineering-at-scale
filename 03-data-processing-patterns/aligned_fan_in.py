from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession, functions as F

builder = (
    SparkSession.builder.appName("aligned_fan_in")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config(
        "spark.sql.catalog.spark_catalog",
        "org.apache.spark.sql.delta.catalog.DeltaCatalog",
    )
)

spark = configure_spark_with_delta_pip(builder).getOrCreate()

orders_eu = spark.createDataFrame(
    [(1, "Alice", 120.0, "2024-01-01"), (2, "Bob", 89.0, "2024-01-01")],
    ["order_id", "customer", "amount", "order_date"],
)

orders_us = spark.createDataFrame(
    [(3, "Carol", 200.0, "2024-01-01"), (4, "Dave", 45.0, "2024-01-01")],
    ["order_id", "customer_name", "amount", "order_date"],
)

orders_us_adapted = orders_us.withColumnRenamed("customer_name", "customer")

all_orders = (orders_eu.withColumn('origin', F.lit('EU'))
              .union(orders_us_adapted.withColumn('origin', F.lit('US'))))

all_orders.write.format("delta").mode("overwrite").saveAsTable("orders_global")

print("All orders after fan-in")
spark.table("orders_global").show()
