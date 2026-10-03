from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession, functions as F

builder = (
    SparkSession.builder.appName("parallel_split")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config(
        "spark.sql.catalog.spark_catalog",
        "org.apache.spark.sql.delta.catalog.DeltaCatalog",
    )
)

spark = configure_spark_with_delta_pip(builder).getOrCreate()

raw_orders = spark.createDataFrame(
    [
        (1, "Alice", 120.0, "2024-01-01", "delivered"),
        (2, "Bob", 89.0, "2024-01-01", "delivered"),
        (3, "Carol", 45.0, "2024-01-02", "cancelled"),
        (4, "Dave", 200.0, "2024-01-02", "delivered"),
        (5, "Dave", 300.0, "2024-01-04", "delivered"),
    ], ["order_id", "customer", "amount", "order_date", "status"],
)

not_cancelled_orders = raw_orders.filter("status != 'cancelled'")
not_cancelled_orders.cache()

# Output 1: unique customers
not_cancelled_orders.dropDuplicates(["customer"]).write.format("delta").mode("overwrite").saveAsTable(
    "unique_customers"
)

# Output 2: analytics table
(not_cancelled_orders.groupBy('customer').agg(
    F.sum("amount").alias("total_amount"), F.min("amount").alias("min_order"),
    F.max("amount").alias("max_order"))
.write.format("delta").mode("overwrite").saveAsTable(
    "orders_analytics"
))

print("unique_customers:")
spark.table("unique_customers").show()

print("orders_analytics:")
spark.table("orders_analytics").show()
