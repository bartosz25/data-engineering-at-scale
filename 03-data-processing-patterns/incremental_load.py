from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession

builder = (
    SparkSession.builder.appName("incremental_load")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config(
        "spark.sql.catalog.spark_catalog",
        "org.apache.spark.sql.delta.catalog.DeltaCatalog",
    )
)

spark = configure_spark_with_delta_pip(builder).getOrCreate()

batch_1 = spark.createDataFrame(
    [(1, "Alice", 120.0, "delivered"), (2, "Bob", 89.0, "delivered"), (3, "Carol", 45.0, "shipped"),],
    ["order_id", "customer", "amount", "status"],
)

batch_1.write.format("delta").mode("overwrite").saveAsTable("orders_incremental")

print("After batch 1:")
spark.table("orders_incremental").show(),

input('A few moments later').strip().lower()

batch_2 = spark.createDataFrame(
    [(4, "Dave", 200.0, "pending"),(5, "Eve", 55.0, "pending")],
    ["order_id", "customer", "amount", "status"],
)

batch_2.write.format("delta").mode("append").saveAsTable("orders_incremental")

print("After batch 2:")
spark.table("orders_incremental").show(),
