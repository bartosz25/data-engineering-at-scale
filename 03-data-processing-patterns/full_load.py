from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession

builder = (
    SparkSession.builder.appName("full_load")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config(
        "spark.sql.catalog.spark_catalog",
        "org.apache.spark.sql.delta.catalog.DeltaCatalog",
    )
)

spark = configure_spark_with_delta_pip(builder).getOrCreate()


orders_20261011 = spark.createDataFrame(
    [(1, "Alice", 120.0, "delivered"), (2, "Bob", 89.0, "delivered"), (3, "Carol", 45.0, "shipped"),],
    ["order_id", "customer", "amount", "status"],
)

orders_20261011.write.format("delta").mode("overwrite").saveAsTable("orders_full")

print("After run 1:")
spark.table("orders_full").show()

input('A few moments later').strip().lower()

orders_20261012 = spark.createDataFrame(
    [(1, "Alice", 120.0, "delivered"), (2, "Bob", 89.0, "delivered")],
    ["order_id", "customer", "amount", "status"],
)

orders_20261012.write.format("delta").mode("overwrite").saveAsTable("orders_full")

print("After run 2 — order 3 deleted at source, deletion propagated:")
spark.table("orders_full").show()
