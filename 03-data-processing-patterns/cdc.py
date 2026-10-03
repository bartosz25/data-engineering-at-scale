from delta import configure_spark_with_delta_pip
from delta.tables import DeltaTable
from pyspark.sql import SparkSession

builder = (
    SparkSession.builder.appName("cdc")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config(
        "spark.sql.catalog.spark_catalog",
        "org.apache.spark.sql.delta.catalog.DeltaCatalog",
    )
)

spark = configure_spark_with_delta_pip(builder).getOrCreate()

initial = spark.createDataFrame(
    [(1, "Alice", 120.0, "delivered"), (2, "Bob", 89.0, "pending"), (3, "Carol", 45.0, "shipped")],
    ["order_id", "customer", "amount", "status"],
)

(initial.write.format("delta").mode("overwrite")
    .option("delta.enableChangeDataFeed", "true")
    .saveAsTable("orders_cdc"))

print("orders_cdc:")
spark.table("orders_cdc").show()

input("Let's apply some changes to the table...")


orders_cdc = DeltaTable.forName(spark, "orders_cdc")

print('Setting delivered status of the order 2')
orders_cdc.update("order_id = 2", {"status": "'delivered'"})
print('Deleting order 3')
orders_cdc.delete("order_id = 3")
print('Inserting new order 4')
new_order = spark.createDataFrame(
    [(4, "Dave", 200.0, "pending")], ["order_id", "customer", "amount", "status"],
)
new_order.write.format("delta").mode("append").saveAsTable("orders_cdc")


input("Reading the Change Data Capture (Change Data Feed in Delta Lake)...")

changes = (
    spark.read.format("delta")
    .option("readChangeFeed", "true")
    .option("startingVersion", 1)
    .table("orders_cdc")
)
changes.select("order_id", "customer", "amount", "status", "_change_type", "_commit_version").show()

