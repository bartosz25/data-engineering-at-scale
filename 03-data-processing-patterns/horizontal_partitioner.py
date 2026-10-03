from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession

builder = (
    SparkSession.builder.appName("horizontal_partitioner")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config(
        "spark.sql.catalog.spark_catalog",
        "org.apache.spark.sql.delta.catalog.DeltaCatalog",
    )
)

spark = configure_spark_with_delta_pip(builder).getOrCreate()

orders = spark.createDataFrame(
    [
        (1, "2024-01-01", 120.0), (2, "2024-01-01", 89.0), (3, "2024-01-01", 45.0),
        (4, "2024-01-02", 200.0), (5, "2024-01-02", 55.0),
        (6, "2024-01-03", 99.0),
    ], ["order_id", "order_date", "amount"],
)

(orders.write.format("delta").partitionBy("order_date")
 .mode("overwrite").saveAsTable("orders_by_date"))

jan1_total = spark.sql("""
    SELECT order_date, SUM(amount) AS total
    FROM orders_by_date
    WHERE order_date = '2024-01-01'
    GROUP BY order_date
""")

print("January 1st total:")
jan1_total.show()

jan1_total.explain(extended=True)
