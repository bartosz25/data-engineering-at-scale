from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession

builder = (
    SparkSession.builder.appName("vertical_partitioner")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config(
        "spark.sql.catalog.spark_catalog",
        "org.apache.spark.sql.delta.catalog.DeltaCatalog",
    )
)

spark = configure_spark_with_delta_pip(builder).getOrCreate()

orders = spark.createDataFrame(
    [(1, "Alice", "alice@example.com", 120.0, "delivered"), 
     (2, "Bob", "bob@example.com", 89.0, "shipped"),
     (3, "Carol", "carol@example.com", 45.0, "pending"),
    ], ["order_id", "customer_name", "email", "amount", "status"],
)

print('Original data')
orders.show()

orders_no_pii = orders.select("order_id", "amount", "status")
orders_no_pii.write.format("delta").mode("overwrite").saveAsTable("orders_no_pii")

orders_pii = orders.select("order_id", "customer_name", "email")
orders_pii.write.format("delta").mode("overwrite").saveAsTable("orders_pii")

print("No PII data:")
spark.table("orders_no_pii").show()

print("PII data:")
spark.table("orders_pii").show()

print('Full orders')
spark.sql('''
CREATE OR REPLACE VIEW orders_all AS 
SELECT o.order_id, o.customer_name, o.email, n.amount, n.status
FROM orders_no_pii n
JOIN orders_pii o
ON o.order_id = n.order_id
''')

spark.sql('SELECT * FROM orders_all').show()