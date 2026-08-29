from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("OrdersTransformationsSQL").getOrCreate()

orders = (
    spark.read
    .option("header", "true")
    .option("inferSchema", "true")
    .csv("data")
)
orders.createOrReplaceTempView("orders")

result = spark.sql("""
    SELECT
        CONCAT(first_name, ' ', last_name) AS client_name,
        order_date,
        order_amount
    FROM orders
    WHERE order_amount > 50
    ORDER BY order_date ASC
""")

result.show(10, truncate=False)