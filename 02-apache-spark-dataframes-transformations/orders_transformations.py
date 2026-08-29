from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = SparkSession.builder.appName("OrdersTransformations").getOrCreate()

orders = (
    spark.read
    .option("header", "true")
    .option("inferSchema", "true")
    .csv("data/")
)

result = (
    orders
    .filter(F.col("order_amount") > 50)
    .withColumn("client_name", F.concat_ws(" ", F.col("first_name"), F.col("last_name")))
    .select("client_name", "order_date", "order_amount")
    .orderBy("order_date", ascending=True)
)

result.show(10, truncate=False)