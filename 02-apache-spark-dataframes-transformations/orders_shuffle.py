from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = SparkSession.builder.appName("Shuffle demo").getOrCreate()

orders = (
    spark.read
    .option("header", "true")
    .option("inferSchema", "true")
    .csv("data/")
)

result = (
    orders
    .groupBy(F.date_trunc("month", "order_date").alias("order_month"))
    .agg(F.round(F.sum("order_amount"), 2).alias("total_amount"))
    .orderBy("order_month", ascending=True).limit(10)
)

result.show(truncate=False)

while True:
    pass