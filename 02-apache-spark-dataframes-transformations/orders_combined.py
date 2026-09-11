from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = SparkSession.builder.appName("OrdersCombined").getOrCreate()

orders = (
    spark.read
    .option("header", "true")
    .option("inferSchema", "true")
    .csv("data")
)

# Cache after loading: both transformations below fan out from this DataFrame.
# Without cache, Spark would re-read and re-parse the CSV files twice —
# once per action that triggers a downstream computation.
# You can comment this line if you want to see this side effect in action.
orders = orders.cache()

filtered = (
    orders
    .filter(F.col("order_amount") > 50)
    .withColumn("client_name", F.concat_ws(" ", F.col("first_name"), F.col("last_name")))
    .select("client_name", "order_date", "order_amount")
    .orderBy("order_date", ascending=True)
)

monthly_totals = (
    orders
    .groupBy(F.date_trunc("month", "order_date").alias("order_month"))
    .agg(F.round(F.sum("order_amount"), 2).alias("total_amount"))
    .orderBy("order_month", ascending=True)
)

print("=== Filtered orders (amount > 50) ===")
filtered.show(10, truncate=False)

print("=== Monthly revenue totals ===")
monthly_totals.show(n=10, truncate=False)


while True:
    pass

orders.unpersist()