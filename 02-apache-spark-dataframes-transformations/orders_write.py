from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

# Delta Lake requires extensions registered on the SparkSession.
# configure_spark_with_delta_pip injects the required JARs and config entries:
#   spark.sql.extensions = io.delta.sql.DeltaSparkSessionExtension
#   spark.sql.catalog.spark_catalog = org.apache.spark.sql.delta.catalog.DeltaCatalog
spark = configure_spark_with_delta_pip(SparkSession.builder.master("local[*]")
                                       .config("spark.sql.extensions",
                                               "io.delta.sql.DeltaSparkSessionExtension")
                                       .config("spark.sql.catalog.spark_catalog",
                                               "org.apache.spark.sql.delta.catalog.DeltaCatalog")
                                       ).getOrCreate()
orders = (
    spark.read
    .option("header", "true")
    .option("inferSchema", "true")
    .csv("data")
)
orders.cache()

monthly_totals = (
    orders
    .groupBy(F.date_trunc("month", "order_date").alias("order_month"))
    .agg(F.round(F.sum("order_amount"), 2).alias("total_amount"))
    .orderBy("order_month")
)

output_json = '/tmp/data-engineering-at-scale/02-apache-spark-dataframes-transformations/orders-json'
monthly_totals.write.mode("overwrite").json(output_json)

output_parquet = '/tmp/data-engineering-at-scale/02-apache-spark-dataframes-transformations/orders-parquet'
monthly_totals.write.mode("overwrite").parquet(output_parquet)

output_delta = '/tmp/data-engineering-at-scale/02-apache-spark-dataframes-transformations/orders-delta'
monthly_totals.write.format("delta").mode("overwrite").save(output_delta)
