from pyspark.sql import functions as F, Observation
from pyspark.sql.types import DoubleType, StringType, StructField, StructType, DateType

from spark_session import with_spark_session

with with_spark_session("Smell#6-Debug_Action") as spark:
    data_path = 'data'
    explicit_schema = StructType([
        StructField("first_name",   StringType()),
        StructField("last_name",    StringType()),
        StructField("order_date",   DateType()),
        StructField("order_amount", DoubleType()),
    ])

    orders = spark.read.option("header", "true").schema(explicit_schema).csv(data_path)

    filtered = orders.filter(F.col("order_amount") > 500)
    debug_count = filtered.count()
    filtered.agg(F.sum("order_amount")).write.format('noop').mode('append').save()
    print(f"Rows after the filtering={debug_count}")

    observation = Observation("order_metrics")
    result_obs = (
        orders
        .filter(F.col("order_amount") > 500)
        .observe(
            observation,
            F.count(F.lit(1)).alias("row_count")
        )
        .agg(F.sum("order_amount"))
        .write.format('noop').mode('append').save()
    )
    print(f"Rows after the filtering (using observation)={observation.get.get('row_count')}")
    input('Blocking the main thread. Go to http://localhost:4040 and press any key to stop the demo')

    orders.unpersist()
