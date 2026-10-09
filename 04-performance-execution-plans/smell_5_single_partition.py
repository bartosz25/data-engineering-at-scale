from pyspark.sql import functions as F, Window
from pyspark.sql.types import DoubleType, StringType, StructField, StructType, DateType

from spark_session import with_spark_session

with with_spark_session("Smell#5-Single_Partition") as spark:
    data_path = 'data'

    explicit_schema = StructType([
        StructField("first_name",   StringType()),
        StructField("last_name",    StringType()),
        StructField("order_date",   DateType()),
        StructField("order_amount", DoubleType()),
    ])
    orders = spark.read.option("header", "true").schema(explicit_schema).csv(data_path)


    bad_single = orders.repartition(1)
    print(f"Partition count after repartition(1): {bad_single.rdd.getNumPartitions()}")
    print(f"Input partitions count without repartitioning: {orders.rdd.getNumPartitions()}")


    global_window_df = orders.withColumn("row_num", F.row_number().over(Window.orderBy("order_date")))
    print(f"Partitions for the global window: {global_window_df.rdd.getNumPartitions()}")
    partitioned_window_df = orders.withColumn("row_num", F.row_number().over(Window.partitionBy("first_name").orderBy("order_date")))
    print(f"Partitions for the partitioned window: {partitioned_window_df.rdd.getNumPartitions()}")
