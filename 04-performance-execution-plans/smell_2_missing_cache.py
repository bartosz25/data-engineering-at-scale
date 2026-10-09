from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, StringType, StructField, StructType, DateType

from spark_session import with_spark_session

with with_spark_session("Smell#2-Missing_Cache") as spark:
    data_path = 'data'

    explicit_schema = StructType([
        StructField("first_name",   StringType()),
        StructField("last_name",    StringType()),
        StructField("order_date",   DateType()),
        StructField("order_amount", DoubleType()),
    ])

    spark.sparkContext.setJobGroup(groupId='Missing cache', description='')
    orders = spark.read.option("header", "true").schema(explicit_schema).csv(data_path)
    
    print("→ Reading the input twice")
    total_rows  = orders.count()
    total_rows  = orders.count()
    total_rows  = orders.count()
    total_spend = orders.agg(F.sum("order_amount")).write.format('noop').mode('append').save()

    print("→ Reading the input once with prior caching")
    spark.sparkContext.setJobGroup(groupId='Present cache', description='')
    orders_cached =  spark.read.option("header", "true").schema(explicit_schema).csv(data_path)
    orders_cached  = orders_cached.cache()


    total_rows  = orders_cached.count()
    total_rows  = orders_cached.count()
    total_rows  = orders_cached.count()
    total_spend = orders_cached.agg(F.sum("order_amount")).write.format('noop').mode('append').save()

    input('Blocking the main thread. Go to http://localhost:4040 and press any key to stop the demo')

    orders_cached.unpersist()
