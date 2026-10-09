from pyspark.sql.types import DoubleType, StringType, StructField, StructType, DateType

from spark_session import with_spark_session

with with_spark_session("Smell#3-Python_UDF") as spark:
    data_path = 'data'
    explicit_schema = StructType([
        StructField("first_name",   StringType()),
        StructField("last_name",    StringType()),
        StructField("order_date",   DateType()),
        StructField("order_amount", DoubleType()),
    ])

    data_parquet = 'data-parquet'
    orders = spark.read.option("header", "true").schema(explicit_schema).csv(data_path)
    orders.write.format('parquet').mode('overwrite').save(data_parquet)
    orders_parquet = spark.read.parquet(data_parquet).cache()
    orders_parquet.createOrReplaceTempView("orders")

    spark.sql('SELECT * FROM orders WHERE order_amount > 1000').explain(mode='formatted')
    spark.sql('SELECT first_name, last_name FROM orders WHERE order_amount > 1000').explain(mode='formatted')
