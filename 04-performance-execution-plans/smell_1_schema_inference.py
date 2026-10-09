from pyspark.sql.types import DoubleType, StringType, StructField, StructType, DateType

from spark_session import with_spark_session

with with_spark_session("Smell#1-Schema_Inference") as spark:
    data_path = 'data'

    print("→ Schema inference at runtime")
    (spark.read
        .option("header", "true")
        .option("inferSchema", "true")
        .csv(data_path)
        .write.format('noop').mode('append').save())

    print("→ Explicit schema")
    explicit_schema = StructType([
        StructField("first_name",   StringType()),
        StructField("last_name",    StringType()),
        StructField("order_date",   DateType()),
        StructField("order_amount", DoubleType()),
    ])

    (spark.read.option("header", "true").schema(explicit_schema).csv(data_path)
    .write.format('noop').mode('append').save())

    input('Blocking the main thread. Go to http://localhost:4040 and press any key to stop the demo')
