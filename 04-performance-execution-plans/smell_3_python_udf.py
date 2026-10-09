from pyspark.sql import functions as F
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

    orders = spark.read.option("header", "true").schema(explicit_schema).csv(data_path).cache()
    # materialise cache so timing reflects only the UDF/builtin cost and not data reading or caching
    orders.count()

    @F.udf(returnType=DoubleType())
    def udf_double(amount):
        return amount * 2.0 if amount else None
    spark.sparkContext.setJobDescription('Python_UDF')
    orders.select(udf_double("order_amount")).write.format('noop').mode('append').save()
    spark.sparkContext.setJobDescription('DataFrame_API')
    orders.select(F.col("order_amount") * 2).write.format('noop').mode('append').save()

    input('Blocking the main thread. Go to http://localhost:4040 and press any key to stop the demo')

    orders.unpersist()
