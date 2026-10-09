from contextlib import contextmanager

from pyspark.sql import SparkSession


@contextmanager
def with_spark_session(app_name: str):
    spark = (
        SparkSession.builder
        .master("local[*]")
        .appName(app_name)
        .config('spark.sql.adaptive.enabled', False)
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")

    yield spark

    spark.stop()
