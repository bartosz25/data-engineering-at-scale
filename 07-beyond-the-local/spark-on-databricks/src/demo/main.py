import argparse
from pyspark.sql import SparkSession


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--job_name", required=True)

    arg = parser.parse_args()
    print(f'Hello world for {arg.job_name}')

    spark = SparkSession.builder.appName("repartition-demo").getOrCreate()
    data = [(i, f"record_{i:03d}", round(i * 1.5, 1)) for i in range(1, 2000)]
    df = spark.createDataFrame(data, ["id", "label", "value"])
    df.show(n=10, truncate=False)
