from pyspark.sql import SparkSession
from pyspark.sql.functions import spark_partition_id

spark = SparkSession.builder.appName("repartition-demo").getOrCreate()

data = [(i, f"record_{i:03d}", round(i * 1.5, 1)) for i in range(1, 10_000)]
df = spark.createDataFrame(data, ["id", "label", "value"])

# Repartition to 4 so each executor owns exactly one partition.
df4 = df.repartition(40)

print(f"Partitions after  repartition: {df4.rdd.getNumPartitions()}")

print("\nRow count per partition:")
df4.groupBy(spark_partition_id().alias("partition")) \
   .count() \
   .orderBy("partition") \
   .show()

print("\nAll rows with their partition ID:")
df4.withColumn("partition", spark_partition_id()) \
   .orderBy("id") \
   .show(truncate=False)
