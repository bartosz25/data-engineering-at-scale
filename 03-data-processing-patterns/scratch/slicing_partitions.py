from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession
from pyspark.sql.functions import lit, struct, to_json

KAFKA_BOOTSTRAP = "localhost:9092"
TOPIC = "orders"

builder = (
    SparkSession.builder.appName("slicing_partitions")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config(
        "spark.sql.catalog.spark_catalog",
        "org.apache.spark.sql.delta.catalog.DeltaCatalog",
    )
    # Adjust the artifact version if needed to match your PySpark version.
    .config(
        "spark.jars.packages",
        "org.apache.spark:spark-sql-kafka-0-10_2.13:4.1.0",
    )
)

spark = configure_spark_with_delta_pip(builder).getOrCreate()

# --- Setup: produce 20 orders to Kafka ---
orders = [(1001 + i, (i % 5) + 1, float((i + 1) * 50)) for i in range(20)]
df_setup = spark.createDataFrame(orders, ["order_id", "customer_id", "amount"])

(
    df_setup.select(
        lit(None).cast("string").alias("key"),
        to_json(struct("*")).alias("value"),
    )
    .write.format("kafka")
    .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
    .option("topic", TOPIC)
    .save()
)

print("Orders written to Kafka.")

# --- Demo: partition-based slicing ---
# Kafka distributes messages across topic partitions (4 in docker-compose).
# By default Spark assigns one task per Kafka partition, so the number of
# Spark tasks equals the number of Kafka partitions the topic has.
# Use minPartitions to force Spark to split partitions further if needed.
df = (
    spark.read.format("kafka")
    .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
    .option("subscribe", TOPIC)
    .option("startingOffsets", "earliest")
    .load()
)

print(f"Kafka partitions → Spark tasks: {df.rdd.getNumPartitions()}")
df.selectExpr(
    "partition",
    "offset",
    "CAST(value AS STRING) AS value",
).orderBy("partition", "offset").show(truncate=False)
