import pyarrow as pa
import pyarrow.compute as pc
from pyspark.sql import SparkSession
from pyspark.sql.types import DateType, DoubleType, StringType, StructField, StructType

spark = SparkSession.builder.appName("OrdersTransformations").getOrCreate()

orders = (
    spark.read
    .option("header", "true")
    .option("inferSchema", "true")
    .csv("data/")
)

# mapInArrow requires the output schema to be declared upfront so Spark can
# plan the downstream operations without inspecting the function at runtime.
OUTPUT_SCHEMA = StructType([
    StructField("client_name", StringType()),
    StructField("order_date",  DateType()),
    StructField("order_amount", DoubleType()),
])

# Receives an iterator of pyarrow.RecordBatch, yields pyarrow.RecordBatch.
# Each RecordBatch is a columnar slice of a single Spark partition.
def transform(batches):
    for batch in batches:
        # Filter: keep rows where order_amount > 50
        mask = pc.greater(batch.column("order_amount"), 50.0)
        filtered = batch.filter(mask)

        client_names = pc.binary_join_element_wise(
            filtered.column("first_name"),
            filtered.column("last_name"),
            " ",
        )

        yield pa.RecordBatch.from_arrays(
            [
                client_names,
                filtered.column("order_date"),
                filtered.column("order_amount"),
            ],
            schema=pa.schema([
                pa.field("client_name", pa.string()),
                pa.field("order_date",  pa.date32()),
                pa.field("order_amount", pa.float64()),
            ]),
        )

result = orders.mapInArrow(transform, OUTPUT_SCHEMA).orderBy("order_date", ascending=True)

result.show(10, truncate=False)
