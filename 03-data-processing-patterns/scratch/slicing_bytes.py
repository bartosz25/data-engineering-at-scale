import os

from pyspark.sql import SparkSession

# --- Setup: generate a text file large enough to span multiple tasks ---
TEXT_FILE = "dataset/orders_large.txt"
os.makedirs("../dataset", exist_ok=True)

with open(TEXT_FILE, "w") as f:
    for i in range(100_000):
        f.write(
            f"order_id={1000 + i},"
            f"customer_id={i % 100},"
            f"amount={i * 1.5:.2f},"
            f"status=delivered\n"
        )

# --- Session ---
spark = SparkSession.builder.appName("slicing_bytes").getOrCreate()

# --- Demo: bytes-based slicing ---
# spark.sql.files.maxPartitionBytes controls the maximum number of bytes
# read by a single task when scanning file-based sources.
# Smaller value → more tasks, each processing less data.
BYTES_PER_TASK = 1 * 1024 * 1024  # 1 MB

spark.conf.set("spark.sql.files.maxPartitionBytes", str(BYTES_PER_TASK))

df = spark.read.text(TEXT_FILE)

print(f"File size (bytes): {os.path.getsize(TEXT_FILE):,}")
print(f"Bytes per task:    {BYTES_PER_TASK:,}")
print(f"Partitions/tasks:  {df.rdd.getNumPartitions()}")
df.show(5, truncate=False)
