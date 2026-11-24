import time
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType
)

spark = (
    SparkSession.builder
    .appName("SparkProcessingRateTest")
    .config("spark.hadoop.fs.defaultFS", "file:///")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

schema = StructType([
    StructField("eventId", IntegerType(), True),
    StructField("studentId", StringType(), True),
    StructField("eventType", StringType(), True),
    StructField("department", StringType(), True),
    StructField("eventTime", StringType(), True)
])

raw = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "localhost:9092")
    .option("subscribe", "campus-events")
    .option("startingOffsets", "latest")
    .load()
)

parsed = (
    raw.selectExpr("CAST(value AS STRING) AS json")
    .select(from_json(col("json"), schema).alias("e"))
    .select("e.*")
    .filter(col("eventId").isNotNull())
)

def measure_batch(batch_df, batch_id):
    start = time.time()
    total = batch_df.count()
    elapsed = time.time() - start

    rate = total / elapsed if elapsed > 0 else 0

    print(f"\nBatch ID: {batch_id}")
    print(f"Events processed: {total}")
    print(f"Processing time: {elapsed:.4f} seconds")
    print(f"Processing throughput: {rate:.2f} events/second")

query = (
    parsed.writeStream
    .foreachBatch(measure_batch)
    .option(
        "checkpointLocation",
        "file:///home/aya_zeghad/tp06-stream-processing/checkpoint/experiment-rate"
    )
    .start()
)

query.awaitTermination()
