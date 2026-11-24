
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, from_json, to_timestamp, window
)
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType
)

# 1. Create Spark Session
spark = (
    SparkSession.builder
    .appName("CampusRealTimeAnalytics")
    .config("spark.hadoop.fs.defaultFS", "file:///")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

# 2. Define JSON Schema
schema = StructType([
    StructField("eventId", IntegerType(), True),
    StructField("studentId", StringType(), True),
    StructField("eventType", StringType(), True),
    StructField("department", StringType(), True),
    StructField("eventTime", StringType(), True)
])

# 3. Read events from Kafka
raw = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "localhost:9092")
    .option("subscribe", "campus-events")
    .option("startingOffsets", "earliest")
    .load()
)

# 4. Convert Kafka value to JSON string
json_events = raw.selectExpr("CAST(value AS STRING) AS json")

# 5. Parse JSON events
parsed = (
    json_events
    .select(from_json(col("json"), schema).alias("e"))
    .select("e.*")
    .withColumn("event_ts", to_timestamp(col("eventTime")))
    .filter(col("event_ts").isNotNull())
)

# 6. Filter check-in events
checkins = parsed.filter(col("eventType") == "check-in")

# 7. Event-time window aggregation
counts = (
    checkins
    .withWatermark("event_ts", "30 seconds")
    .groupBy(
        window(col("event_ts"), "2 minutes", "1 minute"),
        col("department")
    )
    .count()
)

# 8. Display streaming results
query = (
    counts.writeStream
    .outputMode("update")
    .trigger(processingTime="5 seconds")
    .format("console")
    .option("truncate", "false")
    .option(
    "checkpointLocation",
    "file:///home/aya_zeghad/tp06-stream-processing/checkpoint/sliding-2min-1min"
)
    .start()
)

query.awaitTermination()

