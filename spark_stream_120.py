
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, to_timestamp, window
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType
)

spark = (
    SparkSession.builder
    .appName("CampusExperiment120")
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
    .withColumn("event_ts", to_timestamp(col("eventTime")))
    .filter(col("event_ts").isNotNull())
)

checkins = parsed.filter(col("eventType") == "check-in")

counts = (
    checkins
    .withWatermark("event_ts", "30 seconds")
    .groupBy(
        window(col("event_ts"), "1 minute"),
        col("department")
    )
    .count()
)

query = (
    counts.writeStream
    .outputMode("update")
    .format("console")
    .option("truncate", "false")
    .option(
        "checkpointLocation",
        "file:///home/aya_zeghad/tp06-stream-processing/checkpoint/experiment-120"
    )
    .start()
)

query.awaitTermination()
