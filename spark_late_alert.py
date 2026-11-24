from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, from_json, to_timestamp,
    from_utc_timestamp, date_format
)
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType
)

spark = (
    SparkSession.builder
    .appName("CampusLateCheckinAlert")
    .config("spark.sql.session.timeZone", "UTC")
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
    .option("startingOffsets", "earliest")
    .load()
)

parsed = (
    raw.selectExpr("CAST(value AS STRING) AS json")
    .select(from_json(col("json"), schema).alias("e"))
    .select("e.*")
    .withColumn("event_ts", to_timestamp(col("eventTime")))
    .filter(col("event_ts").isNotNull())
)

# Convert UTC event time to Algeria campus local time
local_events = parsed.withColumn(
    "campus_time",
    from_utc_timestamp(col("event_ts"), "Africa/Algiers")
)

# Late check-in: campus local time later than 08:15
late_checkins = local_events.filter(
    (col("eventType") == "check-in") &
    (date_format(col("campus_time"), "HH:mm:ss") > "08:15:00")
)

query = (
    late_checkins
    .select("studentId", "department", "eventTime")
    .writeStream
    .format("console")
    .outputMode("append")
    .option("truncate", "false")
    .option(
        "checkpointLocation",
        "file:///home/aya_zeghad/tp06-stream-processing/checkpoint/late-alert"
    )
    .start()
)

query.awaitTermination()
