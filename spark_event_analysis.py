from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType
)

spark = (
    SparkSession.builder
    .appName("CampusEventTypeAnalysis")
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

analysis = (
    parsed
    .groupBy("eventType", "department")
    .count()
)

query = (
    analysis.writeStream
    .outputMode("complete")
    .format("console")
    .option("truncate", "false")
    .option(
        "checkpointLocation",
        "file:///home/aya_zeghad/tp06-stream-processing/checkpoint/event-analysis"
    )
    .start()
)

query.awaitTermination()
