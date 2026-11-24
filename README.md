# TP06 – Real-Time Data Processing Using Apache Spark and Apache Flink

## 1. Project Overview

This project implements a real-time data processing pipeline for a smart campus environment using Apache Kafka, Apache Spark Structured Streaming, and Apache Flink SQL.

The system processes student events such as check-ins, course access, and library entries. Events are produced in JSON format and published to Kafka, then analyzed using Spark and Flink.

## 2. Technologies Used

- Apache Kafka 3.9.1
- Apache Spark 3.5.3
- Apache Flink 1.20.5
- Python 3.12
- kafka-python (Git commit: 89df9871b84843ad8b9dbd48d5b8d11d06227acf)
- Py4J 0.10.9.7
- Ubuntu on WSL2

## 3. Project Structure

- `producer.py`: Sends synthetic student events to Kafka.
- `producer_100.py`: Additional event generation.
- `producer_120_test.py`: Sends 120 events for throughput testing.
- `producer_watermark_test.py`: Generates normal and late events for watermark testing.
- `spark_stream.py`: Main Spark Structured Streaming application.
- `spark_stream_trigger5.py`: Tests a 5-second processing trigger.
- `spark_stream_tumbling2.py`: Tests 2-minute tumbling windows.
- `spark_stream_sliding.py`: Tests 2-minute sliding windows with a 1-minute slide.
- `spark_stream_watermark.py`: Tests a 2-minute watermark.
- `spark_watermark_isolated.py`: Isolated watermark and checkpoint recovery experiment.
- `spark_late_alert.py`: Detects check-in events after 08:15 Algeria local time.
- `spark_event_analysis.py`: Aggregates events by event type and department.
- `spark_rate_test.py`: Measures Spark batch processing throughput.
- `spark_stream_120.py`: Processes the 120-event experiment.
- `flink_window.sql`: Flink Kafka source table and event-time window query.
- `requirements.txt`: Python dependencies.
- `checkpoint/`: Spark checkpoint data.

## 4. Kafka Configuration

The project reuses the existing Kafka broker from TP05.

- Kafka container: `tp05-kafka`
- Bootstrap server: `localhost:9092`
- Topic: `campus-events`
- Partitions: 3
- Replication factor: 1

The TP05 Kafka configuration is reused without modifying its setup.

## 5. Python Environment Setup

Create and activate the virtual environment:

```bash
python3.12 -m venv .venv312
source .venv312/bin/activate
```

Install dependencies:

```bash
pip install -r requirements.txt
```

## 6. Running the Producer

```bash
python producer.py
```

The producer sends JSON events containing:

- eventId
- studentId
- eventType
- department
- eventTime

The dataset includes an intentionally out-of-order event.

## 7. Running Spark Structured Streaming

From the project directory:

```bash
./.venv312/bin/spark-submit \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3 \
  spark_stream.py
```

The application:

1. Reads JSON events from Kafka.
2. Parses events using a predefined schema.
3. Converts eventTime into a timestamp.
4. Filters check-in events.
5. Groups events by department in one-minute event-time windows.
6. Uses a 30-second watermark.
7. Writes results to the console in update mode.

The application uses a persistent checkpoint directory for recovery.

## 8. Experiments

The following experiments were performed:

- Processing-time trigger: 5 seconds.
- Tumbling event-time window: 2 minutes.
- Sliding event-time window: 2 minutes with a 1-minute slide.
- Watermark comparison and late-event testing.
- Checkpoint recovery after stopping and restarting Spark.
- Late check-in alert using Africa/Algiers local time.
- Processing 120 synthetic events and measuring throughput.
- Event aggregation by type and department.

The 120-event producer test achieved approximately 353.13 events per second in one execution.

## 9. Flink SQL Analysis

Flink SQL uses the Kafka topic `campus-events`.

The SQL implementation includes:

- A Kafka source table named `campus_events`.
- Event-time extraction from the `eventTime` field.
- A 2-minute watermark.
- A 2-minute tumbling window.
- A count of check-in events grouped by department.

To execute the SQL statements, start the Flink SQL Client:

```bash
~/flink/bin/sql-client.sh
```

Then execute the `CREATE TABLE` statement and the `SELECT` query from `flink_window.sql` separately.

## 10. Checkpoint and Recovery

Spark checkpoints store streaming progress and state information.

The recovery experiment demonstrated that Spark resumed processing from the saved checkpoint rather than restarting the batch sequence from zero.

Existing checkpoint directories should be preserved.

## 11. Screenshots

The `screenshots/` directory contains evidence of:

- Kafka topic configuration.
- Producer execution.
- Spark streaming output.
- Window experiments.
- Watermark and late-event testing.
- Checkpoint recovery.
- Flink SQL window results.

## 12. Conclusion

This project demonstrates an end-to-end real-time data processing pipeline using Kafka, Spark Structured Streaming, and Flink SQL.

It illustrates event-time processing, window aggregation, watermarks, late-event handling, throughput measurement, and checkpoint-based recovery.
