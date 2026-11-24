
-- TP06: Flink Event-Time Window Analysis
-- Kafka source: campus-events
-- Event time: eventTime (ISO-8601 UTC)
-- Watermark delay: 2 minutes
-- Window type: Tumbling Window
-- Window duration: 2 minutes

-- 1. Create Kafka source table
CREATE TABLE campus_events (
    eventId INT,
    studentId STRING,
    eventType STRING,
    department STRING,
    eventTime STRING,

    event_ts AS TO_TIMESTAMP(
        SUBSTRING(eventTime, 1, 19),
        'yyyy-MM-dd''T''HH:mm:ss'
    ),

    WATERMARK FOR event_ts AS
        event_ts - INTERVAL '2' MINUTE
) WITH (
    'connector' = 'kafka',
    'topic' = 'campus-events',
    'properties.bootstrap.servers' = 'localhost:9092',
    'properties.group.id' = 'flink-campus-group',
    'scan.startup.mode' = 'earliest-offset',
    'format' = 'json'
);

-- 2. Count check-in events per department
-- using 2-minute Tumbling Event-Time Windows

SELECT
    window_start,
    window_end,
    department,
    COUNT(*) AS total_checkins
FROM TABLE(
    TUMBLE(
        TABLE campus_events,
        DESCRIPTOR(event_ts),
        INTERVAL '2' MINUTE
    )
)
WHERE eventType = 'check-in'
GROUP BY
    window_start,
    window_end,
    department;
