import json
from datetime import datetime, timedelta, timezone
from kafka import KafkaProducer

TOPIC = "campus-events"

producer = KafkaProducer(
    bootstrap_servers="localhost:9092",
    value_serializer=lambda value: json.dumps(value).encode("utf-8"),
    acks="all"
)

base = datetime.now(timezone.utc).replace(microsecond=0)

def send_event(event_id, student, event_time):
    event = {
        "eventId": event_id,
        "studentId": student,
        "eventType": "check-in",
        "department": "Computer Science",
        "eventTime": event_time.isoformat()
    }

    producer.send(TOPIC, value=event)
    producer.flush()
    print("Sent:", event, flush=True)


try:
    # STEP 1: Send a normal event
    print("\nSTEP 1: Sending normal event")
    send_event(9001, "S901", base)

    input("\nWait until Spark processes this event, then press Enter...")

    # STEP 2: Advance event time by 5 minutes
    print("\nSTEP 2: Advancing event time by 5 minutes")
    send_event(9002, "S902", base + timedelta(minutes=5))

    input("\nWait until Spark processes this event, then press Enter...")

    # STEP 3: Send an event that is now too late
    print("\nSTEP 3: Sending late event")
    send_event(9003, "S903", base)

    print("\nWatermark test events sent successfully.")

finally:
    producer.close()