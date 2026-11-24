
import json
import time
from datetime import datetime, timezone
from kafka import KafkaProducer

producer = KafkaProducer(
    bootstrap_servers="localhost:9092",
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    acks="all"
)

departments = ["Computer Science", "Mathematics", "Physics"]
event_types = ["check-in", "open-course", "library-entry"]

total = 120
start = datetime.now(timezone.utc).replace(microsecond=0)

begin = time.time()

for i in range(total):
    event = {
        "eventId": 20000 + i,
        "studentId": f"S{2000 + i}",
        "eventType": event_types[i % len(event_types)],
        "department": departments[(i // 3) % len(departments)],
        "eventTime": start.isoformat()
    }

    producer.send("campus-events", event)

producer.flush()
elapsed = time.time() - begin
producer.close()

print(f"Total events sent: {total}")
print(f"Elapsed time: {elapsed:.3f} seconds")
print(f"Producer throughput: {total / elapsed:.2f} events/second")
