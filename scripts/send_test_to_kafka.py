import json
from kafka import KafkaProducer

producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

test_data = [
    {"timestamp": "2025-04-27T10:00:00Z", "worker_id": "W001", "device_id": "cam_001", "ppe_detected": True, "hazard_zone": False, "incident_reported": False},
    {"timestamp": "2025-04-27T10:01:00Z", "worker_id": "W002", "device_id": "cam_002", "ppe_detected": False, "hazard_zone": True, "incident_reported": True}
]

for record in test_data:
    producer.send('ppe-injury-cleaned', value=record)

producer.flush()
producer.close()

print("✅ Sent test records to Kafka!")
