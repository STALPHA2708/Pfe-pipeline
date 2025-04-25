from kafka import KafkaProducer
import json

producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

sample_data = [
    {"timestamp": "2025-04-21T12:00:00Z", "worker_id": "W101", "device_id": "cam_001", "ppe_detected": False, "hazard_zone": True, "incident_reported": True},
    {"timestamp": "2025-04-21T12:00:05Z", "worker_id": "W102", "device_id": "cam_002", "ppe_detected": True, "hazard_zone": False, "incident_reported": False},
    {"timestamp": "2025-04-21T12:00:10Z", "worker_id": "W103", "device_id": "cam_003", "ppe_detected": False, "hazard_zone": True, "incident_reported": True},
    {"timestamp": "2025-04-21T12:00:15Z", "worker_id": "W104", "device_id": "cam_004", "ppe_detected": True, "hazard_zone": False, "incident_reported": False},
    {"timestamp": "2025-04-21T12:00:20Z", "worker_id": "W105", "device_id": "cam_005", "ppe_detected": True, "hazard_zone": True, "incident_reported": False},
    {"timestamp": "2025-04-21T12:00:25Z", "worker_id": "W106", "device_id": "cam_006", "ppe_detected": False, "hazard_zone": True, "incident_reported": True}
]

for record in sample_data:
    producer.send("test-topic", record)

producer.flush()
print("✅ Messages sent to Kafka.")
