import json
from kafka import KafkaProducer

# Kafka Producer Configuration
producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

# Load all incidents
with open('test.json', 'r') as file:
    incidents = json.load(file)

# Topic Name
topic_name = 'ppe-injury-cleaned'

# Send all incidents immediately
for incident in incidents:
    producer.send(topic_name, value=incident)

# Flush once at the end
producer.flush()
producer.close()

print(f"✅ Successfully sent {len(incidents)} incidents to Kafka topic '{topic_name}' in one shot!")
