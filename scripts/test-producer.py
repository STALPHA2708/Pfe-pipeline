from kafka import KafkaProducer
import json

producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

for msg in [{"device": "helmet"}, {"device": "vest"}, {"device": "gloves"}]:
    producer.send("test-topic", msg)

producer.flush()
print("Messages sent to Kafka.")