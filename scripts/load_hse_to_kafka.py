from kafka import KafkaProducer
import json

producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

with open('hse_data_100k.json') as f:
    for line in f:
        try:
            message = json.loads(line.strip())
            producer.send("test-topic", message)
        except Exception as e:
            print("Skipping bad line:", e)

producer.flush()
print("✅ All messages sent to Kafka.")
