from kafka import KafkaConsumer
import boto3
import json
import random

# S3 client setup
s3 = boto3.client(
    "s3",
    endpoint_url="http://localhost:4566",
    aws_access_key_id="user",
    aws_secret_access_key="password",
    region_name="us-east-1"
)

# Safer deserializer
def safe_json(x):
    try:
        return json.loads(x.decode("utf-8"))
    except Exception as e:
        print("❌ Skipping invalid message:", x)
        return None

# Kafka consumer setup
consumer = KafkaConsumer(
    "test-topic",
    bootstrap_servers="localhost:9092",
    group_id=f"s3-batch-{random.randint(0,99999)}",
    auto_offset_reset="earliest",
    enable_auto_commit=False,
    value_deserializer=lambda x: json.loads(x.decode("utf-8"))
)

print("✅ Listening for messages...")

for message in consumer:
    if message.value is None:
        continue  # Skip bad messages

    key = f"topics/test-topic/partition=0/record-{message.offset}.json"
    value = json.dumps(message.value)

    s3.put_object(Bucket="iceberg-bucket", Key=key, Body=value)
    print(f"✅ Uploaded to S3: {key}")
