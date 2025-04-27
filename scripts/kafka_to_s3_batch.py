from kafka import KafkaConsumer
import boto3
import json

# S3 client setup
s3 = boto3.client(
    "s3",
    endpoint_url="http://minio:9000",
    aws_access_key_id="admin",
    aws_secret_access_key="password",
    region_name="us-east-1"
)

# Kafka consumer setup
def safe_json(x):
    try:
        return json.loads(x.decode("utf-8"))
    except:
        return None

consumer = KafkaConsumer(
    "test-topic",
    bootstrap_servers="localhost:9092",
    group_id="s3-uploader-batch",
    auto_offset_reset="earliest",
    enable_auto_commit=True,
    value_deserializer=safe_json
)

print("📦 Collecting all messages into one record...")

all_records = []

try:
    for message in consumer:
        if message.value is not None:
            all_records.append(message.value)

        
        if len(all_records) >= 100_000:
            break

except KeyboardInterrupt:
    print("⛔ Interrupted. Proceeding to upload.")

# Upload as single JSON array
final_json = json.dumps(all_records, indent=2)
s3.put_object(Bucket="iceberg-bucket", Key="hse_data_batch.json", Body=final_json)

print(f"✅ Uploaded all {len(all_records)} records as hse.json")
