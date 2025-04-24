Invoke-RestMethod -Uri "http://localhost:8083/connectors" `
  -Method POST `
  -Headers @{ "Content-Type" = "application/json" } `
  -Body '{
    "name": "s3-sink",
    "config": {
      "connector.class": "io.confluent.connect.s3.S3SinkConnector",
      "tasks.max": "1",
      "topics": "test-topic",
      "s3.bucket.name": "iceberg-bucket",
      "s3.region": "us-east-1",
      "flush.size": "3",
      "storage.class": "io.confluent.connect.s3.storage.S3Storage",
      "format.class": "io.confluent.connect.s3.format.json.JsonFormat",
      "partitioner.class": "io.confluent.connect.storage.partitioner.DefaultPartitioner",
      "schema.compatibility": "NONE",
      "s3.endpoint": "http://localstack:4566",
      "s3.path.style.access": "true",
      "aws.access.key.id": "user",
      "aws.secret.access.key": "password"
    }
  }'