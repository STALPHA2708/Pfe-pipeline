import requests
import fitz  # PyMuPDF for PDF parsing
from kafka import KafkaProducer
import json
import time

# Kafka producer config
producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

# Download real HSE report PDF
# pdf_url = 'https://www.hse.gov.uk/statistics/pdf/fatalinjuries.pdf'
pdf_path = 'fatal_injuries_hse.pdf'

# Fetch and save PDF
# response = requests.get(pdf_url)
# with open(pdf_path, 'wb') as f:
#     f.write(response.content)
# Open and parse PDF
doc = fitz.open(pdf_path)

print("✅ HSE PDF opened successfully.")

# Keywords related to PPE compliance
ppe_keywords = ["helmet", "hard hat", "gloves", "hi-vis", "goggles", "fall harness", "respirator"]

# Process PDF pages
for page in doc:
    text = page.get_text()

    for line in text.split('\n'):
        line_lower = line.lower()
        if any(keyword in line_lower for keyword in ppe_keywords):
            incident_record = {
                "incident_summary": line.strip(),
                "ppe_detected": [keyword for keyword in ppe_keywords if keyword in line_lower],
                "source_page": page.number
            }
            # Send incident to Kafka topic
            producer.send('ppe-injury-cleaned', value=incident_record)
            print(f"✅ Sent incident: {incident_record}")

producer.flush()
print("✅ All incidents sent to Kafka topic 'ppe-injury-cleaned'.")