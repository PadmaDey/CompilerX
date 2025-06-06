## reading all the dumped data from mongodb collection in kafka topic

from kafka import KafkaConsumer
from bson import json_util
import json

# Kafka configuration
bootstrap_servers = 'localhost:9092'
topic_name = 'company'

# Create Kafka consumer
consumer = KafkaConsumer(
    topic_name,
    bootstrap_servers=bootstrap_servers,
    auto_offset_reset='earliest',
    enable_auto_commit=True,
    group_id='my-group',
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

# Read messages from Kafka topic
print("Reading messages from Kafka topic...")
for message in consumer:
    # Deserialize message value from JSON
    document = message.value
    print("Received message from Kafka:", document)
