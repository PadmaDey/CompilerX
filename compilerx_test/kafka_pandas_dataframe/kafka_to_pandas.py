from kafka import KafkaConsumer
import pandas as pd
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

# Initialize a list to store the messages
messages = []

# Read messages from Kafka topic
print("Reading messages from Kafka topic...")
for message in consumer:
    # Deserialize message value from JSON
    document = message.value
    print("Received message from Kafka:", document)
    
    # Append the document to the list
    messages.append(document)

    # For demonstration purposes, break after reading a few messages
    if len(messages) >= 2:  # Adjust the number as needed
        break

# Convert the list of messages to a pandas DataFrame
df = pd.DataFrame(messages)

# Display the DataFrame
print("\nDataFrame:")
df
