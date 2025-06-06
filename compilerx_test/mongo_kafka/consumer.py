from kafka import KafkaConsumer
from bson import json_util

# Kafka configuration
bootstrap_servers = 'localhost:9092'
topics = ['contacts', 'contacts_details']

# Create Kafka consumer
consumer = KafkaConsumer(
    *topics,
    bootstrap_servers=bootstrap_servers,
    auto_offset_reset='earliest',  # Start reading from the beginning of the partition
    enable_auto_commit=True,       # Commit offsets automatically
    group_id='test_consumer_group' # Specify a consumer group
)

# Subscribe to topics
consumer.subscribe(topics)

# Continuously poll for messages
print("started reading the messages")

try:
    for message in consumer:
        # Decode and print the message value
        print("Received message from Kafka:")
        print(json_util.loads(message.value))
except KeyboardInterrupt:
    # Handle keyboard interrupt gracefully
    print("\nConsumer interrupted. Closing...")
finally:
    # Close the consumer
    consumer.close()
