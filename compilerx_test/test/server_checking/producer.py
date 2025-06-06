from kafka import KafkaProducer
import json

# Create a Kafka producer
producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

# Send a test message to the 'test' topic
producer.send('test', {'key': 'value'})

# Block until all pending messages are sent
producer.flush()

print("Message sent!")
print("msg 2")