## many collection and 3 partion -- almost 1.5h

from kafka.admin import KafkaAdminClient, NewTopic
from kafka import KafkaProducer
from pymongo import MongoClient
from bson import json_util

# Kafka configuration
bootstrap_servers = 'localhost:9092'
topics = {
    'company': 3,           # 'company' topic with 3 partitions
    'contact': 3,           # 'contact' topic with 3 partitions
    'contact_details': 3    # 'contact_details' topic with 3 partitions
}

# Create Kafka Admin client
admin_client = KafkaAdminClient(
    bootstrap_servers=bootstrap_servers,
    client_id='test_client'
)

# Create topics
new_topics = [NewTopic(name=topic, num_partitions=partitions, replication_factor=1) for topic, partitions in topics.items()]
admin_client.create_topics(new_topics)

print("Topics created successfully")
admin_client.close()

# MongoDB configuration
mongo_uri = 'mongodb://Compilerx_test_user:9Xlnz8a6jG6bMt85aC9ml_NhAdv@46.137.68.213:27017/compilerx_test?authSource=admin'
db_name = 'compilerx_test'
collections = ['contacts', 'contacts_details', 'company']

# Create Kafka producer
producer = KafkaProducer(
    bootstrap_servers=bootstrap_servers,
    value_serializer=lambda v: json_util.dumps(v).encode('utf-8')
)

# Connect to MongoDB
client = MongoClient(mongo_uri)
db = client[db_name]

# Loop through each collection and produce messages to corresponding Kafka topic
for collection_name in collections:
    collection = db[collection_name]
    topic_name = collection_name

    # Consume messages from MongoDB collection and produce to Kafka topic
    for document in collection.find():
        # Convert MongoDB document to dictionary
        document_dict = json_util.loads(json_util.dumps(document))

        # Send document to Kafka topic
        producer.send(topic_name, value=document_dict)
        print(f"Produced message to Kafka topic '{topic_name}':", document_dict)

# Flush and close Kafka producer
producer.flush()
producer.close()
