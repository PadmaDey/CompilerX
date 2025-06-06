## creating kafka topic to read the mongodb collection and dump all of the records

from kafka import KafkaProducer
from pymongo import MongoClient
from bson import json_util

# MongoDB configuration
mongo_uri = 'mongodb://Compilerx_test_user:9Xlnz8a6jG6bMt85aC9ml_NhAdv@46.137.68.213:27017/compilerx_test?authSource=admin'
db_name = 'compilerx_test'
collection_name = 'company'

# Kafka configuration
bootstrap_servers = 'localhost:9092'
topic_name = 'company'

# Create Kafka producer
producer = KafkaProducer(bootstrap_servers=bootstrap_servers, value_serializer=lambda v: json_util.dumps(v).encode('utf-8'))

# Connect to MongoDB
client = MongoClient(mongo_uri)
db = client[db_name]
collection = db[collection_name]

# Consume messages from MongoDB collection and produce to Kafka topic
for document in collection.find():
    # Convert MongoDB document to dictionary
    document_dict = json_util.loads(json_util.dumps(document))
    
    # Send document to Kafka topic
    producer.send(topic_name, value=document_dict)
    print("Produced message to Kafka:", document_dict)

# Flush and close Kafka producer
producer.flush()
producer.close()
