from kafka.admin import KafkaAdminClient, NewTopic

# Kafka configuration
bootstrap_servers = 'localhost:9092'
topics = {
    'company': 3,           # 'company' topic with 3 partitions
    'contact': 3,           # 'contact' topic with 3 partitions
    'contact_details': 3    # 'contact_details' topic with 3 partitions
}

# Create an Admin client
admin_client = KafkaAdminClient(
    bootstrap_servers=bootstrap_servers,
    client_id='test_client'
)

# Create topics
new_topics = [NewTopic(name=topic, num_partitions=partitions, replication_factor=1) for topic, partitions in topics.items()]
admin_client.create_topics(new_topics)

print("Topics created successfully")
admin_client.close()
