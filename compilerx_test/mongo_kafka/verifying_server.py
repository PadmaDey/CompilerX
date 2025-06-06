from kafka import KafkaProducer

def check_kafka_server():
    try:
        producer = KafkaProducer(bootstrap_servers='localhost:9092')
        producer.close()
        print("Kafka server is running")
    except Exception as e:
        print(f"Error: {e}")

check_kafka_server()
