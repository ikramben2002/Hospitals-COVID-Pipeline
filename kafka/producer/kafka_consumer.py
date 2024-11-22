from kafka import KafkaConsumer

# Kafka Consumer configuration
topic_name = 'hospital-utilization'
consumer = KafkaConsumer(
    topic_name,
    bootstrap_servers='localhost:9092',
    auto_offset_reset='earliest',  # Start from the beginning of the topic
    enable_auto_commit=True,
    group_id='hospital-consumers',
    value_deserializer=lambda x: x.decode('utf-8')  # Decode messages
)

print("Kafka Consumer est prêt. Lecture des messages...")

# Lire les messages en continu
try:
    for message in consumer:
        print(f"Message reçu : {message.value}")
except KeyboardInterrupt:
    print("Consumer arrêté.")
