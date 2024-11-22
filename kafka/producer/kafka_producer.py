from kafka import KafkaProducer
import time
import csv

# Kafka Producer configuration
producer = KafkaProducer(bootstrap_servers='localhost:9092')
topic_name = 'hospital-utilization'

def publish_data(file_path):
    with open(file_path, 'r') as file:
        csv_reader = csv.reader(file)
        header = next(csv_reader)  # Skip header row
        batch = []

        for row in csv_reader:
            batch.append(','.join(row))
            if len(batch) == 100:
                for record in batch:
                    producer.send(topic_name, value=record.encode('utf-8'))
                batch = []
                time.sleep(10)  # 10-second delay

        # Send any remaining records
        for record in batch:
            producer.send(topic_name, value=record.encode('utf-8'))

file_path = 'data/cleaned/hospital-utilization-trends-cleaned.csv'
publish_data(file_path)
