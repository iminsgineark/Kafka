from kafka import KafkaProducer
import time

producer = KafkaProducer(bootstrap_servers='localhost:9092')

for i in range(20):
    producer.send('delete-demo', value=f"log-{i}".encode())

producer.flush()