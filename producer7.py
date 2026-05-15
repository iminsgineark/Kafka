from kafka import KafkaProducer
import time

producer = KafkaProducer(bootstrap_servers='localhost:9092')

for i in range(10):
    message = f"event-{i}"
    producer.send('stream-input',value=message.encode())
    print(f"Produced: {message}")
    time.sleep(1)

producer.flush()