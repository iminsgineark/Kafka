from kafka import KafkaProducer
import time
producer = KafkaProducer(bootstrap_servers='localhost:9092')
for i in range(30):
    producer.send('lag-demo',value=f"msg-{i}".encode())
    print(f"Produced: msg-{i}")
    time.sleep(0.1)

producer.flush()