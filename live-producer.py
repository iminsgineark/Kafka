import time
from kafka import KafkaProducer

producer = KafkaProducer(bootstrap_servers='localhost:9092')

i=0

while True:
    producer.send('disk-demo', value=f"live-{i}".encode())
    i += 1
    time.sleep(0.001)