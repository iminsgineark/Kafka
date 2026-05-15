from kafka import KafkaProducer

import time

producer = KafkaProducer(bootstrap_servers='localhost:9092')
events = ["error","info", "error", "Warning","Error"]

for event in events:
    producer.send('aggregation-input',value=event.encode())
    print(f"Produced: {event}")
    time.sleep(1)

producer.flush()