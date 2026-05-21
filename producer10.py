from kafka import KafkaProducer
import time

producer = KafkaProducer(bootstrap_servers='localhost:9092')
events = ["user1","user2","user1","user3","user1"]

for event in events:
    producer.send('state-input', value=event.encode())
    print(f"Produced : {event}")
    time.sleep(1)
producer.flush()