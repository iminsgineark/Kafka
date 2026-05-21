from kafka import KafkaProducer
import json

producer = KafkaProducer(bootstrap_servers='localhost:9092',value_serializer=lambda v: json.dumps(v).encode())

events = [
    {"type" : "deposit","amount": 100},
    {"type" : "Withdraw","amount": 40},
    {"type" : "deposit","amount": 50},
]

for event in events:
    producer.send('bank-events', value=event)
    print(f"Produced event: {event}")


producer.flush()