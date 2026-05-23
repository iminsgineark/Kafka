from kafka import KafkaProducer
import json

producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode()
)

events = [
    {"txn_id": "TXN1", "amount": 100},
    {"txn_id": "TXN2", "amount": 200}
]

for event in events:
    producer.send('payment-events', value=event)
    print(f"Sent event: {event}")

producer.flush()