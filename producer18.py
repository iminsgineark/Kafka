from kafka import KafkaProducer
import json
import time

producer = KafkaProducer(bootstrap_servers='localhost:9092',value_serializer=lambda v: json.dumps(v).encode())

orders = [
    {"order_id" : 1, "amount" : 100},
    {"order_id" : 2, "amount" : 200}
]

for order in orders:
    producer.send('order-events', value=order)
    print(f"Order Created: {order}")
    time.sleep(1)

producer.flush()