from kafka import KafkaProducer

producer = KafkaProducer(bootstrap_servers='localhost:9092')

for i in range(10):
    producer.send(
        'orders',
        key=f"tx_{i}".encode(),
        value=f"order-{i}".encode()
    )

import time

for i in range(10):
    composite_key = f"user1_{int(time.time() * 1000)}_{i}".encode()
    producer.send('orders', key=composite_key)
value=f"order-{i}".encode()

producer.flush()
