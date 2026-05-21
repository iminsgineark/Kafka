from kafka import KafkaConsumer

import time

consumer = KafkaConsumer(
    'metrics-demo',
    bootstrap_servers="localhost:9092",
    auto_offset_reset='earliest',
    group_id='metrics-group'
)

for msg in consumer:
    print(f"Consumed : {msg.value.decode()}")
    time.sleep(1)