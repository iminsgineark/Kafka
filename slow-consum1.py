from kafka import KafkaConsumer
import time

consumer = KafkaConsumer(
    'lag-demo',
    bootstrap_servers='localhost:9092',
    group_id='lag-group',
    auto_offset_reset='earliest'
)

for msg in consumer:
    print(f"Consumed: {msg.value.decode()}")
    time.sleep(1)