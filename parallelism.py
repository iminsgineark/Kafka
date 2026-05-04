import time
from kafka import KafkaConsumer

consumer = KafkaConsumer(
    'orders',
    bootstrap_servers='localhost:9092',
    group_id='group-parallel'
)

for msg in consumer:
    print("Processing:", msg.value)
    time.sleep(2)