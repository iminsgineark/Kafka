from kafka import KafkaConsumer
import time

consumer = KafkaConsumer(
    'orders',
    bootstrap_servers='localhost:9092',
    group_id="failure-demo-1",
    enable_auto_commit=False,
    auto_offset_reset='latest'
)

for message in consumer:
    print("Processing: ", message.value)
    time.sleep(5)
    consumer.commit()