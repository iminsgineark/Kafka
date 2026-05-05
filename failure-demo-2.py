from kafka import KafkaConsumer
import time

consumer = KafkaConsumer(
    'orders',
    bootstrap_servers='localhost:9092',
    group_id="failure-demo-2",
    enable_auto_commit=False
)

for messge in consumer:
    consumer.commit()
    print("Processing: ", messge.value)
    time.sleep(5)