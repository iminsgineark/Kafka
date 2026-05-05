from kafka import KafkaConsumer
import time

consumer=KafkaConsumer(
    'orders',
    bootstrap_servers='localhost:9092',
    group_id='rebalance-heartbeat'
)

print("Consumer Started...")

while True:
    consumer.poll(timeout_ms=1000)
    print("Processing...")
    time.sleep(20)