from kafka import KafkaConsumer

consumer = KafkaConsumer(
    'orders',
    bootstrap_servers='localhost:9092',
    group_id='group-1'
)

for msg in consumer:
    print("Consumer-2:", msg.value)