from kafka import KafkaConsumer

consumer = KafkaConsumer(
    'orders',
    bootstrap_servers='localhost:9092',
    group_id='safe-pattern',
    enable_auto_commit=False
)

for msg in consumer:
    print("Processing: ",msg.value)
    consumer.commit()