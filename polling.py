from kafka import KafkaConsumer

consumer = KafkaConsumer(
    'orders',
    bootstrap_servers='localhost:9092',
    group_id='continuous-demo',
    auto_offset_reset='earliest'
)

while True:
    records = consumer.poll(timeout_ms=1000)
    for tp, messages in records.items():
        print(f"\nBatch from {tp}:")
        for message in messages:
            print("Received: ",message.value)