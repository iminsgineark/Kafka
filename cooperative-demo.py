from kafka import KafkaConsumer

consumer = KafkaConsumer(
    'orders',
    bootstrap_servers='localhost:9092',
    group_id='cooperative-demo',
    auto_offset_reset='earliest',
    enable_auto_commit=True
)

print("Consumer Started ...")

while True:
    records = consumer.poll(timeout_ms=1000)

    for tp, msgs in records.items():
        for msg in msgs:
            print(f"Partition {tp.partition}: {msg.value.decode('utf-8')}")