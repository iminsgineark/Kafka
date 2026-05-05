from kafka import KafkaConsumer

consumer=KafkaConsumer(
    'orders',
    bootstrap_servers='localhost:9092',
    group_id='rebalance-demo',
    auto_offset_reset='earliest'
)

print("Consumer Started...")

while True:
    records=consumer.poll(timeout_ms=1000)
    for tp, messgaes in records.items():
        for msg in messgaes:
            print(f"Partition {tp.partition} : {msg.value}")