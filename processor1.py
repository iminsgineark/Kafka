from kafka import KafkaConsumer

consumer = KafkaConsumer(
    'eos-input',
    bootstrap_servers='localhost:9092',
    auto_offset_reset='earliest'
)

processed = []

for msg in consumer:
    txn = msg.value.decode()
    processed.append(txn)
    print(f"Processed: {processed}")