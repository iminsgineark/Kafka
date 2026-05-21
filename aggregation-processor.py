from kafka import KafkaConsumer

consumer = KafkaConsumer(
    'aggregation-input',
    bootstrap_servers='localhost:9092',
    auto_offset_reset='earliest'
)

error_count = 0

for msg in consumer:
    event = msg.value.decode()
    if event == "error":
        error_count += 1
    print(f"Current Error Count: {error_count}")