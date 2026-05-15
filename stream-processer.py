from kafka import KafkaConsumer, KafkaProducer

consumer = KafkaConsumer('stream-input', bootstrap_servers="localhost:9092",auto_offset_reset="earliest")

producer = KafkaProducer(bootstrap_servers='localhost:9092')

print("Stream Processor Running...")

for msg in consumer:
    value = msg.value.decode()
    processed_value = value.upper()
    producer.send('stream-output', value=processed_value.encode())
    print(f"Processed: {value} -> {processed_value}")