from kafka import KafkaProducer, KafkaConsumer

consumer = KafkaConsumer('stateless-input', bootstrap_servers='localhost:9092', auto_offset_reset='earliest')
producer = KafkaProducer(bootstrap_servers="localhost:9092")

print("Stateless Processor Running ...")

for msg in consumer:
    value = msg.value.decode()

    if "error" not in value: continue

    processed = value.upper()

    words = processed.split()
    for word in words:
        producer.send('stateless-output', value=word.encode())
        print(f"Processed: {word}")