from kafka import KafkaConsumer, KafkaProducer

consumer = KafkaConsumer('logs-input', bootstrap_servers='localhost:9092')
producer = KafkaProducer(bootstrap_servers='localhost:9092')

print("Log Processor Running ...")

for msg in consumer:
    log = msg.value.decode()
    if "ERROR" not in log :  continue

    formatted = f"[ALERT] {log.upper()}"
    producer.send('logs-error-output', value=formatted.encode())
    print(f"Processed: {formatted}")