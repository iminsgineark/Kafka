from kafka import KafkaProducer

producer = KafkaProducer(bootstrap_servers='localhost:9092')

for i in range(5):
    producer.send('replay-demo', value=f"event-{i}".encode())
    print(f"Produced: event-{i}")

producer.flush()