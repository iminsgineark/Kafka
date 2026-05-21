from kafka import KafkaProducer

producer = KafkaProducer(bootstrap_servers = 'localhost:9092')

for i in range(10):
    producer.send('scale-demo', value=f"msg-{i}".encode())
    print(f"Produced: msg{i}")

producer.flush()