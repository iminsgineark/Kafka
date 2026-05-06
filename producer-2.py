from kafka import KafkaProducer

producer = KafkaProducer(bootstrap_servers='localhost:9092')

for i in range(1000):
    producer.send('disk-demo', value=f"msg-{i}".encode())

producer.flush()
