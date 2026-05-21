from kafka import KafkaProducer

producer = KafkaProducer(bootstrap_servers='localhost:9092')

for i in range(10,20):
    producer.send('scale-demo',value=f"msg-{i}".encode())

producer.flush()