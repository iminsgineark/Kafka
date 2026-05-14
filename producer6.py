from kafka import KafkaProducer

prodcuer = KafkaProducer(bootstrap_servers='localhost:9092')

for i in range(20):
    prodcuer.send('parallel-demo',value=f"msg-{i}".encode())

prodcuer.flush()

print("Message Sent")