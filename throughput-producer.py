from kafka import KafkaProducer

import time

producer = KafkaProducer(bootstrap_servers='localhost:9092')
start = time.time()

for i in  range(50):
    producer.send('perf-demo', value=f"msg-{i}".encode())

producer.flush()

throughput = 50 / (time.time() - start)

print(f"Producer Throughput : {throughput:.2f} messages/sec")