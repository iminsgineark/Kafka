from kafka import KafkaProducer
import time

producer = KafkaProducer(bootstrap_servers='localhost:9092')
message = ["error: disk Full",  "info: System running", "error: memory low"]

for msg in message:
    producer.send('stateless-input', value=msg.encode())
    time.sleep(1)

producer.flush()