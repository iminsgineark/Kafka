from kafka import KafkaProducer
import time

producer = KafkaProducer(bootstrap_servers='localhost:9092')

logs = [
    "INFO: system Running",
    "Error: Disk Full",
    "Warning: High CPU",
    "Error : Memory Leak Detected"
]

for log in logs:
    producer.send('logs-input', value=log.encode())
    time.sleep(1)

producer.flush()