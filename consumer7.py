from kafka import KafkaConsumer
import json

consumer = KafkaConsumer('payment-events', bootstrap_servers='localhost:9092', auto_offset_reset='earliest', value_deserializer=lambda v: json.loads(v.decode()))

balance = 0

for msg in consumer:
    event = msg.value
    balance += event['amopunt']
    print(f"Current balance: {balance}")