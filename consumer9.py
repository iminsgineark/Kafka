from kafka import KafkaConsumer
import json

consumer = KafkaConsumer('payment-events', bootstrap_servers='localhost:9092',auto_offset_reset='earliest',value_deserializer=lambda x: json.loads(x.decode()))

processed_txns = set()
balance = 0

for msg in consumer:
    event = msg.value
    print("Received:", event)
    txn_id = event["txn_id"]

    if txn_id in processed_txns:
        print(f"Duplicate Ignored: {txn_id}")
        continue
    processed_txns.add(txn_id)
    balance += event["amount"]
    print(f"Processed: {event}, Balance: {balance}")