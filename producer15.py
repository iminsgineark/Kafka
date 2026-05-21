from kafka import KafkaProducer
import time
import json

producer = KafkaProducer(bootstrap_servers='localhost:9092',value_serializer=lambda v: json.dumps(v).encode())

transactions = [
    {"account_id" : "A1", "Type" : "Deposit", "amount" : 100},
    {"account_id" : "A1", "Type" : "Withdraw", "amount" : 400},
    {"account_id" : "A1", "Type" : "Deposit", "amount" : 600}
]

for txn in transactions:
    producer.send('bank-transactions', value=txn)
    print(f"Event Sent : {txn}")
    time.sleep(1)

producer.flush()