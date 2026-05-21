from kafka import KafkaConsumer
import json

consumer = KafkaConsumer('bank-transactions', bootstrap_servers='localhost:9092', auto_offset_reset='earliest', value_deserializer=lambda v: json.loads(v.decode()))

balance = {}

for msg in consumer:
    txn = msg.value
    acc = txn['account_id']

    if acc not in balance:
        balance[acc] = 0

    
    if txn['Type'] == 'Deposit':
        balance[acc] += txn['amount']
    elif txn['Type'] == 'Withdraw':
        balance[acc] -= txn['amount']

    print(f"Processed Transaction: {txn}, Current Balance for {acc}: {balance[acc]}")