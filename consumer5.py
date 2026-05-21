from kafka import KafkaConsumer
import json

consumer = KafkaConsumer('bank-events', bootstrap_servers='localhost:9092',auto_offset_reset='earliest', value_deserializer=lambda m: json.loads(m.decode()))

balance = 0
for msg in consumer:
    event = msg.value

    if event['type'] == 'deposit':
        balance += event['amount']
    elif event['type'] == 'Withdraw':
        balance -= event['amount']
    
    print(f"Processed Event : {event}, Current Balance: {balance}")