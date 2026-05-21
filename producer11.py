from kafka import KafkaProducer

producer = KafkaProducer(bootstrap_servers='localhost:9092')

events = ["txn1","txn2","txn1"]

for event in events:
    producer.send('eos-input', value=event.encode())
    print(f"Produced : {event}")

producer.flush()