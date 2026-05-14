from kafka import KafkaProducer

producer = KafkaProducer(bootstrap_servers='localhost:9092')

producer.send('state-demo',key=b'user1', value=b'v1')
producer.send('state-demo',key=b'user1', value=b'v2')
producer.send('state-demo',key=b'user1', value=b'v3')

producer.flush()
