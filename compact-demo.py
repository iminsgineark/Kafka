from kafka import KafkaProducer

producer = KafkaProducer(bootstrap_servers='localhost:9092')

producer.send('mixed-demo',key=b'user1',value=b'v1')
producer.send('mixed-demo',key=b'user1',value=b'v2')
producer.send('mixed-demo',key=b'user2',value=b'v3')

producer.flush()