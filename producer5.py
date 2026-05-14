from kafka import KafkaProducer

producer = KafkaProducer(bootstrap_servers='localhost:9092')

producer.send('connect-demo', value=b'sink test 1')
producer.send('connect-demo',value=b'sink test 2')

producer.flush()