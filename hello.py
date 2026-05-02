from kafka import KafkaProducer

producer = KafkaProducer(bootstrap_servers='localhost:9092')
producer.send('orders',b'Hello From Kafka')
producer.flush()