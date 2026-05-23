from kafka import KafkaProducer
import json
producer = KafkaProducer(bootstrap_servers='localhost:9092',value_serializer=lambda v : json.dumps(v).encode())

correlation_id = "12345"

producer.send('request-topic', value={'action' : "process_order", "order_id" : 1}, headers=[('correlation_id', correlation_id.encode())])

print(f"Request Sent with correlation _id : {correlation_id}")

producer.flush()