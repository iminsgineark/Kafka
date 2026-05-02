from kafka import KafkaProducer
import json

def custom_serializer(data):
    serialized = f"{data['order_id']} | {data['amount']}"
    return serialized.encode('utf-8')

producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=custom_serializer
)

producer.send('orders',value={"order_id":1, "amount":600})
producer.send('orders',value={"order_id":2, "amount":200})

producer.flush()