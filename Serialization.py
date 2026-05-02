from kafka import KafkaProducer
import json

producer = KafkaProducer(
    bootstrap_servers="localhost:9092",
    key_serializer=lambda k : k.encode('utf-8'),
    value_serializer=lambda v : json.dumps(v).encode('utf-8')
)
producer.send(
    'orders',
    key='user1',
    value={"order_id" : 2, "amount": 200}
)
producer.flush()
