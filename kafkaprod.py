from kafka import KafkaProducer
import json

producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

try:
    future = producer.send('invalid_topic',value={"msg":"test"})
    future.get(timeout=5)
except Exception as e:
    print("Error Occurred:", e)

producer.flush()