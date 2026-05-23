from kafka import KafkaConsumer, KafkaProducer
import json

consumer = KafkaConsumer(
    'request-topic',
    bootstrap_servers='localhost:9092',
    auto_offset_reset='earliest',
    value_deserializer=lambda x: json.loads(x.decode())
)

producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v : json.dumps(v).encode()
)

for msg in consumer:
    request = msg.value
    headers = dict(msg.headers)
    correlation_id = headers.get("correlation_id").decode()
    print(f"Processing Request with correlation_id: {correlation_id}")

    response = {
        "status" : "Processed ",
        "order_id" : request["order_id"]
    }

    producer.send('response-topic',value=response, headers=[("correlation_id", correlation_id.encode())])
    print(f"Response Sent With Correlation_id: {correlation_id}")

    producer.flush()