from kafka import KafkaConsumer
import json

consumer = KafkaConsumer('response-topic', bootstrap_servers='localhost:9092', auto_offset_reset='earliest', value_deserializer=lambda x: json.loads(x.decode()))

for msg in consumer:
    response = msg.value
    headers = dict(msg.headers)

    correlation_id = headers.get("correlation_id").decode()

    print(f"Received Response : {response} with correlation_id : {correlation_id}")