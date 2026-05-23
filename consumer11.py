from kafka import KafkaConsumer
import json
import time

consumer = KafkaConsumer('order-events', bootstrap_servers='localhost:9092',auto_offset_reset='earliest',value_deserializer=lambda x: json.loads(x.decode()))

for msg in consumer:
    order = msg.value

    print(f"Processng Payment for Order: {order}")
    time.sleep(3)
    print(f"Payment Completed for Order : {order['order_id']}")