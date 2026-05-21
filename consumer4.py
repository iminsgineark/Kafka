from kafka import KafkaConsumer

consumer = KafkaConsumer('scale-demo', bootstrap_servers='localhost:9092',group_id='scale-group',auto_offset_reset='earliest')

for msg in consumer:
    print(f"Consumer - 1 : {msg.value.decode()}")

    