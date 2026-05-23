from kafka import KafkaConsumer

consumer = KafkaConsumer('replay-demo', bootstrap_servers='localhost:9092',group_id='replay-group',auto_offset_reset='earliest')

for msg in consumer:
    print(f"Consumer: {msg.value.decode()}")