from kafka import KafkaConsumer

consumer = KafkaConsumer(
    'orders',
    bootstrap_servers='localhost:9092',
    group_id='reprocess-demo',
    auto_offset_reset='earliest'
)

for msg in consumer:
    print (msg.value)
    