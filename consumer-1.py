from kafka import KafkaConsumer

consumer = KafkaConsumer(
    'orders',
    bootstrap_servers='localhost:9092',
    group_id='coordinator-demo'
)
print("Consumer-1 Started... ")
for msg in consumer:
    print("Consumer-1: ", msg.value)