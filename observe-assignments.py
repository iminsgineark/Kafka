from kafka import KafkaConsumer

consumer = KafkaConsumer(
    'orders',
    bootstrap_servers='localhost:9092',
    group_id='coordinator-demo'
)

while True:
    consumer.poll(timeout_ms=1000)
    print("Assigned Partitions:",consumer.assignment())