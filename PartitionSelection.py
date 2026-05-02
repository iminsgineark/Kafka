from kafka import KafkaProducer

def custom_partitioner(key_bytes, all_partitions, available_partitions):
    if key_bytes == b'priority':
        return available_partitions[0]
    return all_partitions[-1]

producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    partitioner=custom_partitioner
)

for i in range(5):
    key = b'priority' if i % 2 == 0 else b'normal'
    value = f"order - {i}".encode()

    producer.send('orders',key=key, value=f"order - {i}".encode())

producer.flush()