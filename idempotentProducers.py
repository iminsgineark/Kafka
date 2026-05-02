from kafka import KafkaProducer

p1 = KafkaProducer(
    bootstrap_servers="localhost:9092",
 
)
p2 = KafkaProducer(
    bootstrap_servers="localhost:9092",
    enable_idempotence=True
)

for _ in range(3):
    p1.send(
        'orders',
        value=b'non-idempotent'
    )

for _ in range(3):
    p2.send(
        'orders',
        value=b'idempotent'
    )
p1.flush()
p2.flush()