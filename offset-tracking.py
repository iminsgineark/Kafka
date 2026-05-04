from kafka import KafkaConsumer

consumer = KafkaConsumer(
    'orders',
    bootstrap_servers='localhost:9092',
    group_id='batch-manual-failure',
    enable_auto_commit=False,
)

while True:
    records = consumer.poll(timeout_ms=2000)
    for tp, messages in records.items():
        for message in messages:
            print("Processing: ", message.value)

    consumer.commit()


# next topic - Lecture_4_2_03