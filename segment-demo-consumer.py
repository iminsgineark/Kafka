from kafka import KafkaConsumer, TopicPartition

consumer = KafkaConsumer(bootstrap_servers='localhost:9092')
tp = TopicPartition('segment-demo',0)
consumer.assign([tp])
consumer.seek(tp,50)


for msg in consumer:
    print(msg.offset, msg.value)