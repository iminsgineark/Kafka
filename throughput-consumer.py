from kafka import KafkaConsumer

import time
consumer = KafkaConsumer(
    'perf-demo',
    group_id='perf-group',
    auto_offset_reset='earliest'
)

count=0
start = time.time()

for msg in consumer:
    count += 1
    print(f"Consumed: {msg.value.decode()}")
    if count == 50:
        break

print(f"Consumer Throughput : {count/(time.time() - start):.2f} messages/sec")