from kafka import KafkaConsumer
import time

consumer = KafkaConsumer('window-input', bootstrap_servers='localhost:9092', auto_offset_reset='earliest')

window_size = 5
window_start = time.time()
count = 0

for msg in consumer:
    event = msg.value.decode()
    if event == "error":
        count += 1
    if time.time() - window_start >= window_size:
        print(f"Window Result (last {window_size}s) : errors = {count}")
        window_start = time.time()
        count=0