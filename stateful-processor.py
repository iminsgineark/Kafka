from kafka import KafkaConsumer

consumer = KafkaConsumer(
    'state-input',
    bootstrap_servers='localhost:9092',
    auto_offset_reset='earliest'
)

state_store = {}

for msg in consumer:
    key = msg.value.decode()
    if key not in state_store:
        state_store[key] = 0
    state_store[key] += 1
    print(f"Updated State : {state_store}")