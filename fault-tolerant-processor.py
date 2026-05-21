from kafka import KafkaConsumer

consumer = KafkaConsumer(
    'fault-input',
    bootstrap_servers='localhost:9092',
    auto_offset_reset='earliest'
)

state_store = {}

log_file = "changelog.txt"

try:
    with open(log_file, "r") as f:
        for line in f:
            key, value = line.strip().split(":")
            state_store[key] = int(value)
    print("Recovered State:", state_store)
except FileNotFoundError:
    print("Starting Fresh ...")


for msg in consumer:
    key = msg.value.decode()
    state_store.setdefault(key,0)
    state_store[key] += 1
    with open(log_file, "w") as f:
        for k,v in state_store.items():
            f.write(f"{k}:{v}\n")
    print("Updated State: ", state_store)