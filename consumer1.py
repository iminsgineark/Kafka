from kafka import KafkaConsumer

def custom_deserializer(data):
    try:
        decoded = data.decode('utf-8')
        order_id, amount = decoded.split('|')
        return {
            "order_id": int(order_id),
            "amount": int(amount)
        }
    except Exception:
        return None

consumer = KafkaConsumer(
    'orders',
    bootstrap_servers='localhost:9092',
    auto_offset_reset='latest',  
    group_id='group1',          
    value_deserializer=custom_deserializer
)

for msg in consumer:
        print(msg.value)