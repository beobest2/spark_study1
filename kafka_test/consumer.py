from kafka import KafkaConsumer
import json

consumer = KafkaConsumer(
    'test',
    bootstrap_servers=['localhost:9094'],
    group_id='group1',
    auto_offset_reset='earliest',
    value_deserializer=lambda m: json.loads(m.decode('utf-8')) 
)

for message in consumer:
    print(f"Received message: {message.value}")
