from kafka import KafkaConsumer
import json

consumer = KafkaConsumer(
    'test',
    bootstrap_servers=['kafka:9092'],
    group_id='group6',
    auto_offset_reset='latest',
    value_deserializer=lambda m: json.loads(m.decode('utf-8')) 
)

for message in consumer:
    print(f"Received message: {message.value}")
