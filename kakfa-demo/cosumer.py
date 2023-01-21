from kafka import KafkaConsumer
from json import loads

#Kafka consumer class where we consumer DempTopic 

consumer = KafkaConsumer(
    'DemoTopic',
     bootstrap_servers=['localhost:9092'],
     auto_offset_reset='earliest',
     enable_auto_commit=True,
     group_id='my-group',
     value_deserializer=lambda x: loads(x.decode('utf-8')))

print("Kafka Consumer is started")

for message in consumer:
    message = message.value
    print("Kafka message is ", message)