from time import sleep
from json import dumps
from kafka import KafkaProducer

#Kakfa producer class where we are producing the 0 to 100 on DemoTopic 

producer = KafkaProducer(bootstrap_servers=['localhost:9092'],
                         value_serializer=lambda x: 
                         dumps(x).encode('utf-8'))

for e in range(100):
    data = e
    producer.send('DemoTopic', value=data)
    sleep(5)

