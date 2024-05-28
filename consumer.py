from kafka import KafkaConsumer
from constants import TOPIC_NAME, BOOTSTRAP_SERVERS

consumer = KafkaConsumer(TOPIC_NAME, bootstrap_servers=[BOOTSTRAP_SERVERS], auto_offset_reset='earliest')
for message in consumer:
    print(message.value.decode())



### Write to SQL?
