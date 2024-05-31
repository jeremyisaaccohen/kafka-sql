import json
import time
from time import sleep

from kafka import KafkaProducer, KafkaConsumer, KafkaAdminClient
from kafka.admin import NewTopic

import pandas as pd

from constants import TOPIC_NAME, NUM_PARTITIONS, REPLICATION_FACTOR, BOOTSTRAP_SERVERS, FILE_PATH


my_topic = NewTopic(TOPIC_NAME, num_partitions=NUM_PARTITIONS, replication_factor=REPLICATION_FACTOR)
admin_client = KafkaAdminClient(bootstrap_servers=[BOOTSTRAP_SERVERS], client_id='test')

producer = KafkaProducer(bootstrap_servers=[BOOTSTRAP_SERVERS])
for i ,chunk in pd.read_csv(FILE_PATH).iterrows():
    print(i,"chunk: ",chunk)
    chunk_dict = chunk.to_dict()
    data = json.dumps(chunk_dict).encode('utf-8')
    producer.send(my_topic.name, value=data)
    print("sent ", data)

print(producer.metrics())

producer.flush()
producer.close()





"""

from kafka.admin import KafkaAdminClient, NewTopic
from kafka import KafkaProducer, KafkaConsumer
import time

# Step 1: Create Topic
def create_topic():
    admin_client = KafkaAdminClient(
        bootstrap_servers="localhost:9092", 
        client_id='test'
    )
    
    topic = NewTopic(name="jeremy_topic", num_partitions=1, replication_factor=1)
    
    try:
        admin_client.create_topics(new_topics=[topic], validate_only=False)
        print("Topic 'jeremy_topic' created successfully")
    except Exception as e:
        print(f"Topic creation failed: {e}")

# Step 2: Produce Messages
def produce_messages():
    producer = KafkaProducer(bootstrap_servers=["localhost:9092"])
    
    for i in range(40):
        producer.send("jeremy_topic", value="heres a test".encode('utf-8'))
        print("Sent", i)
    
    producer.flush()
    producer.close()

# Step 3: Consume Messages
def consume_messages():
    consumer = KafkaConsumer(
        "jeremy_topic",
        bootstrap_servers=['localhost:9092'],
        auto_offset_reset='earliest',  # Start from the beginning of the topic
        enable_auto_commit=True,
        group_id='my_group'  # Specify a group ID
    )
    
    print("Starting consumer...")

    for message in consumer:
        print("Received:", message.value.decode("utf-8"))
    
    print("Done ???")

if __name__ == "__main__":
    # Create the topic
    create_topic()
    
    # Give some time for the topic to be fully created and registered
    time.sleep(1)
    
    # Produce messages
    produce_messages()
    
    # Give some time for all messages to be produced
    time.sleep(1)
    
    # Consume messages
    consume_messages()


"""