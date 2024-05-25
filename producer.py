import time
from time import sleep

from kafka import KafkaProducer, KafkaConsumer, KafkaAdminClient
from kafka.admin import NewTopic

import pandas as pd



my_topic = NewTopic("jeremy_topic", num_partitions=1, replication_factor=1)
admin_client = KafkaAdminClient(bootstrap_servers="localhost:9092", client_id='test')
# admin_client.create_topics(new_topics=[my_topic])

producer = KafkaProducer(bootstrap_servers=["localhost:9092"])
for i in range(40):
    producer.send("jeremy_topic", value="heres a test".encode('utf-8'))
    print("sent ", i)

producer.flush()
producer.close()

consumer = KafkaConsumer("jeremy_topic", bootstrap_servers=['localhost:9092'], auto_offset_reset='earliest')
for message in consumer:
    print("hmmmm")
    print(message)
    print(message.value.decode("utf-8"))

print("done ???")





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