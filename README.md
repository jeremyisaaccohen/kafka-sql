`cd Downloads/kafka/kafka_2.13-3.7.0/`

Start the ZooKeeper service 

`$ bin/zookeeper-server-start.sh config/zookeeper.properties`

And in another terminal, start the Kafka broker service:

`$ bin/kafka-server-start.sh config/server.properties`