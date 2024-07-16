`cd Downloads/kafka/kafka_2.13-3.7.0/`

Start the ZooKeeper service 

`$ bin/zookeeper-server-start.sh config/zookeeper.properties`

And in another terminal, start the Kafka broker service:

`$ bin/kafka-server-start.sh config/server.properties`

`fastapi run living_producer.py --reload`

reload flag useful for live reloads of changes in src

## Todo:

Load test MySql and postgres
 - could read from kafka and write to sql
 - figure out some random little fun data model schema
 - script that pushes to kafka as producer
	 - write json to kafka
	 - can play w diff formats after (protobuf)
	 - log metrics or something
- read from kafka and write to db

2 scripts when containers spin up that run automatically
	 1 . just start reading from configurable source of data, writing to kafka
	 2. read from kafka and write to sql


After: Take producer and make an api so you can make a post request that publishes to kafka

use promoetheus for stats


## TODO:

Working capital ratio