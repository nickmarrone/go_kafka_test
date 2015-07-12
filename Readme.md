# Go Kafka Test

## 1. Set up Apache Kafka

Go to the [http://kafka.apache.org/documentation.html](Apache Kafka homepage) and follow the quick start guide to create a topic. I created a topic called 'multiply'

## 2. Start any number of consumers

./consumer --brokers=localhost:9092 --topic=multiply --op=*

./consumer --brokers=localhost:9092 --topic=multiply --op=-

./consumer --brokers=localhost:9092 --topic=multiply --op=/

./consumer --brokers=localhost:9092 --topic=multiply --op=+

## 3. Start a producer

./producer --brokers=localhost:9092 --topic=multiply

## 4. Start sending messages in the producer

x y: 3 4

=> 3 * 4 = 12