# springboot-kafkastreams
SpringBoot with Kafka Streams

#1# Start Zookeeper
zookeeper-server-start.bat config\zookeeper.properties

#2# Start Kafka Broker server
kafka-server-start.bat config\server.properties

#3# Create topics 
kafka-topics.bat --create --zookeeper localhost:2181 --replication-factor 1 --partitions 3 --topic <streams-file-input>
kafka-topics.bat --create --zookeeper localhost:2181 --replication-factor 1 --partitions 3 --topic <streams-wordcount-output>

#4# List topics
kafka-topics.bat --list --zookeeper localhost:2181

#5# Publish messages to topics
kafka-console-producer.bat --broker-list localhost:9092 --topic <streams-file-input>

>kafka streams udemy
>kafka data processing
>kafka streams course

#6# Display messages from topic
kafka-console-consumer.bat --bootstrap-server localhost:9092 --from-beginning --topic <streams-file-input> 

#7# Run example Kafka Streams app
kafka-run-class.bat org.apache.kafka.streams.examples.wordcount.WordCountDemo

#8# Start Kafka consumer console for reading Kafka stream sink topic
kafka-console-consumer.bat --bootstrap-server localhost:9092 --topic <streams-wordcount-output> --from-beginning --formatter kafka.tools.DefaultMessageFormatter --property print.key=true --property print.value=true --property key.deserializer=org.apache.kafka.common.serialization.StringDeserializer --property value.deserializer=org.apache.kafka.common.serialization.LongDeserializer

#9# View Kafka Streams app result along with listing of temporary topics
kafka-topics.bat --list --zookeeper localhost:2181