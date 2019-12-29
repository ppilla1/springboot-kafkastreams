#1# Start Zookeeper
zookeeper-server-start.bat config\zookeeper.properties

#2# Start Kafka Broker server
### Set property delete.topic.enable=true in server.properties , to enable topic deletion
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

#5# Publish messages to topics with key 
kafka-console-producer.bat --broker-list localhost:9092 --property parse.key=true --property key.separator="|" --topic <streams-file-input>

>1|kafka streams udemy
>2|kafka data processing
>3|kafka streams course

#6# Display messages from topic
kafka-console-consumer.bat --bootstrap-server localhost:9092 --from-beginning --property print.key=true --property key.separator="|" --topic <streams-file-input> 

#7# Run example Kafka Streams app
kafka-run-class.bat org.apache.kafka.streams.examples.wordcount.WordCountDemo

#8# Start Kafka consumer console for reading Kafka stream sink topic
kafka-console-consumer.bat --bootstrap-server localhost:9092 --topic <streams-wordcount-output> --from-beginning --formatter kafka.tools.DefaultMessageFormatter --property print.key=true --property print.value=true --property key.deserializer=org.apache.kafka.common.serialization.StringDeserializer --property value.deserializer=org.apache.kafka.common.serialization.LongDeserializer

#9# View Kafka Streams app result along with listing of temporary topics
kafka-topics.bat --list --zookeeper localhost:2181

#10# Delete kafka topics
kafka-topics.bat --zookeeper localhost:2181 --delete --topic <topic-name>

### Sample Test <key>|<value> pair for json for Kafka Movie payload  ###
The Matrix|{"title":"The Matrix","year":1999,"cast":["Keanu Reeves","Laurence Fishburne","Carrie-Anne Moss","Hugo Weaving","Joe Pantoliano"],"genres":["Science Fiction"]}
Die Hard|{"title":"Die Hard","year":1988,"cast":["Bruce Willis","Alan Rickman","Bonnie Bedelia","William Atherton","Paul Gleason","Reginald VelJohnson","Alexander Godunov"],"genres":["Action"]}
Toy Story|{"title":"Toy Story","year":1995,"cast":["Tim Allen","Tom Hanks","(voices)"],"genres":["Animated"]}
Jurassic Park|{"title":"Jurassic Park","year":1993,"cast":["Sam Neill","Laura Dern","Jeff Goldblum","Richard Attenborough"],"genres":["Adventure"]}
The Lord of the Rings: The Fellowship of the Ring|{"title":"The Lord of the Rings: The Fellowship of the Ring","year":2001,"cast":["Elijah Wood","Ian McKellen","Liv Tyler","Sean Astin","Viggo Mortensen","Orlando Bloom","Sean Bean","Hugo Weaving","Ian Holm"],"genres":["Fantasy"]}
Inception|{"title":"Inception","year":2010,"cast":["Leonardo DiCaprio","Ken Watanabe","Joseph Gordon-Levitt", "Marion Cotillard", "Ellen Page", "Tom Hardy", "Cillian Murphy", "Tom Berenger", "Michael Caine"],"genres":["Science Fiction"]}
