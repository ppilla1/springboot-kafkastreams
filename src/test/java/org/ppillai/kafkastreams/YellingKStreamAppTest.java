package org.ppillai.kafkastreams;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Produced;
import org.junit.jupiter.api.Test;

import java.util.Properties;

@Slf4j
public class YellingKStreamAppTest {

    public static final String SOURCE_TOPIC = "src-topic";
    public static final String SINK_TOPIC = "out-topic";

    @Test
    public void test_yelling_app_KStream() throws InterruptedException {

        /* Initialize Kafka Stream configuration */
        Properties props = new Properties();

        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "yelling_app_id");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");

        StreamsConfig streamsConfig = new StreamsConfig(props);

        /* Create instance for Serializer/De-serializer for read/write
        to Kafka topics
        */
        Serde<String> stringSerde = Serdes.String();

        /* Construct Kafka Stream processing topology*/
        StreamsBuilder streamsBuilder = new StreamsBuilder();

        /** 1. Stream processing node for reading from Kafka topic **/
        KStream<String, String> sourceNode = streamsBuilder.stream(SOURCE_TOPIC, Consumed.with(stringSerde, stringSerde));

        /** 2. Stream processing node for transforming source stream to uppercase **/
        KStream<String, String> upperCaseProcessingNode = sourceNode.mapValues((String value) ->  value.toUpperCase());

        /** 3. Stream processing node for writing uppercase transformed data to target Kafka topic **/
        upperCaseProcessingNode.to(SINK_TOPIC, Produced.with(stringSerde, stringSerde));

        /* Start Kafka stream */
        KafkaStreams kafkaStreams = new KafkaStreams(streamsBuilder.build(), streamsConfig);

        kafkaStreams.start();
        Thread.sleep(35000);
        log.info("Starting Yelling streaming app");
        kafkaStreams.close();
    }
}
