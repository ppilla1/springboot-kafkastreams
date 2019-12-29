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
import org.ppillai.kafkastreams.model.Movie;
import org.ppillai.kafkastreams.model.MovieGenere;
import org.ppillai.kafkastreams.serde.JsonDeserializer;
import org.ppillai.kafkastreams.serde.JsonSerializer;

import java.util.Properties;

@Slf4j
public class MovieGenreKStreamAppTest {

    public static final String SOURCE_TOPIC = "src-topic";
    public static final String SINK_TOPIC = "out-topic";

    @Test
    public void test_yelling_app_KStream() throws InterruptedException {

        /* Initialize Kafka Stream configuration */
        Properties props = new Properties();

        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "moviegenre_app_id");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");

        /* Create instance for Serializer/De-serializer for keys in Kafka topic */
        Serde<String> stringSerde = Serdes.String();

        /* Create instance for Serializer/De-serializer for reading source
        Kafka topics
        */
        JsonSerializer<Movie> movieJsonSerializer = new JsonSerializer<>();
        JsonDeserializer<Movie> movieJsonDeserializer = new JsonDeserializer<>(Movie.class);
        Serde<Movie> movieSerde = Serdes.serdeFrom(movieJsonSerializer, movieJsonDeserializer);

        /* Construct Kafka Stream processing topology*/
        StreamsBuilder streamsBuilder = new StreamsBuilder();

        /** 1. Stream processing node for reading from Kafka topic **/
        KStream<String, Movie> sourceNode = streamsBuilder.stream(SOURCE_TOPIC, Consumed.with(stringSerde, movieSerde));

        /** 2. Stream processing node for transforming source stream to uppercase **/
        KStream<String, MovieGenere> movieGenereNode = sourceNode.mapValues(this::transformToMovieGenre);

        /* Create instance for Serializer/De-serializer for reading source
        Kafka topics
        */
        JsonSerializer<MovieGenere> movieGenreJsonSerializer = new JsonSerializer<>();
        JsonDeserializer<MovieGenere> movieGenreJsonDeserializer = new JsonDeserializer<>(MovieGenere.class);
        Serde<MovieGenere> movieGenreSerde = Serdes.serdeFrom(movieGenreJsonSerializer, movieGenreJsonDeserializer);

        /** 3. Stream processing node for writing uppercase transformed data to target Kafka topic **/
        movieGenereNode.to(SINK_TOPIC, Produced.with(stringSerde, movieGenreSerde));

        /* Start Kafka stream */
        KafkaStreams kafkaStreams = new KafkaStreams(streamsBuilder.build(), props);

        kafkaStreams.start();
        Thread.sleep(35000);
        log.info("Starting MovieGenre streaming app");
        kafkaStreams.close();
    }

    public MovieGenere transformToMovieGenre(Movie movie){
        MovieGenere movieGenere = new MovieGenere();
        movieGenere.setTitle(movie.getTitle());
        movieGenere.setGenres(movie.getGenres());
        return  movieGenere;
    }
}
