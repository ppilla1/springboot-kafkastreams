package org.ppillai.kafkastreams;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.*;
import org.junit.jupiter.api.Test;
import org.ppillai.kafkastreams.model.Movie;
import org.ppillai.kafkastreams.model.MovieGenere;
import org.ppillai.kafkastreams.serde.JsonDeserializer;
import org.ppillai.kafkastreams.serde.JsonSerializer;

import java.util.Properties;

@Slf4j
public class MovieStatelessKStreamAppTest {

    public static final String SOURCE_TOPIC = "src-topic";
    public static final String SINK_TOPIC = "out-topic";
    public static final String LEONARDO_MOVIES_TOPIC = "leonardo-movies-topic";
    public static final String BEFORE_1995_MOVIES_TOPIC = "movies-before1995-topic";
    public static final String AFTER_1995_MOVIES_TOPIC = "movies-after1995-topic";

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

        /** 1. Stream processing node for reading Movie json payload from Kafka topic **/
        KStream<String, Movie> sourceNode = streamsBuilder.stream(SOURCE_TOPIC, Consumed.with(stringSerde, movieSerde));
        sourceNode.peek((key, movie) -> log.info("[Movie] key={}, value={}", key, movie));

        /** 2.1 Stream processing node for transforming source stream to MovieGenre payload **/
        KStream<String, MovieGenere> movieGenereNode = sourceNode.mapValues(this::transformToMovieGenre);

        sourceNode.filter((key, movie) -> movie.getYear() < 2000).selectKey((key, movie) -> movie.getYear());
        movieGenereNode.peek((key, moviegenere) -> log.info("[MovieGenere] key={}, value={}", key, moviegenere));

        /** 2.2 Processing node for filtering Leonardo movies with setting key as name of primary actor **/
        KStream<String, Movie> leonardoMoviesNode = sourceNode
                                                        .filter(this::filterLeonardoMovies)
                                                        .selectKey(this::setKeyAsPrimaryCastMemberName);

        /** 2.3 Processing node for spliting movies stream into before/after 1995 release years **/
        Predicate<String, Movie> before1995Movies = (key, movie) -> movie.getYear() < 1995;
        Predicate<String, Movie> after1995Movies = (key, movie) -> movie.getYear() >= 1995;

        KStream<String, Movie>[] moviesSplitByReleaseYear = sourceNode.branch(before1995Movies, after1995Movies);

        int BEFORE1995MOVIES = 0;
        int AFTER1995MOVIES = 1;

        /* Create instance for Serializer/De-serializer for writing to sink
        Kafka topics
        */
        JsonSerializer<MovieGenere> movieGenreJsonSerializer = new JsonSerializer<>();
        JsonDeserializer<MovieGenere> movieGenreJsonDeserializer = new JsonDeserializer<>(MovieGenere.class);
        Serde<MovieGenere> movieGenreSerde = Serdes.serdeFrom(movieGenreJsonSerializer, movieGenreJsonDeserializer);

        /** 2.1.1. Stream processing node for writing MovieGenre data to target Kafka topic **/

        movieGenereNode.to(SINK_TOPIC, Produced.with(stringSerde, movieGenreSerde));

        /** 2.2.1 Processing node for writing movies with Leonardo as primary cast **/
        leonardoMoviesNode.to(LEONARDO_MOVIES_TOPIC, Produced.with(stringSerde, movieSerde));

        /** 2.3.1 Processing node for writing before 1995 movies with keys as year of release **/
        KStream<Integer, Movie> moviesBefore1995Node = moviesSplitByReleaseYear[BEFORE1995MOVIES]
                                                        .selectKey(this::setReleaseYearAsKey);

        moviesBefore1995Node.peek((key, movie) -> log.info("[MoviesBefore1995] key={}, value={}", key, movie));

        moviesBefore1995Node.to(BEFORE_1995_MOVIES_TOPIC, Produced.with(Serdes.Integer(), movieSerde));

        /** 2.3.2 Processing node for writing after 1995 movies with keys as year of release **/
        KStream<Integer, Movie> moviesAfter1995Node = moviesSplitByReleaseYear[AFTER1995MOVIES]
                                                        .selectKey(this::setReleaseYearAsKey);

        moviesAfter1995Node.peek((key, movie) -> log.info("[MoviesAfter1995] key={}, value={}", key, movie));

        moviesAfter1995Node.to(AFTER_1995_MOVIES_TOPIC, Produced.with(Serdes.Integer(), movieSerde));

        /* Start Kafka stream */
        KafkaStreams kafkaStreams = new KafkaStreams(streamsBuilder.build(), props);

        kafkaStreams.start();
        Thread.sleep(35000);
        log.info("Starting MovieGenre streaming app");
        kafkaStreams.close();
    }

    private Integer setReleaseYearAsKey(String s, Movie movie) {
        return movie.getYear();
    }

    public MovieGenere transformToMovieGenre(Movie movie){
        MovieGenere movieGenere = new MovieGenere();
        movieGenere.setTitle(movie.getTitle());
        movieGenere.setGenres(movie.getGenres());
        return  movieGenere;
    }

    public boolean filterLeonardoMovies(String key, Movie movie){
        boolean status = false;

        if (null != movie.getCast() && !movie.getCast().isEmpty()) {
            status = movie.getCast().get(0).equalsIgnoreCase("Leonardo DiCaprio");
        }

        return status;
    }

    public String setKeyAsPrimaryCastMemberName(String key, Movie movie){
        String newKey = null;

        if (null != movie.getCast() && !movie.getCast().isEmpty()){
            newKey = movie.getCast().get(0);
        }

        return newKey;
    }
}
