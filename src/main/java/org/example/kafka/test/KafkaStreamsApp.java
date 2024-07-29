package org.example.kafka.test;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Named;
import org.apache.kafka.streams.state.QueryableStoreTypes;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;
import org.apache.kafka.streams.state.Stores;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RestController;

import java.util.Properties;

@SpringBootApplication
public class KafkaStreamsApp implements CommandLineRunner {

    private static final Logger logger = LoggerFactory.getLogger(KafkaStreamsApp.class);

    @Autowired
    private KafkaStreams kafkaStreams;

    public static void main(String[] args) {
        SpringApplication.run(KafkaStreamsApp.class, args);
    }

    @Override
    public void run(String... args) throws Exception {
        logger.info("********** Application Started **********");
    }

    @Bean
    public KafkaStreams kafkaStreams() {
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "state-store-app");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");

        StreamsBuilder builder = new StreamsBuilder();

        KStream<String, String> stream = builder.stream("input-topic");
        stream.groupByKey()
                .count((Named) Stores.persistentKeyValueStore("state-store"));

        KafkaStreams streams = new KafkaStreams(builder.build(), props);
        streams.start();

        // Add shutdown hook to respond to SIGTERM and gracefully close Kafka Streams
        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));

        return streams;
    }

    @RestController
    public class StateStoreController {

        @GetMapping("/state-store/print/{storeName}")
        public void printStateStore(@PathVariable String storeName) {
            printStateStoreContents(storeName);
        }

        public void printStateStoreContents(String storeName) {
            logger.info("********** START - Printing State Store Contents **********");
            ReadOnlyKeyValueStore<String, Long> keyValueStore =
                    kafkaStreams.store(storeName, QueryableStoreTypes.keyValueStore());

            keyValueStore.all().forEachRemaining(entry ->
                    logger.info("Key: {}, Value: {}", entry.key, entry.value));

            logger.info("********** END - State Store Contents Printed **********");
        }
    }
}
