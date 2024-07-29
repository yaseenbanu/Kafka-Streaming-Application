package org.example.kafka.streaming;

import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.example.kafka.config.KafkaConfig;
import org.example.kafka.topology.StreamTopology;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class KafkaStreamProcessor {
    private static final Logger logger = LoggerFactory.getLogger(KafkaStreamProcessor.class);

    public static void main(String[] args) {
        // Load Kafka Streams configuration from KafkaConfig
        Properties props = KafkaConfig.getStreamsConfig();

        // Create StreamsBuilder and build the topology
        StreamsBuilder builder = new StreamsBuilder();
        StreamTopology.buildTopology(builder);

        // Create and start Kafka Streams application
        KafkaStreams streams = new KafkaStreams(builder.build(), props);
        streams.start();

        // Add shutdown hook to close Kafka Streams on application exit
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            logger.info("Shutting down Kafka Streams application.");
            streams.close();
        }));
    }
}
