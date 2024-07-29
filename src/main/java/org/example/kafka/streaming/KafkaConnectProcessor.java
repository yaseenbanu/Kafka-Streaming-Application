package org.example.kafka.streaming;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Produced;

import java.util.Properties;

public class KafkaConnectProcessor {
    public static void main(String[] args) {
        // Define Kafka Streams configuration
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "simpleStreamsApp");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.ByteArray().getClass().getName());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.ByteArray().getClass().getName());
        props.put(StreamsConfig.APPLICATION_SERVER_CONFIG, "localhost:8080");
        props.put(StreamsConfig.STATE_DIR_CONFIG, ".");

        // Build the streaming topology
        StreamsBuilder builder = new StreamsBuilder();

        // Read from the input topic
        KStream<byte[], byte[]> inputStream = builder.stream("input-topic-test");

        // Write raw data to the raw output topic
        inputStream.to("raw-output-topic-test", Produced.with(Serdes.ByteArray(), Serdes.ByteArray()));

        // Create and start the Kafka Streams instance
        KafkaStreams streams = new KafkaStreams(builder.build(), props);
        streams.start();

        // Add a shutdown hook to close the streams application gracefully
        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
    }
}
