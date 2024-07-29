package org.example.kafka.topology;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.Stores;
import org.example.kafka.processor.StreamProcessor;
import org.example.kafka.processor.UniqueRecordProcessor;
import org.example.kafka.validator.KafkaMessageValidator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class StreamTopology {
    private static final Logger logger = LoggerFactory.getLogger(StreamTopology.class);

    public static void buildTopology(StreamsBuilder builder) {
        StoreBuilder<KeyValueStore<String, String>> storeBuilder = Stores.keyValueStoreBuilder(
                Stores.persistentKeyValueStore("state-store"),
                Serdes.String(),
                Serdes.String()
        );

        builder.addStateStore(storeBuilder);

        KStream<byte[], byte[]> rawStream = builder.stream("input-topic-test", Consumed.with(Serdes.ByteArray(), Serdes.ByteArray()));

        KStream<Void, String> deserializedStream = StreamProcessor.deserialize(rawStream);

        KafkaMessageValidator validator;
        try {
            validator = new KafkaMessageValidator("localhost:9092", "your-group-id", "schema-path.yml", "validations-path.yml");
        } catch (Exception e) {
            logger.error("Failed to initialize KafkaMessageValidator", e);
            throw new RuntimeException(e);
        }

        KStream<Void, String>[] branches = deserializedStream.branch(
                (key, value) -> {
                    try {
                        logger.info("*******************************************************************************");
                        return validator.validateMessage(value, "input-topic-test");
                    } catch (Exception e) {
                        logger.error("Validation error for key: {}, value: {}", key, value, e);
                        return false;
                    }
                },
                (key, value) -> true
        );

        KStream<Void, String> validStream = branches[0];
        KStream<byte[], byte[]> invalidStream = StreamProcessor.processInvalidStream(branches[1]);
        invalidStream.to("dead-letter-topic-test", Produced.with(Serdes.ByteArray(), Serdes.ByteArray()));

        // Use UniqueRecordProcessor with transform method
        KStream<Void, String> uniqueStream = validStream
                .transform(() -> new UniqueRecordProcessor(), "state-store");

        KStream<byte[], byte[]> outputStream = StreamProcessor.processUniqueStream(uniqueStream);
        outputStream.to("output-topic-test", Produced.with(Serdes.ByteArray(), Serdes.ByteArray()));
    }
}