package org.example.kafka.test;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Arrays;
import java.util.List;
import java.util.Properties;

public class KafkaEventProducer {

    public static void main(String[] args) {
        Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);

        String topic = "input-topic-test";

        List<String> testCases = Arrays.asList(
                // Valid Event
                "{"
                        + "\"event_id\": \"unique-event-id-001\","
                        + "\"timestamp\": \"2024-07-25T12:00:00Z\","
                        + "\"vault_version\": {"
                        + "    \"major\": 1,"
                        + "    \"minor\": 0,"
                        + "    \"patch\": 0,"
                        + "    \"label\": \"v1.0.0\""
                        + "},"
                        + "\"change_id\": \"change-id-001\","
                        + "\"account_created\": {"
                        + "    \"account\": {"
                        + "        \"id\": \"acc-001\","
                        + "        \"name\": \"Account Name\","
                        + "        \"product_id\": \"prod-001\","
                        + "        \"product_version_id\": \"v1\","
                        + "        \"permitted_denominations\": [\"GBP\", \"USD\"],"
                        + "        \"status\": \"active\","
                        + "        \"opening_timestamp\": \"2024-07-25T12:00:00Z\","
                        + "        \"closing_timestamp\": null,"
                        + "        \"stakeholder_ids\": [\"stakeholder-001\"],"
                        + "        \"instance_param_vals\": {"
                        + "            \"account_indicators\": \"indicator-001\","
                        + "            \"account_legal_entity\": \"entity-001\","
                        + "            \"account_max_balance\": \"10000\","
                        + "            \"account_min_balance\": \"1000\","
                        + "            \"account_rate\": \"5%\","
                        + "            \"account_term\": \"12 months\","
                        + "            \"early_closure\": \"no\","
                        + "            \"manual_remit\": \"no\","
                        + "            \"manual_statement\": \"no\","
                        + "            \"maturity_date\": \"2025-07-25\","
                        + "            \"remittance_account\": \"remit-001\""
                        + "        },"
                        + "        \"derived_instance_param_vals\": {},"
                        + "        \"details\": {"
                        + "            \"account_type\": \"savings\","
                        + "            \"brand\": \"Brand Name\","
                        + "            \"default_currency\": \"GBP\""
                        + "        },"
                        + "        \"accounting\": {"
                        + "            \"tside\": \"credit\""
                        + "        }"
                        + "    }"
                        + "}"
                        + "}",

                // Another Valid Event
                "{"
                        + "\"event_id\": \"unique-event-id-002\","
                        + "\"timestamp\": \"2024-07-26T14:30:00Z\","
                        + "\"vault_version\": {"
                        + "    \"major\": 1,"
                        + "    \"minor\": 1,"
                        + "    \"patch\": 0,"
                        + "    \"label\": \"v1.1.0\""
                        + "},"
                        + "\"change_id\": \"change-id-002\","
                        + "\"account_created\": {"
                        + "    \"account\": {"
                        + "        \"id\": \"acc-002\","
                        + "        \"name\": \"Account Name 2\","
                        + "        \"product_id\": \"prod-002\","
                        + "        \"product_version_id\": \"v2\","
                        + "        \"permitted_denominations\": [\"USD\", \"EUR\"],"
                        + "        \"status\": \"active\","
                        + "        \"opening_timestamp\": \"2024-07-26T14:30:00Z\","
                        + "        \"closing_timestamp\": \"2024-07-26T14:30:00Z\","
                        + "        \"stakeholder_ids\": [\"stakeholder-002\"],"
                        + "        \"instance_param_vals\": {"
                        + "            \"account_indicators\": \"indicator-002\","
                        + "            \"account_legal_entity\": \"entity-002\","
                        + "            \"account_max_balance\": \"20000\","
                        + "            \"account_min_balance\": \"2000\","
                        + "            \"account_rate\": \"6%\","
                        + "            \"account_term\": \"24 months\","
                        + "            \"early_closure\": \"no\","
                        + "            \"manual_remit\": \"no\","
                        + "            \"manual_statement\": \"no\","
                        + "            \"maturity_date\": \"2026-07-26\","
                        + "            \"remittance_account\": \"remit-002\""
                        + "        },"
                        + "        \"derived_instance_param_vals\": {},"
                        + "        \"details\": {"
                        + "            \"account_type\": \"current\","
                        + "            \"brand\": \"Brand Name 2\","
                        + "            \"default_currency\": \"USD\""
                        + "        },"
                        + "        \"accounting\": {"
                        + "            \"tside\": \"debit\""
                        + "        }"
                        + "    }"
                        + "}"
                        + "}",

                // Invalid Event - Missing Required Fields
                "{"
                        // Removed "event_id": "unique-event-id-001",
                        // Removed "timestamp": "2024-07-25T12:00:00Z",
                        // Removed "vault_version": { "major": 1, "minor": 0, "patch": 0, "label": "v1.0.0" },
                        + "\"change_id\": \"change-id-001\","
                        + "\"account_created\": {"
                        + "    \"account\": {"
                        + "        \"id\": \"acc-001\","
                        + "        \"name\": \"Account Name\","
                        + "        \"product_id\": \"prod-001\","
                        + "        \"product_version_id\": \"v1\","
                        + "        \"permitted_denominations\": [\"GBP\", \"USD\"],"
                        + "        \"status\": \"active\","
                        + "        \"opening_timestamp\": \"2024-07-25T12:00:00Z\","
                        + "        \"closing_timestamp\": null,"
                        + "        \"stakeholder_ids\": [\"stakeholder-001\"],"
                        + "        \"instance_param_vals\": {"
                        + "            \"account_indicators\": \"indicator-001\","
                        + "            \"account_legal_entity\": \"entity-001\","
                        + "            \"account_max_balance\": \"10000\","
                        + "            \"account_min_balance\": \"1000\","
                        + "            \"account_rate\": \"5%\","
                        + "            \"account_term\": \"12 months\","
                        + "            \"early_closure\": \"no\","
                        + "            \"manual_remit\": \"no\","
                        + "            \"manual_statement\": \"no\","
                        + "            \"maturity_date\": \"2025-07-25\","
                        + "            \"remittance_account\": \"remit-001\""
                        + "        },"
                        + "        \"derived_instance_param_vals\": {},"
                        + "        \"details\": {"
                        + "            \"account_type\": \"savings\","
                        + "            \"brand\": \"Brand Name\","
                        + "            \"default_currency\": \"GBP\""
                        + "        },"
                        + "        \"accounting\": {"
                        + "            \"tside\": \"credit\""
                        + "        }"
                        + "    }"
                        + "}"
                        + "}",

                // Invalid Event - Invalid Enum Value
                "{"
                        + "\"event_id\": \"unique-event-id-003\","
                        + "\"timestamp\": \"2024-07-25T12:00:00Z\","
                        + "\"vault_version\": {"
                        + "    \"major\": 1,"
                        + "    \"minor\": 0,"
                        + "    \"patch\": 0,"
                        + "    \"label\": \"v1.0.0\""
                        + "},"
                        + "\"change_id\": \"change-id-002\","
                        + "\"account_created\": {"
                        + "    \"account\": {"
                        + "        \"id\": \"acc-002\","
                        + "        \"name\": \"Account Name\","
                        + "        \"product_id\": \"prod-002\","
                        + "        \"product_version_id\": \"v1\","
                        + "        \"permitted_denominations\": [\"GBP\", \"USD\"],"
                        + "        \"status\": \"active\","
                        + "        \"opening_timestamp\": \"2024-07-25T12:00:00Z\","
                        + "        \"closing_timestamp\": null,"
                        + "        \"stakeholder_ids\": [\"stakeholder-002\"],"
                        + "        \"instance_param_vals\": {"
                        + "            \"account_indicators\": \"indicator-002\","
                        + "            \"account_legal_entity\": \"entity-002\","
                        + "            \"account_max_balance\": \"20000\","
                        + "            \"account_min_balance\": \"2000\","
                        + "            \"account_rate\": \"3.5%\","
                        + "            \"account_term\": \"12 months\","
                        + "            \"early_closure\": \"no\","
                        + "            \"manual_remit\": \"no\","
                        + "            \"manual_statement\": \"no\","
                        + "            \"maturity_date\": \"2025-07-25\","
                        + "            \"remittance_account\": \"remit-002\""
                        + "        },"
                        + "        \"derived_instance_param_vals\": {},"
                        + "        \"details\": {"
                        + "            \"account_type\": \"savings\","
                        + "            \"brand\": \"Brand Name\","
                        + "            \"default_currency\": \"GBP\""
                        + "        },"
                        + "        \"accounting\": {"
                        + "            \"tside\": \"credit\""
                        + "        }"
                        + "    }"
                        + "}"
                        + "}"
        );

        testCases.forEach(testCase -> {
            ProducerRecord<String, String> record = new ProducerRecord<>(topic, testCase);
            producer.send(record, (metadata, exception) -> {
                if (exception == null) {
                    System.out.printf("Sent message: key = %s, value = %s, to partition = %d%n", record.key(), record.value(), metadata.partition());
                } else {
                    System.err.printf("Error sending message: %s%n", exception.getMessage());
                }
            });
        });

        producer.close();
    }
}
