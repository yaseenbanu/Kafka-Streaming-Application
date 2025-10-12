# Kafka Streams Processor

![Apache Kafka](https://img.shields.io/badge/Apache%20Kafka-231F20?style=for-the-badge&logo=apache-kafka&logoColor=white)
![Java](https://img.shields.io/badge/Java-ED8B00?style=for-the-badge&logo=java&logoColor=white)
![Spring Boot](https://img.shields.io/badge/Spring%20Boot-6DB33F?style=for-the-badge&logo=spring-boot&logoColor=white)
![Docker](https://img.shields.io/badge/Docker-2496ED?style=for-the-badge&logo=docker&logoColor=white)

Java-based Kafka Streams application for real-time data processing with deduplication, validation, and state store management.

## 📁 Project Structure

```
Kafka-Streams-Processor/
├── 📂 src/main/java/
│   ├── 📂 consumers/                    # Kafka consumer implementations
│   └── 📂 org/example/kafka/
│       ├── 📂 config/                   # Kafka configuration
│       ├── 📂 processor/                # Stream processors
│       ├── 📂 streaming/                # Main streaming applications
│       ├── 📂 topology/                 # Stream topology definitions
│       └── 📂 validator/                # Message validation
├── 📂 src/main/resources/               # Configuration files
├── 📄 docker-compose.yml               # Kafka cluster setup
├── 📄 pom.xml                          # Maven dependencies
└── 📄 runbook.txt                      # Execution instructions
```

## 🚀 Quick Start

### 1. Start Kafka Cluster
```bash
docker-compose up -d
```

### 2. Build Application
```bash
mvn clean compile
```

### 3. Run Stream Processor
```bash
mvn exec:java -Dexec.mainClass="org.example.kafka.streaming.KafkaStreamProcessor"
```

### 4. Send Test Messages
```bash
# Connect to Kafka container
docker exec -it kafka-streams-processor-kafka-1 bash

# Start producer
kafka-console-producer --broker-list localhost:19092 --topic input-topic-account-create \
  --property "parse.key=true" --property "key.separator=:"
```

## 🔧 Components

### Stream Processors
| Class | Purpose |
|-------|---------|
| [`KafkaStreamProcessor`](./src/main/java/org/example/kafka/streaming/KafkaStreamProcessor.java) | Main streaming application entry point |
| [`KafkaJoinStreamingProcessor`](./src/main/java/org/example/kafka/streaming/KafkaJoinStreamingProcessor.java) | Stream joining operations |
| [`StreamProcessor`](./src/main/java/org/example/kafka/processor/StreamProcessor.java) | Core stream processing logic |
| [`UniqueRecordProcessor`](./src/main/java/org/example/kafka/processor/UniqueRecordProcessor.java) | Deduplication processing |

### Configuration
| File | Purpose |
|------|---------|
| [`KafkaConfig.java`](./src/main/java/org/example/kafka/config/KafkaConfig.java) | Kafka streams configuration |
| [`application.yaml`](./src/main/resources/application.yaml) | Application properties |
| [`config.yaml`](./src/main/resources/config.yaml) | Custom configuration |

### Topics
- `input-topic-account-create` - Account creation events
- `input-topic-account-update` - Account update events
- Output topics configured in topology

## 🛠️ Features

- **Deduplication**: Unique record processing with state stores
- **Validation**: Schema-based message validation
- **Join Operations**: Stream-to-stream and stream-to-table joins
- **State Management**: RocksDB-backed state stores
- **Error Handling**: Dead letter queue patterns
- **Monitoring**: Application metrics and logging

## 📋 Prerequisites

- Java 11+
- Maven 3.6+
- Docker and Docker Compose
- Apache Kafka 2.7+

## 🔧 Configuration

### Environment Variables
```properties
KAFKA_BOOTSTRAP_SERVERS=localhost:19092
KAFKA_APPLICATION_ID=KafkaStreamProcessor
```

### State Stores
- `state-store-account-create` - Account creation deduplication
- `state-store-account-update` - Account update deduplication

### Schema Files
- [`schema-path-account-create.yml`](./src/main/resources/schema-path-account-create.yml)
- [`schema-path-account-update.yml`](./src/main/resources/schema-path-account-update.yml)
- [`validations-path.yml`](./src/main/resources/validations-path.yml)

## 📊 Sample Data

Account creation event:
```json
{
  "event_id": "unique-event-id-001",
  "timestamp": "2024-07-25T12:00:00Z",
  "account_created": {
    "account": {
      "id": "acc-001",
      "name": "Account Name",
      "status": "active",
      "permitted_denominations": ["GBP", "USD"]
    }
  }
}
```

## 🔍 Monitoring

### Application Logs
```bash
tail -f logs/application.log
```

### Kafka Topics
```bash
# List topics
kafka-topics --bootstrap-server localhost:19092 --list

# Monitor consumer group
kafka-consumer-groups --bootstrap-server localhost:19092 --describe --group KafkaStreamProcessor
```

### State Store Inspection
```bash
# Check RocksDB state stores
ls -la KafkaStreamProcessor/
```

## 🏗️ Architecture

```
Input Topics → Stream Processor → Validation → Deduplication → Output Topics
                      ↓
                 State Stores (RocksDB)
```

## 🧪 Testing

### Manual Testing
See [`runbook.txt`](./runbook.txt) for detailed test scenarios and sample messages.

### Integration Tests
```bash
mvn test
```

## 🛑 Cleanup

```bash
# Stop application
Ctrl+C

# Stop Kafka cluster
docker-compose down

# Remove volumes
docker-compose down -v
```

---

*Real-time data processing with Apache Kafka Streams*