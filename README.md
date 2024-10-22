# Bao Buffet Stream

Bao Buffet Stream is a pure Java, Kafka-like event system that simulates buffet restaurant operations.

## What this project includes

- Topic + partition model
- Producer with key-based partition routing
- Consumer groups with independent committed offsets
- Protocol flow: request header parsing, API-key dispatch, and binary response serialization
- Buffet event flow: entry, plate pickup, restock, payment

## Tech

- Java 17
- Maven

## Run

```bash
mvn clean package
java -jar target/bao-buffet-stream-1.0.0.jar
```

## Example output

You will see producer logs per partition/offset and consumer logs from different consumer groups (kitchen and billing) processing the same topic independently.
