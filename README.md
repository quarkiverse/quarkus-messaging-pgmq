# Quarkus Messaging PGMQ

[![Version](https://img.shields.io/maven-central/v/io.quarkiverse.messaging.pgmq/quarkus-messaging-pgmq?logo=apache-maven&style=flat-square)](https://central.sonatype.com/artifact/io.quarkiverse.messaging.pgmq/quarkus-messaging-pgmq-parent)

A [Quarkus](https://quarkus.io) extension that provides a [SmallRye Reactive Messaging](https://smallrye.io/smallrye-reactive-messaging) connector for [PGMQ](https://github.com/tembo-io/pgmq) (PostgreSQL Message Queue). 
Use PostgreSQL as a durable, transactional message broker -- no external infrastructure required.

## Features

- **Reactive Messaging connector** -- use `@Incoming` / `@Outgoing` / `@Channel` with PGMQ queues
- **POJO serialization** -- send and receive Java objects with automatic Jackson serialization
- **Topic-based routing** -- pub/sub patterns with wildcard topic matching (`orders.*`, `events.#`)
- **FIFO ordering** -- ordered message consumption with throughput and round-robin modes
- **Batch operations** -- batch send and batch acknowledgment for high-throughput workloads
- **Message headers** -- attach and read custom JSON headers on messages
- **Delayed / scheduled messages** -- send messages with a visibility delay
- **Flexible ack/nack strategies** -- delete, archive, reset visibility, or ignore on ack/failure
- **Queue management** -- auto-create standard, unlogged, or partitioned queues on startup
- **OpenTelemetry tracing** -- automatic trace context propagation (enabled by default)
- **Health checks** -- readiness, liveness, and startup checks via SmallRye Health
- **Dev Services** -- zero-config development with an auto-provisioned PostgreSQL + PGMQ container
- **Native image support** -- automatic reflection registration for payload types

## Getting Started

### Installation

Add the extension to your `pom.xml`:

```xml
<dependency>
    <groupId>io.quarkiverse.messaging.pgmq</groupId>
    <artifactId>quarkus-messaging-pgmq</artifactId>
    <version>${quarkus-messaging-pgmq.version}</version>
</dependency>
```

### Configuration

Configure channels in `application.properties`:

```properties
# Outgoing channel
mp.messaging.outgoing.orders-out.connector=quarkus-pgmq
mp.messaging.outgoing.orders-out.queue=orders

# Incoming channel
mp.messaging.incoming.orders-in.connector=quarkus-pgmq
mp.messaging.incoming.orders-in.queue=orders
```

### Usage

Send and receive messages using standard Reactive Messaging annotations:

```java
@ApplicationScoped
public class OrderService {

    @Channel("orders-out")
    Emitter<Order> emitter;

    public void placeOrder(Order order) {
        emitter.send(order);
    }

    @Incoming("orders-in")
    public void process(Order order) {
        // process the order
    }
}
```

### Metadata and Headers

Attach metadata to outgoing messages and read it from incoming messages:

```java
// Sending with headers and topic routing
PGMQOutgoingMetadata metadata = PGMQOutgoingMetadata.builder()
    .withHeaders(JsonObject.of("correlation-id", "abc-123"))
    .withTopic("orders.created")
    .build();
emitter.send(Message.of(payload, Metadata.of(metadata)));

// Reading metadata on receive
@Incoming("orders-in")
public void process(JsonObject msg, PGMQIncomingMetadata metadata) {
    long msgId = metadata.getId();
    int readCount = metadata.getReadCount();
    JsonObject headers = metadata.getHeaders();
}
```

### Delayed Messages

```java
SendMessage<JsonObject> msg = new SendMessage<>();
msg.setPayload(JsonObject.of("status", "pending"));
msg.setDelay(60); // visible after 60 seconds
emitter.send(msg);
```

## Configuration Reference

### Common Properties

| Property | Type | Default | Description |
|----------|------|---------|-------------|
| `queue` | string | channel name | Target queue name |
| `client-id` | string | | PostgreSQL client pool identifier |
| `tracing-enabled` | boolean | `true` | Enable OpenTelemetry tracing |
| `queue.declare` | boolean | `false` | Auto-create queue on startup |
| `queue.declare.type` | string | `non_partitioned` | Queue type: `non_partitioned`, `partitioned`, `unlogged` |

### Incoming Properties

| Property | Type | Default | Description |
|----------|------|---------|-------------|
| `visibility-timeout` | int | `10` | Visibility timeout in seconds |
| `max-poll-messages` | int | `10` | Max messages per poll |
| `max-poll-seconds` | int | `10` | Max wait time for poll (seconds) |
| `poll-interval` | int | `100` | Poll interval (milliseconds) |
| `ack-type` | string | `delete` | On ack: `delete` or `archive` |
| `failure-type` | string | `visibility` | On nack: `visibility`, `delete`, `archive`, `ignore` |
| `fifo` | boolean | `false` | Enable FIFO consumption |
| `fifo-mode` | string | `throughput` | FIFO mode: `throughput` or `round-robin` |
| `topic-patterns` | string | | Comma-separated topic patterns |
| `ack-batch` | boolean | `false` | Enable batch acknowledgment |
| `ack-batch-size` | int | `500` | Max buffered acks before flush |
| `ack-batch-delay` | long | `5000` | Max delay (ms) before flushing |

### Outgoing Properties

| Property | Type | Default | Description |
|----------|------|---------|-------------|
| `batch` | boolean | `false` | Enable batch send mode |
| `batch-size` | int | `1000` | Messages per batch |
| `batch-delay` | long | `1000` | Max delay (ms) before sending partial batch |
| `routing-key` | string | | Topic routing key for pub/sub |

## Module Structure

This is a multi-module Maven project with a standard Quarkus extension structure:

- `client/`: Low-level PGMQ client library providing direct database operations
- `connector/`: SmallRye Reactive Messaging connector implementation
- `runtime/`: Runtime implementation of the PGMQ connector
- `deployment/`: Build-time configuration and processing for Quarkus
- `integration-tests/`: End-to-end tests using the extension
- `docs/`: Documentation in AsciiDoc format

## Build Commands

### Full Build

```shell
./mvnw clean install
```

### Build Without Formatting

```shell
./mvnw clean install -Dno-format
```

### Native Build

```shell
./mvnw install -Dnative -Dquarkus.native.container-build -Dnative.surefire.skip
```

### Run Tests Only

```shell
./mvnw test
```

### Run Integration Tests

```shell
./mvnw verify -Pit
```

### Run Single Test

```shell
# For deployment module tests
./mvnw test -pl deployment -Dtest=ClassName

# For integration tests
./mvnw test -pl integration-tests -Dtest=ClassName
```

### Build Documentation

```shell
./mvnw install -Pdocs
```

## Documentation

Full documentation is available at <https://docs.quarkiverse.io/quarkus-messaging-pgmq>.

## Contributing

Feel free to contribute to this project by submitting issues or pull requests.

## License

This project is licensed under the [Apache License 2.0](LICENSE).
