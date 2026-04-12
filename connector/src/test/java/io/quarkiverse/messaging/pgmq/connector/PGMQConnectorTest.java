package io.quarkiverse.messaging.pgmq.connector;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import java.time.OffsetDateTime;
import java.util.List;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;

import org.eclipse.microprofile.reactive.messaging.Channel;
import org.eclipse.microprofile.reactive.messaging.Emitter;
import org.eclipse.microprofile.reactive.messaging.Incoming;
import org.eclipse.microprofile.reactive.messaging.Message;
import org.eclipse.microprofile.reactive.messaging.Metadata;
import org.junit.jupiter.api.Test;

import io.quarkiverse.messaging.pgmq.client.PGMQMessage;
import io.quarkiverse.messaging.pgmq.client.PGMQMetrics;
import io.quarkiverse.messaging.pgmq.client.SendMessage;
import io.vertx.core.json.JsonObject;

public class PGMQConnectorTest extends PGMQTestBase {

    @Test
    void incomingChannel() {
        pgmq.createQueue(queueName).await().indefinitely();

        MapBasedConfig config = new MapBasedConfig()
                .with("mp.messaging.incoming.data.connector", PGMQConnector.CONNECTOR_NAME)
                .with("mp.messaging.incoming.data.queue", queueName);
        IncomingApp app = runApplication(config, IncomingApp.class);

        int expected = 10;
        for (int i = 0; i < expected; i++) {
            pgmq.send(queueName, JsonObject.of("key", "value " + i)).await().indefinitely();
        }

        await().atMost(5, TimeUnit.SECONDS).until(() -> app.received().size() == expected);
    }

    @Test
    void outgoingChannel() {
        pgmq.createQueue(queueName).await().indefinitely();

        MapBasedConfig config = new MapBasedConfig()
                .with("mp.messaging.outgoing.out.connector", PGMQConnector.CONNECTOR_NAME)
                .with("mp.messaging.outgoing.out.queue", queueName);
        OutgoingApp app = runApplication(config, OutgoingApp.class);

        int expected = 10;
        for (int i = 0; i < expected; i++) {
            app.send(JsonObject.of("key", "value " + i));
        }

        await().atMost(5, TimeUnit.SECONDS).untilAsserted(() -> {
            PGMQMetrics metrics = pgmq.getQueueMetrics(queueName).await().indefinitely();
            assertThat(metrics.queueLength()).isEqualTo(expected);
        });

        List<PGMQMessage> messages = pgmq.read(queueName, 1, expected).await().indefinitely();
        assertThat(messages).hasSize(expected);
        assertThat(messages).allSatisfy(m -> assertThat(m.message().containsKey("key")).isTrue());
    }

    @Test
    void bidirectional() {
        pgmq.createQueue(queueName).await().indefinitely();

        MapBasedConfig config = new MapBasedConfig()
                .with("mp.messaging.outgoing.out.connector", PGMQConnector.CONNECTOR_NAME)
                .with("mp.messaging.outgoing.out.queue", queueName)
                .with("mp.messaging.incoming.in.connector", PGMQConnector.CONNECTOR_NAME)
                .with("mp.messaging.incoming.in.queue", queueName);
        BidirectionalApp app = runApplication(config, BidirectionalApp.class);

        int expected = 5;
        for (int i = 0; i < expected; i++) {
            app.send(JsonObject.of("key", "value " + i));
        }

        await().atMost(10, TimeUnit.SECONDS).until(() -> app.received().size() == expected);
    }

    @Test
    void outgoingBatchChannel() {
        pgmq.createQueue(queueName).await().indefinitely();

        MapBasedConfig config = new MapBasedConfig()
                .with("mp.messaging.outgoing.out.connector", PGMQConnector.CONNECTOR_NAME)
                .with("mp.messaging.outgoing.out.queue", queueName)
                .with("mp.messaging.outgoing.out.batch", true)
                .with("mp.messaging.outgoing.out.batch-size", 5)
                .with("mp.messaging.outgoing.out.batch-delay", 500);
        OutgoingApp app = runApplication(config, OutgoingApp.class);

        int expected = 10;
        for (int i = 0; i < expected; i++) {
            app.send(JsonObject.of("key", "value " + i));
        }

        await().atMost(5, TimeUnit.SECONDS).untilAsserted(() -> {
            PGMQMetrics metrics = pgmq.getQueueMetrics(queueName).await().indefinitely();
            assertThat(metrics.queueLength()).isEqualTo(expected);
        });

        List<PGMQMessage> messages = pgmq.read(queueName, 1, expected).await().indefinitely();
        assertThat(messages).hasSize(expected);
        assertThat(messages).allSatisfy(m -> assertThat(m.message().containsKey("key")).isTrue());
    }

    @Test
    void autoCreateStandardQueue() {
        MapBasedConfig config = new MapBasedConfig()
                .with("mp.messaging.outgoing.out.connector", PGMQConnector.CONNECTOR_NAME)
                .with("mp.messaging.outgoing.out.queue", queueName)
                .with("mp.messaging.outgoing.out.queue.declare", true)
                .with("mp.messaging.outgoing.out.queue.declare.type", "non_partitioned");
        OutgoingApp app = runApplication(config, OutgoingApp.class);

        app.send(JsonObject.of("key", "value"));

        await().atMost(5, TimeUnit.SECONDS).untilAsserted(() -> {
            PGMQMetrics metrics = pgmq.getQueueMetrics(queueName).await().indefinitely();
            assertThat(metrics.queueLength()).isEqualTo(1);
        });
    }

    @Test
    void autoCreateUnloggedQueue() {
        MapBasedConfig config = new MapBasedConfig()
                .with("mp.messaging.incoming.data.connector", PGMQConnector.CONNECTOR_NAME)
                .with("mp.messaging.incoming.data.queue", queueName)
                .with("mp.messaging.incoming.data.queue.declare", true)
                .with("mp.messaging.incoming.data.ack-batch", true)
                .with("mp.messaging.incoming.data.queue.declare.type", "unlogged");
        IncomingApp app = runApplication(config, IncomingApp.class);

        await().until(() -> pgmq.listQueues().await().indefinitely().stream().anyMatch(q -> q.name().equals(queueName)));

        for (int i = 0; i < 10_000; i++) {
            pgmq.send(queueName, JsonObject.of("key", "value-" + i)).subscribeAsCompletionStage();
        }

        await().untilAsserted(() -> {
            assertThat(app.received()).hasSizeGreaterThanOrEqualTo(10_000);
            assertThat(pgmq.getQueueMetrics(queueName).await().indefinitely().queueLength()).isZero();
        });
    }

    @Test
    void autoCreatePartitionedQueue() {
        MapBasedConfig config = new MapBasedConfig()
                .with("mp.messaging.outgoing.out.connector", PGMQConnector.CONNECTOR_NAME)
                .with("mp.messaging.outgoing.out.queue", queueName)
                .with("mp.messaging.outgoing.out.queue.declare", true)
                .with("mp.messaging.outgoing.out.queue.declare.type", "partitioned")
                .with("mp.messaging.outgoing.out.queue.declare.partition-interval", "10000")
                .with("mp.messaging.outgoing.out.queue.declare.retention-interval", "100000");
        OutgoingApp app = runApplication(config, OutgoingApp.class);

        app.send(JsonObject.of("key", "value"));

        await().atMost(5, TimeUnit.SECONDS).untilAsserted(() -> {
            PGMQMetrics metrics = pgmq.getQueueMetrics(queueName).await().indefinitely();
            assertThat(metrics.queueLength()).isEqualTo(1);
        });
    }

    @Test
    void outgoingBatchWithHeaders() {
        pgmq.createQueue(queueName).await().indefinitely();

        MapBasedConfig config = new MapBasedConfig()
                .with("mp.messaging.outgoing.out.connector", PGMQConnector.CONNECTOR_NAME)
                .with("mp.messaging.outgoing.out.queue", queueName)
                .with("mp.messaging.outgoing.out.batch", true)
                .with("mp.messaging.outgoing.out.batch-size", 3)
                .with("mp.messaging.outgoing.out.batch-delay", 500)
                .with("mp.messaging.outgoing.out.tracing-enabled", false);
        HeadersApp app = runApplication(config, HeadersApp.class);

        int expected = 3;
        for (int i = 0; i < expected; i++) {
            app.send(JsonObject.of("key", "value-" + i), JsonObject.of("h1", "v" + i));
        }

        await().atMost(5, TimeUnit.SECONDS).untilAsserted(() -> {
            PGMQMetrics metrics = pgmq.getQueueMetrics(queueName).await().indefinitely();
            assertThat(metrics.queueLength()).isEqualTo(expected);
        });

        List<PGMQMessage> messages = pgmq.read(queueName, 1, expected).await().indefinitely();
        assertThat(messages).hasSize(expected);
        assertThat(messages).allSatisfy(m -> {
            assertThat(m.headers()).isNotNull();
            assertThat(m.headers().getString("h1")).startsWith("v");
        });
    }

    @Test
    void outgoingWithVisibilityTimeout() {
        pgmq.createQueue(queueName).await().indefinitely();

        MapBasedConfig config = new MapBasedConfig()
                .with("mp.messaging.outgoing.out.connector", PGMQConnector.CONNECTOR_NAME)
                .with("mp.messaging.outgoing.out.queue", queueName)
                .with("mp.messaging.outgoing.out.tracing-enabled", false);
        SendMessageApp app = runApplication(config, SendMessageApp.class);

        // Send with visibility timeout 60 seconds in the future — message should be invisible
        OffsetDateTime futureVt = OffsetDateTime.now().plusSeconds(60);
        app.send(JsonObject.of("key", "deferred"), futureVt);

        // Wait for the message to be enqueued — verify via queue table directly
        await().atMost(5, TimeUnit.SECONDS).untilAsserted(() -> {
            long count = pgmq.getClient()
                    .query("SELECT count(*) AS cnt FROM pgmq.q_" + queueName)
                    .execute()
                    .map(rows -> rows.iterator().next().getLong("cnt"))
                    .await().indefinitely();
            assertThat(count).isEqualTo(1);
        });

        // The message should not be readable yet (visibility timeout in the future)
        List<PGMQMessage> readResult = pgmq.read(queueName, 1, 1).await().indefinitely();
        assertThat(readResult).isEmpty();
    }

    @Test
    void headersViaOutgoingMetadata() {
        pgmq.createQueue(queueName).await().indefinitely();

        MapBasedConfig config = new MapBasedConfig()
                .with("mp.messaging.outgoing.out.connector", PGMQConnector.CONNECTOR_NAME)
                .with("mp.messaging.outgoing.out.queue", queueName);
        HeadersApp app = runApplication(config, HeadersApp.class);

        app.send(JsonObject.of("key", "value"), JsonObject.of("h1", "v1"));

        // Read back directly via client and verify headers were stored
        await().atMost(5, TimeUnit.SECONDS).untilAsserted(() -> {
            List<PGMQMessage> messages = pgmq.read(queueName, 1, 1).await().indefinitely();
            assertThat(messages).hasSize(1);
            assertThat(messages.get(0).headers()).isNotNull();
            assertThat(messages.get(0).headers().getString("h1")).isEqualTo("v1");
        });
    }

    @Test
    void headersViaIncomingMetadata() {
        pgmq.createQueue(queueName).await().indefinitely();

        MapBasedConfig config = new MapBasedConfig()
                .with("mp.messaging.incoming.in.connector", PGMQConnector.CONNECTOR_NAME)
                .with("mp.messaging.incoming.in.queue", queueName);
        HeadersReceivingApp app = runApplication(config, HeadersReceivingApp.class);

        // Send with headers directly via client
        pgmq.send(queueName, JsonObject.of("key", "value"), JsonObject.of("h1", "v1")).await().indefinitely();

        await().atMost(5, TimeUnit.SECONDS).until(() -> app.receivedHeaders().size() == 1);
        assertThat(app.receivedHeaders().get(0).getString("h1")).isEqualTo("v1");
    }

    @Test
    void outgoingQueueNameOverride() {
        String overrideQueue = queueName + "_override";
        pgmq.createQueue(queueName).await().indefinitely();
        pgmq.createQueue(overrideQueue).await().indefinitely();

        MapBasedConfig config = new MapBasedConfig()
                .with("mp.messaging.outgoing.out.connector", PGMQConnector.CONNECTOR_NAME)
                .with("mp.messaging.outgoing.out.queue", queueName)
                .with("mp.messaging.outgoing.out.tracing-enabled", false);
        QueueOverrideApp app = runApplication(config, QueueOverrideApp.class);

        app.send(JsonObject.of("key", "value"), overrideQueue);

        // Message should land in the override queue, not the default one
        await().atMost(5, TimeUnit.SECONDS).untilAsserted(() -> {
            PGMQMetrics overrideMetrics = pgmq.getQueueMetrics(overrideQueue).await().indefinitely();
            assertThat(overrideMetrics.queueLength()).isEqualTo(1);
        });

        PGMQMetrics defaultMetrics = pgmq.getQueueMetrics(queueName).await().indefinitely();
        assertThat(defaultMetrics.queueLength()).isZero();
    }

    @Test
    void incomingMetadataAccessors() {
        pgmq.createQueue(queueName).await().indefinitely();

        MapBasedConfig config = new MapBasedConfig()
                .with("mp.messaging.incoming.in.connector", PGMQConnector.CONNECTOR_NAME)
                .with("mp.messaging.incoming.in.queue", queueName);
        MetadataAccessorApp app = runApplication(config, MetadataAccessorApp.class);

        pgmq.send(queueName, JsonObject.of("key", "value"), JsonObject.of("h1", "v1")).await().indefinitely();

        await().atMost(5, TimeUnit.SECONDS).until(() -> app.receivedMetadata().size() == 1);

        PGMQIncomingMetadata meta = app.receivedMetadata().get(0);
        assertThat(meta.getId()).isPositive();
        assertThat(meta.getReadCount()).isGreaterThanOrEqualTo(1);
        assertThat(meta.getEnqueuedAt()).isNotNull();
        assertThat(meta.getVisibilityTimeout()).isNotNull();
        assertThat(meta.getBody()).isNotNull();
        assertThat(meta.getBody().getString("key")).isEqualTo("value");
        assertThat(meta.getHeaders()).isNotNull();
        assertThat(meta.getHeaders().getString("h1")).isEqualTo("v1");
        assertThat(meta.getMessage()).isNotNull();
        assertThat(meta.getMessage().msgId()).isEqualTo(meta.getId());
    }

    @Test
    void topicRouting() {
        pgmq.createQueue(queueName).await().indefinitely();

        MapBasedConfig config = new MapBasedConfig()
                .with("mp.messaging.outgoing.out.connector", PGMQConnector.CONNECTOR_NAME)
                .with("mp.messaging.outgoing.out.routing-key", "orders.new")
                .with("mp.messaging.incoming.in.connector", PGMQConnector.CONNECTOR_NAME)
                .with("mp.messaging.incoming.in.queue", queueName)
                .with("mp.messaging.incoming.in.topic-patterns", "orders.*");
        BidirectionalApp app = runApplication(config, BidirectionalApp.class);

        int expected = 3;
        for (int i = 0; i < expected; i++) {
            app.send(JsonObject.of("order", i));
        }

        await().atMost(5, TimeUnit.SECONDS).until(() -> app.received().size() == expected);
    }

    @Test
    void topicRoutingWithHeaders() {
        pgmq.createQueue(queueName).await().indefinitely();

        MapBasedConfig config = new MapBasedConfig()
                .with("mp.messaging.outgoing.out.connector", PGMQConnector.CONNECTOR_NAME)
                .with("mp.messaging.outgoing.out.routing-key", "events.new")
                .with("mp.messaging.outgoing.out.tracing-enabled", false)
                .with("mp.messaging.incoming.in.connector", PGMQConnector.CONNECTOR_NAME)
                .with("mp.messaging.incoming.in.queue", queueName)
                .with("mp.messaging.incoming.in.topic-patterns", "events.*");
        HeadersBidirectionalApp app = runApplication(config, HeadersBidirectionalApp.class);

        app.send(JsonObject.of("key", "value"), JsonObject.of("h1", "v1"));

        await().atMost(5, TimeUnit.SECONDS).until(() -> app.receivedHeaders().size() == 1);
        assertThat(app.receivedHeaders().get(0).getString("h1")).isEqualTo("v1");
    }

    @Test
    void fifoGroupedRead() {
        pgmq.createQueue(queueName).await().indefinitely();

        MapBasedConfig config = new MapBasedConfig()
                .with("mp.messaging.incoming.data.connector", PGMQConnector.CONNECTOR_NAME)
                .with("mp.messaging.incoming.data.queue", queueName)
                .with("mp.messaging.incoming.data.fifo", true)
                .with("mp.messaging.incoming.data.fifo-mode", "throughput");
        IncomingApp app = runApplication(config, IncomingApp.class);

        int expected = 5;
        for (int i = 0; i < expected; i++) {
            pgmq.send(queueName, JsonObject.of("key", "value " + i)).await().indefinitely();
        }

        await().atMost(5, TimeUnit.SECONDS).until(() -> app.received().size() == expected);
    }

    @Test
    void fifoGroupedRRRead() {
        pgmq.createQueue(queueName).await().indefinitely();

        MapBasedConfig config = new MapBasedConfig()
                .with("mp.messaging.incoming.data.connector", PGMQConnector.CONNECTOR_NAME)
                .with("mp.messaging.incoming.data.queue", queueName)
                .with("mp.messaging.incoming.data.fifo", true)
                .with("mp.messaging.incoming.data.fifo-mode", "round-robin");
        IncomingApp app = runApplication(config, IncomingApp.class);

        int expected = 5;
        for (int i = 0; i < expected; i++) {
            pgmq.send(queueName, JsonObject.of("key", "value " + i)).await().indefinitely();
        }

        await().atMost(5, TimeUnit.SECONDS).until(() -> app.received().size() == expected);
    }

    @Test
    void incomingChannelWithPojo() {
        pgmq.createQueue(queueName).await().indefinitely();

        MapBasedConfig config = new MapBasedConfig()
                .with("mp.messaging.incoming.data.connector", PGMQConnector.CONNECTOR_NAME)
                .with("mp.messaging.incoming.data.queue", queueName);
        PojoIncomingApp app = runApplication(config, PojoIncomingApp.class);

        int expected = 3;
        for (int i = 0; i < expected; i++) {
            pgmq.send(queueName, JsonObject.of("name", "item-" + i, "value", i)).await().indefinitely();
        }

        await().atMost(5, TimeUnit.SECONDS).until(() -> app.received().size() == expected);
        assertThat(app.received()).allSatisfy(pojo -> {
            assertThat(pojo.name).startsWith("item-");
            assertThat(pojo.value).isGreaterThanOrEqualTo(0);
        });
    }

    // --- App beans ---

    @ApplicationScoped
    public static class IncomingApp {
        List<String> received = new CopyOnWriteArrayList<>();

        @Incoming("data")
        void consume(JsonObject msg) {
            received.add(msg.toString());
        }

        public List<String> received() {
            return received;
        }
    }

    @ApplicationScoped
    public static class OutgoingApp {
        @Inject
        @Channel("out")
        Emitter<JsonObject> emitter;

        public void send(JsonObject msg) {
            emitter.send(msg);
        }
    }

    @ApplicationScoped
    public static class BidirectionalApp {
        @Inject
        @Channel("out")
        Emitter<JsonObject> emitter;

        List<String> received = new CopyOnWriteArrayList<>();

        @Incoming("in")
        void consume(JsonObject msg) {
            received.add(msg.toString());
        }

        public void send(JsonObject msg) {
            emitter.send(msg);
        }

        public List<String> received() {
            return received;
        }
    }

    @ApplicationScoped
    public static class HeadersApp {
        @Inject
        @Channel("out")
        Emitter<JsonObject> emitter;

        public void send(JsonObject payload, JsonObject headers) {
            PGMQOutgoingMetadata metadata = PGMQOutgoingMetadata.builder()
                    .withHeaders(headers)
                    .build();
            emitter.send(Message.of(payload, Metadata.of(metadata)));
        }
    }

    @ApplicationScoped
    public static class MetadataAccessorApp {
        List<PGMQIncomingMetadata> receivedMetadata = new CopyOnWriteArrayList<>();

        @Incoming("in")
        CompletionStage<Void> consume(Message<JsonObject> msg) {
            msg.getMetadata(PGMQIncomingMetadata.class).ifPresent(receivedMetadata::add);
            return msg.ack();
        }

        public List<PGMQIncomingMetadata> receivedMetadata() {
            return receivedMetadata;
        }
    }

    @ApplicationScoped
    public static class SendMessageApp {
        @Inject
        @Channel("out")
        Emitter<SendMessage> emitter;

        public void send(JsonObject payload, OffsetDateTime visibilityTimeout) {
            SendMessage<JsonObject> sendMessage = new SendMessage<>();
            sendMessage.setPayload(payload);
            sendMessage.setVisibilityTimeout(visibilityTimeout);
            emitter.send(sendMessage);
        }
    }

    @ApplicationScoped
    public static class QueueOverrideApp {
        @Inject
        @Channel("out")
        Emitter<JsonObject> emitter;

        public void send(JsonObject payload, String targetQueue) {
            PGMQOutgoingMetadata metadata = PGMQOutgoingMetadata.builder()
                    .withTopic(targetQueue)
                    .build();
            emitter.send(Message.of(payload, Metadata.of(metadata)));
        }
    }

    @ApplicationScoped
    public static class HeadersBidirectionalApp {
        @Inject
        @Channel("out")
        Emitter<JsonObject> emitter;

        List<JsonObject> receivedHeaders = new CopyOnWriteArrayList<>();

        @Incoming("in")
        CompletionStage<Void> consume(Message<JsonObject> msg) {
            msg.getMetadata(PGMQIncomingMetadata.class).ifPresent(meta -> {
                JsonObject h = meta.getHeaders();
                if (h != null) {
                    receivedHeaders.add(h);
                }
            });
            return msg.ack();
        }

        public void send(JsonObject payload, JsonObject headers) {
            PGMQOutgoingMetadata metadata = PGMQOutgoingMetadata.builder()
                    .withHeaders(headers)
                    .build();
            emitter.send(Message.of(payload, Metadata.of(metadata)));
        }

        public List<JsonObject> receivedHeaders() {
            return receivedHeaders;
        }
    }

    @ApplicationScoped
    public static class HeadersReceivingApp {
        List<JsonObject> receivedHeaders = new CopyOnWriteArrayList<>();

        @Incoming("in")
        CompletionStage<Void> consume(Message<JsonObject> msg) {
            msg.getMetadata(PGMQIncomingMetadata.class).ifPresent(meta -> {
                receivedHeaders.add(meta.getHeaders());
            });
            return msg.ack();
        }

        public List<JsonObject> receivedHeaders() {
            return receivedHeaders;
        }
    }

    public static class TestPojo {
        public String name;
        public int value;

        public TestPojo() {
        }
    }

    @ApplicationScoped
    public static class PojoIncomingApp {
        List<TestPojo> received = new CopyOnWriteArrayList<>();

        @Incoming("data")
        void consume(TestPojo msg) {
            received.add(msg);
        }

        public List<TestPojo> received() {
            return received;
        }
    }
}
