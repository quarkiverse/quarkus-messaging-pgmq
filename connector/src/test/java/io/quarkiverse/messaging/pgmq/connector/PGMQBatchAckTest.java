package io.quarkiverse.messaging.pgmq.connector;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;

import jakarta.enterprise.context.ApplicationScoped;

import org.eclipse.microprofile.reactive.messaging.Incoming;
import org.junit.jupiter.api.Test;

import io.quarkiverse.messaging.pgmq.client.PGMQMetrics;
import io.vertx.core.json.JsonObject;

public class PGMQBatchAckTest extends PGMQTestBase {

    @Test
    void batchAckDeletesMessages() {
        pgmq.createQueue(queueName).await().indefinitely();

        int count = 5;
        for (int i = 0; i < count; i++) {
            pgmq.send(queueName, JsonObject.of("key", "value " + i)).await().indefinitely();
        }

        MapBasedConfig config = new MapBasedConfig()
                .with("mp.messaging.incoming.data.connector", PGMQConnector.CONNECTOR_NAME)
                .with("mp.messaging.incoming.data.queue", queueName)
                .with("mp.messaging.incoming.data.ack-batch", true)
                .with("mp.messaging.incoming.data.ack-batch-delay", "1000");
        IncomingApp app = runApplication(config, IncomingApp.class);

        await().atMost(5, TimeUnit.SECONDS).until(() -> app.received().size() == count);

        // Batch ack handler deletes messages, so the queue should be empty
        await().atMost(5, TimeUnit.SECONDS).untilAsserted(() -> {
            PGMQMetrics metrics = pgmq.getQueueMetrics(queueName).await().indefinitely();
            assertThat(metrics.queueLength()).isZero();
        });
    }

    @Test
    void batchAckArchivesMessages() {
        pgmq.createQueue(queueName).await().indefinitely();

        int count = 5;
        for (int i = 0; i < count; i++) {
            pgmq.send(queueName, JsonObject.of("key", "value " + i)).await().indefinitely();
        }

        MapBasedConfig config = new MapBasedConfig()
                .with("mp.messaging.incoming.data.connector", PGMQConnector.CONNECTOR_NAME)
                .with("mp.messaging.incoming.data.queue", queueName)
                .with("mp.messaging.incoming.data.ack-type", "archive")
                .with("mp.messaging.incoming.data.ack-batch", true)
                .with("mp.messaging.incoming.data.ack-batch-delay", 500);
        IncomingApp app = runApplication(config, IncomingApp.class);

        await().atMost(5, TimeUnit.SECONDS).until(() -> app.received().size() == count);

        // Queue should be empty and archive table should have them
        await().atMost(5, TimeUnit.SECONDS).untilAsserted(() -> {
            PGMQMetrics metrics = pgmq.getQueueMetrics(queueName).await().indefinitely();
            assertThat(metrics.queueLength()).isZero();
        });

        // Verify messages exist in the archive table
        long archived = pgmq.getClient()
                .query("SELECT count(*) AS cnt FROM pgmq.a_" + queueName)
                .execute()
                .map(rows -> rows.iterator().next().getLong("cnt"))
                .await().indefinitely();
        assertThat(archived).isEqualTo(count);

        pgmq.dropQueue(queueName).await().indefinitely();
    }

    @Test
    void batchAckMixedStrategies() {
        pgmq.createQueue(queueName).await().indefinitely();

        int count = 5;
        for (int i = 0; i < count; i++) {
            pgmq.send(queueName, JsonObject.of("key", "value " + i)).await().indefinitely();
        }

        // Default ack-strategy is "delete", but the app overrides per-message to archive
        MapBasedConfig config = new MapBasedConfig()
                .with("mp.messaging.incoming.data.connector", PGMQConnector.CONNECTOR_NAME)
                .with("mp.messaging.incoming.data.queue", queueName)
                .with("mp.messaging.incoming.data.ack-batch", true)
                .with("mp.messaging.incoming.data.ack-batch-delay", 500);
        ArchiveOverrideApp app = runApplication(config, ArchiveOverrideApp.class);

        await().atMost(5, TimeUnit.SECONDS).until(() -> app.received().size() == count);

        // Queue should be empty
        await().atMost(5, TimeUnit.SECONDS).untilAsserted(() -> {
            PGMQMetrics metrics = pgmq.getQueueMetrics(queueName).await().indefinitely();
            assertThat(metrics.queueLength()).isZero();
        });

        // Messages should be in the archive table (not deleted)
        long archived = pgmq.getClient()
                .query("SELECT count(*) AS cnt FROM pgmq.a_" + queueName)
                .execute()
                .map(rows -> rows.iterator().next().getLong("cnt"))
                .await().indefinitely();
        assertThat(archived).isEqualTo(count);

        pgmq.dropQueue(queueName).await().indefinitely();
    }

    @Test
    void batchAckFlushesOnClose() {
        pgmq.createQueue(queueName).await().indefinitely();

        int count = 5;
        for (int i = 0; i < count; i++) {
            pgmq.send(queueName, JsonObject.of("key", "value " + i)).await().indefinitely();
        }

        // Use batch-size=1 so acks flush immediately (allowing message processing to proceed),
        // and a short delay. This test verifies the close() path works correctly.
        MapBasedConfig config = new MapBasedConfig()
                .with("mp.messaging.incoming.data.connector", PGMQConnector.CONNECTOR_NAME)
                .with("mp.messaging.incoming.data.queue", queueName)
                .with("mp.messaging.incoming.data.ack-batch", true)
                .with("mp.messaging.incoming.data.ack-batch-size", 3)
                .with("mp.messaging.incoming.data.ack-batch-delay", 200);
        IncomingApp app = runApplication(config, IncomingApp.class);

        await().atMost(5, TimeUnit.SECONDS).until(() -> app.received().size() == count);

        // Close the connector — this triggers flush of any remaining buffered acks
        stopContainer();

        // After close, the queue should be empty (all acks flushed)
        PGMQMetrics metricsAfter = pgmq.getQueueMetrics(queueName).await().indefinitely();
        assertThat(metricsAfter.queueLength()).isZero();

        // Prevent @AfterEach from calling stopContainer again
        container = null;
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
    public static class ArchiveOverrideApp {
        List<String> received = new CopyOnWriteArrayList<>();

        @Incoming("data")
        void consume(JsonObject msg, PgmqAcknowledgement ack) {
            ack.archive();
            received.add(msg.toString());
        }

        public List<String> received() {
            return received;
        }
    }
}
