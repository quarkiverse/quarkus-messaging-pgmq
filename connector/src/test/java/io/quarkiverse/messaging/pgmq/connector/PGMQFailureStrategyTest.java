package io.quarkiverse.messaging.pgmq.connector;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import jakarta.enterprise.context.ApplicationScoped;

import org.eclipse.microprofile.reactive.messaging.Incoming;
import org.junit.jupiter.api.Test;

import io.quarkiverse.messaging.pgmq.client.PGMQMetrics;
import io.vertx.core.json.JsonObject;

public class PGMQFailureStrategyTest extends PGMQTestBase {

    @Test
    void nackResetsVisibility() {
        pgmq.createQueue(queueName).await().indefinitely();

        pgmq.send(queueName, JsonObject.of("key", "value")).await().indefinitely();

        MapBasedConfig config = new MapBasedConfig()
                .with("mp.messaging.incoming.data.connector", PGMQConnector.CONNECTOR_NAME)
                .with("mp.messaging.incoming.data.queue", queueName);
        NackApp app = runApplication(config, NackApp.class);

        // First attempt fails (nack resets visibility), second attempt succeeds
        await().atMost(10, TimeUnit.SECONDS).until(() -> app.received().size() == 1);
        assertThat(app.attempts().get()).isGreaterThanOrEqualTo(2);
    }

    @Test
    void failureStrategyDelete() {
        pgmq.createQueue(queueName).await().indefinitely();

        pgmq.send(queueName, JsonObject.of("key", "value")).await().indefinitely();

        MapBasedConfig config = new MapBasedConfig()
                .with("mp.messaging.incoming.data.connector", PGMQConnector.CONNECTOR_NAME)
                .with("mp.messaging.incoming.data.queue", queueName)
                .with("mp.messaging.incoming.data.failure-type", "delete");
        FailOnceApp app = runApplication(config, FailOnceApp.class);

        // First attempt fails (nack deletes the message), message is gone
        // The message should eventually be consumed on retry or the queue should be empty
        await().atMost(5, TimeUnit.SECONDS).untilAsserted(() -> {
            assertThat(app.attempts().get()).isGreaterThanOrEqualTo(1);
            PGMQMetrics metrics = pgmq.getQueueMetrics(queueName).await().indefinitely();
            assertThat(metrics.queueLength()).isZero();
        });
    }

    @Test
    void failureStrategyIgnore() {
        pgmq.createQueue(queueName).await().indefinitely();

        pgmq.send(queueName, JsonObject.of("key", "value")).await().indefinitely();

        MapBasedConfig config = new MapBasedConfig()
                .with("mp.messaging.incoming.data.connector", PGMQConnector.CONNECTOR_NAME)
                .with("mp.messaging.incoming.data.queue", queueName)
                .with("mp.messaging.incoming.data.failure-type", "ignore")
                .with("mp.messaging.incoming.data.visibility-timeout", 30);
        AlwaysFailApp app = runApplication(config, AlwaysFailApp.class);

        // First attempt fails, ignore strategy does nothing — message stays invisible until vt expires
        await().atMost(5, TimeUnit.SECONDS).until(() -> app.attempts().get() >= 1);

        // Message should still be in the queue (invisible, not deleted)
        PGMQMetrics metrics = pgmq.getQueueMetrics(queueName).await().indefinitely();
        assertThat(metrics.queueLength()).isEqualTo(1);
    }

    @Test
    void failureStrategyArchive() {
        pgmq.createQueue(queueName).await().indefinitely();

        pgmq.send(queueName, JsonObject.of("key", "value")).await().indefinitely();

        MapBasedConfig config = new MapBasedConfig()
                .with("mp.messaging.incoming.data.connector", PGMQConnector.CONNECTOR_NAME)
                .with("mp.messaging.incoming.data.queue", queueName)
                .with("mp.messaging.incoming.data.failure-type", "archive");
        FailOnceApp app = runApplication(config, FailOnceApp.class);

        // First attempt fails (nack archives the message), message is removed from queue
        await().atMost(5, TimeUnit.SECONDS).untilAsserted(() -> {
            assertThat(app.attempts().get()).isGreaterThanOrEqualTo(1);
            PGMQMetrics metrics = pgmq.getQueueMetrics(queueName).await().indefinitely();
            assertThat(metrics.queueLength()).isZero();
        });

        // Verify message was archived (not just deleted)
        long archived = pgmq.getClient()
                .query("SELECT count(*) AS cnt FROM pgmq.a_" + queueName)
                .execute()
                .map(rows -> rows.iterator().next().getLong("cnt"))
                .await().indefinitely();
        assertThat(archived).isEqualTo(1);

        pgmq.dropQueue(queueName).await().indefinitely();
    }

    @Test
    void nackDeleteOverride() {
        pgmq.createQueue(queueName).await().indefinitely();

        pgmq.send(queueName, JsonObject.of("key", "value")).await().indefinitely();

        // Default failure-strategy is "visibility", but the app overrides per-message to delete
        MapBasedConfig config = new MapBasedConfig()
                .with("mp.messaging.incoming.data.connector", PGMQConnector.CONNECTOR_NAME)
                .with("mp.messaging.incoming.data.queue", queueName);
        DeleteOnFailApp app = runApplication(config, DeleteOnFailApp.class);

        // First attempt fails — per-message override deletes the message instead of resetting visibility
        await().atMost(5, TimeUnit.SECONDS).untilAsserted(() -> {
            assertThat(app.attempts().get()).isGreaterThanOrEqualTo(1);
            PGMQMetrics metrics = pgmq.getQueueMetrics(queueName).await().indefinitely();
            assertThat(metrics.queueLength()).isZero();
        });
    }

    // --- App beans ---

    @ApplicationScoped
    public static class NackApp {
        AtomicInteger attempts = new AtomicInteger(0);
        List<String> received = new CopyOnWriteArrayList<>();

        @Incoming("data")
        void consume(JsonObject msg) {
            if (attempts.incrementAndGet() == 1) {
                throw new RuntimeException("Simulated failure");
            }
            received.add(msg.toString());
        }

        public AtomicInteger attempts() {
            return attempts;
        }

        public List<String> received() {
            return received;
        }
    }

    @ApplicationScoped
    public static class FailOnceApp {
        AtomicInteger attempts = new AtomicInteger(0);

        @Incoming("data")
        void consume(JsonObject msg) {
            if (attempts.incrementAndGet() == 1) {
                throw new RuntimeException("Simulated failure");
            }
        }

        public AtomicInteger attempts() {
            return attempts;
        }
    }

    @ApplicationScoped
    public static class AlwaysFailApp {
        AtomicInteger attempts = new AtomicInteger(0);

        @Incoming("data")
        void consume(JsonObject msg) {
            attempts.incrementAndGet();
            throw new RuntimeException("Simulated failure");
        }

        public AtomicInteger attempts() {
            return attempts;
        }
    }

    @ApplicationScoped
    public static class DeleteOnFailApp {
        AtomicInteger attempts = new AtomicInteger(0);

        @Incoming("data")
        void consume(JsonObject msg, PgmqAcknowledgement ack) {
            attempts.incrementAndGet();
            ack.delete();
            throw new RuntimeException("Simulated failure");
        }

        public AtomicInteger attempts() {
            return attempts;
        }
    }
}
