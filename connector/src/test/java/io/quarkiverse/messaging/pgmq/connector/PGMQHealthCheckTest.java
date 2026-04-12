package io.quarkiverse.messaging.pgmq.connector;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;

import org.eclipse.microprofile.reactive.messaging.Channel;
import org.eclipse.microprofile.reactive.messaging.Emitter;
import org.eclipse.microprofile.reactive.messaging.Incoming;
import org.junit.jupiter.api.Test;

import io.quarkiverse.messaging.pgmq.client.PGMQMetrics;
import io.vertx.core.json.JsonObject;

public class PGMQHealthCheckTest extends PGMQTestBase {

    @Test
    void incomingChannelHealthy() {
        pgmq.createQueue(queueName).await().indefinitely();

        MapBasedConfig config = new MapBasedConfig()
                .with("mp.messaging.incoming.data.connector", PGMQConnector.CONNECTOR_NAME)
                .with("mp.messaging.incoming.data.queue", queueName);
        IncomingApp app = runApplication(config, IncomingApp.class);

        assertThat(isStarted()).isTrue();
        assertThat(isReady()).isTrue();
        assertThat(isAlive()).isTrue();

        pgmq.send(queueName, JsonObject.of("key", "value")).await().indefinitely();
        await().atMost(5, TimeUnit.SECONDS).until(() -> app.received().size() == 1);

        assertThat(isStarted()).isTrue();
        assertThat(isReady()).isTrue();
        assertThat(isAlive()).isTrue();
    }

    @Test
    void outgoingChannelHealthy() {
        pgmq.createQueue(queueName).await().indefinitely();

        MapBasedConfig config = new MapBasedConfig()
                .with("mp.messaging.outgoing.out.connector", PGMQConnector.CONNECTOR_NAME)
                .with("mp.messaging.outgoing.out.queue", queueName);
        OutgoingApp app = runApplication(config, OutgoingApp.class);

        assertThat(isStarted()).isTrue();
        assertThat(isReady()).isTrue();
        assertThat(isAlive()).isTrue();

        app.send(JsonObject.of("key", "value"));

        await().atMost(5, TimeUnit.SECONDS).untilAsserted(() -> {
            PGMQMetrics metrics = pgmq.getQueueMetrics(queueName).await().indefinitely();
            assertThat(metrics.queueLength()).isEqualTo(1);
        });

        assertThat(isStarted()).isTrue();
        assertThat(isReady()).isTrue();
        assertThat(isAlive()).isTrue();
    }

    @Test
    void bidirectionalChannelHealthy() {
        pgmq.createQueue(queueName).await().indefinitely();

        MapBasedConfig config = new MapBasedConfig()
                .with("mp.messaging.outgoing.out.connector", PGMQConnector.CONNECTOR_NAME)
                .with("mp.messaging.outgoing.out.queue", queueName)
                .with("mp.messaging.incoming.in.connector", PGMQConnector.CONNECTOR_NAME)
                .with("mp.messaging.incoming.in.queue", queueName);
        BidirectionalApp app = runApplication(config, BidirectionalApp.class);

        assertThat(isStarted()).isTrue();
        assertThat(isReady()).isTrue();
        assertThat(isAlive()).isTrue();

        app.send(JsonObject.of("key", "value"));
        await().atMost(5, TimeUnit.SECONDS).until(() -> app.received().size() == 1);

        assertThat(isStarted()).isTrue();
        assertThat(isReady()).isTrue();
        assertThat(isAlive()).isTrue();
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
}
