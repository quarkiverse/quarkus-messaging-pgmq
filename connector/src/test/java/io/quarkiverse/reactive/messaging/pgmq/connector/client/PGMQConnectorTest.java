package io.quarkiverse.reactive.messaging.pgmq.connector.client;

import static org.awaitility.Awaitility.await;

import java.util.List;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;

import jakarta.enterprise.context.ApplicationScoped;

import org.eclipse.microprofile.reactive.messaging.Incoming;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import io.quarkiverse.reactive.messaging.pgmq.connector.PGMQConnector;
import io.vertx.core.json.JsonObject;

public class PGMQConnectorTest extends PGMQTestBase {

    private PGMQClient pgmq;

    @BeforeEach
    @Override
    void setUp() {
        super.setUp();
        pgmq = getClient();
    }

    @Test
    void incomingChannel() {
        String myQueue = UUID.randomUUID().toString();
        pgmq.createQueue(myQueue).await().indefinitely();

        MapBasedConfig config = new MapBasedConfig()
                .with("mp.messaging.incoming.data.connector", PGMQConnector.CONNECTOR_NAME)
                .with("mp.messaging.incoming.data.queue", myQueue);
        PGMQApp app = runApplication(config, PGMQApp.class);

        int expected = 10;
        for (int i = 0; i < expected; i++) {
            pgmq.send(myQueue, JsonObject.of("map", "value " + i)).await().indefinitely();
        }

        await().atMost(3, TimeUnit.SECONDS).until(() -> app.received().size() == expected);
    }

    @ApplicationScoped
    public static class PGMQApp {

        List<String> received = new CopyOnWriteArrayList<>();

        @Incoming("data")
        void consume(JsonObject msg) {
            received.add(msg.toString());
        }

        public List<String> received() {
            return received;
        }
    }
}
