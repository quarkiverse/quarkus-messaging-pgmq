package io.quarkiverse.reactive.messaging.pgmq.connector.ack;

import java.util.function.BiConsumer;

import io.quarkiverse.reactive.messaging.pgmq.connector.IncomingPGMQMessage;
import io.quarkiverse.reactive.messaging.pgmq.connector.PGMQConnectorIncomingConfiguration;
import io.quarkiverse.reactive.messaging.pgmq.connector.client.PGMQClient;
import io.smallrye.mutiny.Uni;
import io.vertx.mutiny.core.Vertx;

public interface PGMQAckHandler {
    interface Strategy {
        String DELETE = "delete";
        String ARCHIVE = "archive";
        String IGNORE = "ignore";
    }

    interface Factory {
        PGMQAckHandler create(PGMQConnectorIncomingConfiguration conf, Vertx vertx, PGMQClient client,
                BiConsumer<Throwable, Boolean> reportFailure);
    }

    default Uni<IncomingPGMQMessage> received(IncomingPGMQMessage message) {
        return Uni.createFrom().item(message);
    }

    Uni<Void> handle(IncomingPGMQMessage message);

    default void close(boolean graceful) {

    }
}
