package io.quarkiverse.reactive.messaging.pgmq.connector.fault;

import java.util.function.BiConsumer;

import org.eclipse.microprofile.reactive.messaging.Metadata;

import io.quarkiverse.reactive.messaging.pgmq.connector.IncomingPGMQMessage;
import io.smallrye.mutiny.Uni;

public interface PGMQFailureHandler {
    interface Strategy {
        String DELETE = "delete";
        String ARCHIVE = "archive";
        String VISIBILITY = "visibility";
        String FAIL = "fail";
        String IGNORE = "ignore";
    }

    interface Factory {
        PGMQFailureHandler create(String queueName, PGMQFailureHandler client,
                BiConsumer<Throwable, Boolean> reportFailure);
    }

    Uni<Void> handle(IncomingPGMQMessage message, Metadata metadata, Throwable throwable);

}
