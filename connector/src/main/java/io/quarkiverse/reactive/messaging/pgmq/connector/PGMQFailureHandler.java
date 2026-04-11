package io.quarkiverse.reactive.messaging.pgmq.connector;

import java.util.concurrent.CompletionStage;

import org.eclipse.microprofile.reactive.messaging.Metadata;

import io.quarkiverse.reactive.messaging.pgmq.connector.client.PGMQClient;

public class PGMQFailureHandler {

    private final String queueName;
    private final PGMQClient client;

    static PGMQFailureHandler create(String queueName, PGMQClient client) {
        return new PGMQFailureHandler(queueName, client);
    }

    public PGMQFailureHandler(String queueName, PGMQClient client) {
        this.queueName = queueName;
        this.client = client;
    }

    public CompletionStage<Void> handle(IncomingPGMQMessage msg, Throwable reason, Metadata metadata) {
        return client.setVisibilityTimeout(queueName, msg.getMessage().msgId(), 0)
                .replaceWithVoid()
                .emitOn(msg::runOnMessageContext)
                .subscribeAsCompletionStage();
    }
}
