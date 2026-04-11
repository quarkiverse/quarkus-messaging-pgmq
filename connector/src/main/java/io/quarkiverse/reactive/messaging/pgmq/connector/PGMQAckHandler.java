package io.quarkiverse.reactive.messaging.pgmq.connector;

import java.util.concurrent.CompletionStage;

import io.quarkiverse.reactive.messaging.pgmq.connector.client.PGMQClient;

public class PGMQAckHandler {

    private final String queueName;
    private final PGMQClient client;

    static PGMQAckHandler create(String queueName, PGMQClient client) {
        return new PGMQAckHandler(queueName, client);
    }

    public PGMQAckHandler(String queueName, PGMQClient client) {
        this.queueName = queueName;
        this.client = client;
    }

    public CompletionStage<Void> handle(IncomingPGMQMessage msg) {
        return client.delete(queueName, msg.getMessage().msgId())
                .emitOn(msg::runOnMessageContext)
                .replaceWithVoid()
                .subscribeAsCompletionStage();
    }
}
