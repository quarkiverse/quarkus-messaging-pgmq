package io.quarkiverse.messaging.pgmq.connector;

import java.util.Optional;
import java.util.concurrent.CompletionStage;

import org.eclipse.microprofile.reactive.messaging.Metadata;

import io.quarkiverse.messaging.pgmq.client.PGMQClient;
import io.quarkiverse.messaging.pgmq.connector.i18n.PGMQLogging;
import io.smallrye.mutiny.Uni;

public class PGMQAckHandler {

    protected final String queueName;
    protected final PGMQClient client;
    protected final PgmqAcknowledgement.Strategy defaultAckStrategy;
    protected final PgmqAcknowledgement.Strategy defaultFailureStrategy;

    public PGMQAckHandler(PGMQConnectorIncomingConfiguration cfg, PGMQClient client) {
        this.queueName = cfg.getQueue().orElse(cfg.getChannel());
        this.client = client;
        this.defaultAckStrategy = PgmqAcknowledgement.Strategy.from(cfg.getAckType());
        this.defaultFailureStrategy = PgmqAcknowledgement.Strategy.from(cfg.getFailureType());
    }

    public CompletionStage<Void> handle(IncomingPGMQMessage msg, Metadata metadata,
            PgmqAcknowledgement.Strategy strategy, int visibilityTimeout) {
        return switch (strategy) {
            case VISIBILITY -> client.setVisibilityTimeout(queueName, msg.getMessage().msgId(), visibilityTimeout)
                    .replaceWithVoid()
                    .emitOn(msg::runOnMessageContext)
                    .subscribeAsCompletionStage();
            case DELETE -> client.delete(queueName, msg.getMessage().msgId())
                    .replaceWithVoid()
                    .emitOn(msg::runOnMessageContext)
                    .subscribeAsCompletionStage();
            case ARCHIVE -> client.archive(queueName, msg.getMessage().msgId())
                    .replaceWithVoid()
                    .emitOn(msg::runOnMessageContext)
                    .subscribeAsCompletionStage();
            case IGNORE -> Uni.createFrom().voidItem()
                    .emitOn(msg::runOnMessageContext)
                    .subscribeAsCompletionStage();
        };
    }

    public CompletionStage<Void> ack(IncomingPGMQMessage msg, Metadata metadata) {
        Optional<PgmqAcknowledgement> ackMaybe = metadata.get(PgmqAcknowledgement.class)
                .or(() -> msg.getMetadata(PgmqAcknowledgement.class));
        PgmqAcknowledgement.Strategy strategy = ackMaybe.flatMap(PgmqAcknowledgement::getStrategy)
                .orElse(defaultAckStrategy);
        int visibilityTimeout = ackMaybe.flatMap(PgmqAcknowledgement::getVisibilityTimeout)
                .orElse(0);
        return handle(msg, metadata, strategy, visibilityTimeout);
    }

    public CompletionStage<Void> nack(IncomingPGMQMessage msg, Throwable reason, Metadata metadata) {
        Optional<PgmqAcknowledgement> ackMaybe = metadata.get(PgmqAcknowledgement.class)
                .or(() -> msg.getMetadata(PgmqAcknowledgement.class));
        PgmqAcknowledgement.Strategy strategy = ackMaybe.flatMap(PgmqAcknowledgement::getStrategy)
                .orElse(defaultFailureStrategy);
        int visibilityTimeout = ackMaybe.flatMap(PgmqAcknowledgement::getVisibilityTimeout)
                .orElse(0);
        PGMQLogging.log.messageNacked(msg.getMessage().msgId(), queueName, reason);
        return handle(msg, metadata, strategy, visibilityTimeout);
    }

    public void close() {
        // no-op for the base handler; overridden by PGMQThrottledAckHandler
    }
}
