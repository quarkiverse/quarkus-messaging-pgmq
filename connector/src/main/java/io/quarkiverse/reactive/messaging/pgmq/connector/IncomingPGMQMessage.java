package io.quarkiverse.reactive.messaging.pgmq.connector;

import java.util.concurrent.CompletionStage;

import org.eclipse.microprofile.reactive.messaging.Metadata;

import io.quarkiverse.reactive.messaging.pgmq.connector.client.PGMQMessage;
import io.smallrye.reactive.messaging.providers.MetadataInjectableMessage;
import io.smallrye.reactive.messaging.providers.locals.ContextAwareMessage;
import io.vertx.core.json.JsonObject;

public class IncomingPGMQMessage implements ContextAwareMessage<JsonObject>, MetadataInjectableMessage<JsonObject> {

    private final JsonObject payload;
    private Metadata metadata;
    private final PGMQAckHandler ackHandler;

    private final PGMQFailureHandler nackHandler;
    private final PGMQMessage consumed;

    public IncomingPGMQMessage(PGMQMessage message, PGMQAckHandler ackHandler, PGMQFailureHandler nackHandler) {
        this.consumed = message;
        this.payload = message.message();
        this.ackHandler = ackHandler;
        this.nackHandler = nackHandler;
        this.metadata = ContextAwareMessage.captureContextMetadata(new PGMQIncomingMetadata(message));
    }

    public PGMQMessage getMessage() {
        return consumed;
    }

    @Override
    public JsonObject getPayload() {
        return payload;
    }

    @Override
    public Metadata getMetadata() {
        return metadata;
    }

    @Override
    public CompletionStage<Void> ack() {
        return ackHandler.handle(this);
    }

    @Override
    public CompletionStage<Void> nack(Throwable reason, Metadata nackMetadata) {
        return nackHandler.handle(this, reason, nackMetadata);
    }

    @Override
    public void injectMetadata(Object metadataObject) {
        this.metadata = this.metadata.with(metadataObject);
    }
}
