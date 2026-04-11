package io.quarkiverse.reactive.messaging.pgmq.connector;

import java.time.OffsetDateTime;

import io.quarkiverse.reactive.messaging.pgmq.connector.client.PGMQMessage;
import io.vertx.core.json.JsonObject;

public class PGMQIncomingMetadata {

    private final PGMQMessage msg;

    public PGMQIncomingMetadata(PGMQMessage msg) {
        this.msg = msg;
    }

    public PGMQMessage getMessage() {
        return msg;
    }

    public JsonObject getBody() {
        return msg.message();
    }

    public Long getId() {
        return msg.msgId();
    }

    public int getReadCound() {
        return msg.readCount();
    }

    public OffsetDateTime getEnqueuedAt() {
        return msg.enqueuedAt();
    }

    public OffsetDateTime getVisibilityTimeout() {
        return msg.visibilityTimeout();
    }
}
