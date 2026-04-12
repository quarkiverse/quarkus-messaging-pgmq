package io.quarkiverse.messaging.pgmq.connector;

import java.time.OffsetDateTime;

import io.quarkiverse.messaging.pgmq.client.PGMQMessage;
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

    public int getReadCount() {
        return msg.readCount();
    }

    public OffsetDateTime getEnqueuedAt() {
        return msg.enqueuedAt();
    }

    public OffsetDateTime getVisibilityTimeout() {
        return msg.visibilityTimeout();
    }

    public JsonObject getHeaders() {
        return msg.headers();
    }
}
