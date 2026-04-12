package io.quarkiverse.messaging.pgmq.client;

import java.time.OffsetDateTime;

import io.vertx.core.json.JsonObject;

public class SendMessage<T> {

    T payload;

    String queueName;

    int delaySeconds;

    OffsetDateTime visibilityTimeout;

    JsonObject headers;

    public T getPayload() {
        return payload;
    }

    public void setPayload(T payload) {
        this.payload = payload;
    }

    public String getQueueName() {
        return queueName;
    }

    public void setQueueName(String queueName) {
        this.queueName = queueName;
    }

    public void setDelay(int delaySeconds) {
        this.delaySeconds = delaySeconds;
    }

    public int getDelay() {
        return this.delaySeconds;
    }

    public OffsetDateTime getVisibilityTimeout() {
        return visibilityTimeout;
    }

    public void setVisibilityTimeout(OffsetDateTime visibilityTimeout) {
        this.visibilityTimeout = visibilityTimeout;
    }

    public JsonObject getHeaders() {
        return headers;
    }

    public void setHeaders(JsonObject headers) {
        this.headers = headers;
    }
}
