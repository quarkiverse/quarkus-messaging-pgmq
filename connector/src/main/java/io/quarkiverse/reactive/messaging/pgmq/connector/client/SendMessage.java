package io.quarkiverse.reactive.messaging.pgmq.connector.client;

import java.time.OffsetDateTime;

public class SendMessage<T> {

    T payload;

    String queueName;

    int delaySeconds;

    OffsetDateTime visibilityTimeout;

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
}
