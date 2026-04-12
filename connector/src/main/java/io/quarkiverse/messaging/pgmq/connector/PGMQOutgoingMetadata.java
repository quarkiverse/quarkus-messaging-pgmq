package io.quarkiverse.messaging.pgmq.connector;

import io.vertx.core.json.JsonObject;

public class PGMQOutgoingMetadata {
    private String queueName;
    private String key;
    private JsonObject headers;

    public static PGMQOutgoingMetadataBuilder builder() {
        return new PGMQOutgoingMetadataBuilder();
    }

    public static PGMQOutgoingMetadataBuilder builder(PGMQOutgoingMetadata metadata) {
        return new PGMQOutgoingMetadataBuilder(metadata);
    }

    public PGMQOutgoingMetadata(String queueName, String key) {
        this(queueName, key, null);
    }

    public PGMQOutgoingMetadata(String queueName, String key, JsonObject headers) {
        this.queueName = queueName;
        this.key = key;
        this.headers = headers;
    }

    public String getQueueName() {
        return queueName;
    }

    public String getKey() {
        return key;
    }

    public JsonObject getHeaders() {
        return headers;
    }

    public static class PGMQOutgoingMetadataBuilder {
        private String topic;
        private String key;
        private JsonObject headers;

        public PGMQOutgoingMetadataBuilder() {

        }

        public PGMQOutgoingMetadataBuilder(PGMQOutgoingMetadata metadata) {
            this.key = metadata.getKey();
            this.topic = metadata.getQueueName();
            this.headers = metadata.getHeaders();
        }

        public PGMQOutgoingMetadataBuilder withTopic(String topic) {
            this.topic = topic;
            return this;
        }

        public PGMQOutgoingMetadataBuilder withKey(String key) {
            this.key = key;
            return this;
        }

        public PGMQOutgoingMetadataBuilder withHeaders(JsonObject headers) {
            this.headers = headers;
            return this;
        }

        public PGMQOutgoingMetadata build() {
            return new PGMQOutgoingMetadata(topic, key, headers);
        }
    }

}
