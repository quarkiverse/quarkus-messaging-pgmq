package io.quarkiverse.reactive.messaging.pgmq.connector;

public class PGMQOutgoingMetadata {
    private String queueName;
    private String key;

    public static PGMQOutgoingMetadataBuilder builder() {
        return new PGMQOutgoingMetadataBuilder();
    }

    public static PGMQOutgoingMetadataBuilder builder(PGMQOutgoingMetadata metadata) {
        return new PGMQOutgoingMetadataBuilder(metadata);
    }

    public PGMQOutgoingMetadata(String queueName, String key) {
        this.queueName = queueName;
        this.key = key;
    }

    public String getQueueName() {
        return queueName;
    }

    public String getKey() {
        return key;
    }

    public static class PGMQOutgoingMetadataBuilder {
        private String topic;
        private String key;

        public PGMQOutgoingMetadataBuilder() {

        }

        public PGMQOutgoingMetadataBuilder(PGMQOutgoingMetadata metadata) {
            this.key = metadata.getKey();
            this.topic = metadata.getQueueName();
        }

        public PGMQOutgoingMetadataBuilder withTopic(String topic) {
            this.topic = topic;
            return this;
        }

        public PGMQOutgoingMetadataBuilder withKey(String key) {
            this.key = key;
            return this;
        }

        public PGMQOutgoingMetadata build() {
            return new PGMQOutgoingMetadata(topic, key);
        }
    }

}
