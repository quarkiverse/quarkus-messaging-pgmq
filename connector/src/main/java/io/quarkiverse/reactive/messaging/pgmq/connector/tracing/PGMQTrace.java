package io.quarkiverse.reactive.messaging.pgmq.connector.tracing;

import java.util.Map;

public class PGMQTrace {
    private final String clientId;
    private final String topic;
    private final Map<String, String> messageProperties;

    private PGMQTrace(String clientId, String topic, Map<String, String> messageProperties) {
        this.clientId = clientId;
        this.topic = topic;
        this.messageProperties = messageProperties;
    }

    public String getClientId() {
        return clientId;
    }

    public String getTopic() {
        return topic;
    }

    public Map<String, String> getMessageProperties() {
        return messageProperties;
    }

    public static class Builder {
        private String clientId;
        private String topic;
        private Map<String, String> properties;

        public Builder withClientId(String clientId) {
            this.clientId = clientId;
            return this;
        }

        public Builder withQueue(String topic) {
            this.topic = topic;
            return this;
        }

        public Builder withProperties(Map<String, String> properties) {
            this.properties = properties;
            return this;
        }

        public PGMQTrace build() {
            return new PGMQTrace(clientId, topic, properties);
        }
    }
}
