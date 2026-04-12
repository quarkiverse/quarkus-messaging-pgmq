package io.quarkiverse.messaging.pgmq.connector.tracing;

import java.util.Map;

import io.opentelemetry.context.propagation.TextMapSetter;

public enum PGMQTraceTextMapSetter implements TextMapSetter<PGMQTrace> {
    INSTANCE;

    @Override
    public void set(final PGMQTrace carrier, final String key, final String value) {
        if (carrier != null) {
            // fill message properties from key, value attribute
            Map<String, String> properties = carrier.getMessageProperties();
            properties.put(key, value);
        }
    }
}
