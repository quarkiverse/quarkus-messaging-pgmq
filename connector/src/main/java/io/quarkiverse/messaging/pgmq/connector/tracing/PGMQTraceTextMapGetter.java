package io.quarkiverse.messaging.pgmq.connector.tracing;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Map;

import io.opentelemetry.context.propagation.TextMapGetter;

public enum PGMQTraceTextMapGetter implements TextMapGetter<PGMQTrace> {
    INSTANCE;

    @Override
    public Iterable<String> keys(final PGMQTrace carrier) {
        Map<String, String> headers = carrier.getMessageProperties();
        if (headers == null) {
            return Collections.emptyList();
        }
        return new ArrayList<>(headers.keySet());
    }

    @Override
    public String get(final PGMQTrace carrier, final String key) {
        if (carrier != null) {
            Map<String, String> properties = carrier.getMessageProperties();
            if (properties != null) {
                return properties.get(key);
            }
        }
        return null;
    }
}
