package io.quarkiverse.messaging.pgmq.deployment;

import java.util.Collections;
import java.util.Map;
import java.util.OptionalInt;

import org.eclipse.microprofile.config.spi.ConfigSource;

import io.smallrye.config.ConfigSourceContext;
import io.smallrye.config.ConfigSourceFactory;
import io.smallrye.config.ConfigValue;
import io.smallrye.config.PropertiesConfigSource;

public class DevServicesConfigSourceFactory implements ConfigSourceFactory {

    private static final String ENABLED_PROPERTY = "quarkus.messaging-pgmq.devservices.enabled";

    @Override
    public Iterable<ConfigSource> getConfigSources(final ConfigSourceContext context) {
        ConfigValue enabled = context.getValue(ENABLED_PROPERTY);
        if (enabled != null && "false".equals(enabled.getValue())) {
            return Collections.emptyList();
        }
        return Collections.singletonList(
                new PropertiesConfigSource(
                        Map.of("quarkus.datasource.devservices.image-name", "ghcr.io/pgmq/pg18-pgmq:v1.11.0",
                                "quarkus.datasource.devservices.init-script-path", "init-pgmq.sql"),
                        "pgmq-devservices-defaults",
                        Integer.MIN_VALUE));
    }

    @Override
    public OptionalInt getPriority() {
        return OptionalInt.of(Integer.MIN_VALUE);
    }
}
