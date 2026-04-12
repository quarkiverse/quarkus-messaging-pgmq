package io.quarkiverse.messaging.pgmq.deployment;

import io.quarkus.runtime.annotations.ConfigPhase;
import io.quarkus.runtime.annotations.ConfigRoot;
import io.smallrye.config.ConfigMapping;
import io.smallrye.config.WithDefault;

@ConfigMapping(prefix = "quarkus.messaging-pgmq.devservices")
@ConfigRoot(phase = ConfigPhase.BUILD_TIME)
public interface PGMQDevServicesBuildTimeConfig {

    /**
     * Whether to configure the datasource dev services for PGMQ automatically.
     * <p>
     * When enabled, the dev services datasource will use a PostgreSQL image with the pgmq extension
     * pre-installed and run an init script to enable the extension.
     */
    @WithDefault("true")
    boolean enabled();
}
