package io.quarkiverse.messaging.pgmq.connector;

import java.util.Locale;
import java.util.Optional;

public class PgmqAcknowledgement {

    public enum Strategy {
        DELETE,
        ARCHIVE,
        VISIBILITY,
        IGNORE;

        public static Strategy from(String value) {
            return Strategy.valueOf(value.toUpperCase(Locale.ROOT));
        }
    }

    private Strategy strategy;
    private Integer visibilityTimeout;

    public void delete() {
        this.strategy = Strategy.DELETE;
    }

    public void archive() {
        this.strategy = Strategy.ARCHIVE;
    }

    public void visibilityTimeout(int seconds) {
        this.strategy = Strategy.VISIBILITY;
        this.visibilityTimeout = seconds;
    }

    public void ignore() {
        this.strategy = Strategy.IGNORE;
    }

    Optional<Strategy> getStrategy() {
        return Optional.ofNullable(strategy);
    }

    Optional<Integer> getVisibilityTimeout() {
        return Optional.ofNullable(visibilityTimeout);
    }
}
