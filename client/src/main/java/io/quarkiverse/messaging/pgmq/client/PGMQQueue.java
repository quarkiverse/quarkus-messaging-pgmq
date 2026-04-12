package io.quarkiverse.messaging.pgmq.client;

import java.time.OffsetDateTime;

/**
 * Record representing a PGMQ queue.
 *
 * @param name the name of the queue
 * @param partitioned whether the queue is partitioned
 * @param unlogged whether the queue is unlogged
 * @param createdAt the creation time of the queue
 */
public record PGMQQueue(String name,
        boolean partitioned,
        boolean unlogged,
        OffsetDateTime createdAt) {
}
