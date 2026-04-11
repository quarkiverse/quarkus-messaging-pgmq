package io.quarkiverse.reactive.messaging.pgmq.connector.client;

import java.time.OffsetDateTime;

/**
 * Record representing the metrics of a PGMQ queue.
 *
 * @param queueName the name of the queue
 * @param queueLength the current length of the queue
 * @param newestMsgAgeSec age of the newest message in seconds, or null if not available
 * @param oldestMsgAgeSec age of the oldest message in seconds, or null if not available
 * @param totalMessages total number of messages in the queue
 * @param scrapeTime time when these metrics were scraped
 */
public record PGMQMetrics(String queueName,
        long queueLength,
        Integer newestMsgAgeSec,
        Integer oldestMsgAgeSec,
        long totalMessages,
        OffsetDateTime scrapeTime) {
}
