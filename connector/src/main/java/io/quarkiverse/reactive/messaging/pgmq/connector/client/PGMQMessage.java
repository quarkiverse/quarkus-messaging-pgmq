package io.quarkiverse.reactive.messaging.pgmq.connector.client;

import java.time.OffsetDateTime;

import io.vertx.core.json.JsonObject;

/**
 * Record representing a PGMQ message.
 *
 * @param msgId the unique identifier of the message
 * @param readCount the number of times the message has been read
 * @param enqueuedAt the time when the message was enqueued
 * @param visibilityTimeout the time when the message will be visible again after being read
 * @param message the actual message content as a JSON object
 */
public record PGMQMessage(
        long msgId,
        int readCount,
        OffsetDateTime enqueuedAt,
        OffsetDateTime visibilityTimeout,
        JsonObject message) {
}
