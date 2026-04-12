package io.quarkiverse.messaging.pgmq.client;

import java.time.OffsetDateTime;

/**
 * Record representing a notify insert throttle configuration.
 *
 * @param queueName the name of the queue
 * @param throttleIntervalMs the throttle interval in milliseconds
 * @param lastNotifiedAt the time of the last notification
 */
public record PGMQNotifyThrottle(String queueName, int throttleIntervalMs, OffsetDateTime lastNotifiedAt) {
}
