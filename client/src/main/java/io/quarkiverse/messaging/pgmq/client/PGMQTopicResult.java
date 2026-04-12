package io.quarkiverse.messaging.pgmq.client;

/**
 * Record representing the result of a topic-based send operation.
 *
 * @param queueName the name of the queue the message was routed to
 * @param msgId the unique identifier of the sent message
 */
public record PGMQTopicResult(String queueName, long msgId) {
}
