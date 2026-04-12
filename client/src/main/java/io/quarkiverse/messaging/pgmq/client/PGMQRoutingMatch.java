package io.quarkiverse.messaging.pgmq.client;

/**
 * Record representing a routing match result from test_routing.
 *
 * @param pattern the topic pattern that matched
 * @param queueName the queue name bound to the pattern
 * @param compiledRegex the compiled regex used for matching
 */
public record PGMQRoutingMatch(String pattern, String queueName, String compiledRegex) {
}
