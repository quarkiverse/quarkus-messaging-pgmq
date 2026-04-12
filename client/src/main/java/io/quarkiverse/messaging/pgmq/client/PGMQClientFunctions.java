package io.quarkiverse.messaging.pgmq.client;

import java.time.OffsetDateTime;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import io.smallrye.mutiny.Uni;
import io.vertx.core.json.JsonObject;
import io.vertx.mutiny.sqlclient.Row;
import io.vertx.mutiny.sqlclient.RowSet;
import io.vertx.mutiny.sqlclient.SqlClient;
import io.vertx.mutiny.sqlclient.Tuple;

/**
 * Client functions for interacting with the PGMQ (PostgreSQL Message Queue) system.
 * Provides methods for creating queues, sending messages, reading messages, and managing queues.
 *
 * @param <T> the type of SqlClient used to interact with the database
 */
public class PGMQClientFunctions<T extends SqlClient> {
    protected final T client;

    public PGMQClientFunctions(T client) {
        this.client = client;
    }

    // Message row mapper
    public static PGMQMessage rowToMessage(Row row) {
        int headersIdx = row.getColumnIndex("headers");
        JsonObject headers = headersIdx >= 0 ? row.get(JsonObject.class, headersIdx) : null;
        return new PGMQMessage(
                row.getLong("msg_id"),
                row.getInteger("read_ct"),
                row.getOffsetDateTime("enqueued_at"),
                row.getOffsetDateTime("vt"),
                row.get(JsonObject.class, row.getColumnIndex("message")),
                headers);
    }

    // Queue row mapper
    public static PGMQQueue rowToQueue(Row row) {
        return new PGMQQueue(
                row.getString("queue_name"),
                row.getBoolean("is_partitioned"),
                row.getBoolean("is_unlogged"),
                row.getOffsetDateTime("created_at"));
    }

    // Queue row mapper
    public static PGMQMetrics rowToMetrics(Row row) {
        return new PGMQMetrics(
                row.getString("queue_name"),
                row.getLong("queue_length"),
                row.getInteger("newest_msg_age_sec"),
                row.getInteger("oldest_msg_age_sec"),
                row.getLong("total_messages"),
                row.getOffsetDateTime("scrape_time"));
    }

    public static PGMQTopicResult rowToTopicResult(Row row) {
        return new PGMQTopicResult(row.getString("queue_name"), row.getLong("msg_id"));
    }

    public static PGMQRoutingMatch rowToRoutingMatch(Row row) {
        return new PGMQRoutingMatch(
                row.getString("pattern"),
                row.getString("queue_name"),
                row.getString("compiled_regex"));
    }

    public static PGMQNotifyThrottle rowToNotifyThrottle(Row row) {
        return new PGMQNotifyThrottle(
                row.getString("queue_name"),
                row.getInteger("throttle_interval_ms"),
                row.getOffsetDateTime("last_notified_at"));
    }

    private static Stream<Row> streamRows(RowSet<Row> resultSet) {
        return StreamSupport.stream(resultSet.spliterator(), false);
    }

    /**
     * Returns the underlying SqlClient used by this PGMQClientFunctions instance.
     *
     * @return the SqlClient instance T
     */
    public T getClient() {
        return client;
    }

    /**
     * Creates a new queue with the specified name.
     *
     * @param queueName the name of the queue to create
     * @return a Uni that completes when the queue is created
     */
    public Uni<Void> createQueue(String queueName) {
        return client.preparedQuery("SELECT pgmq.create($1::text)")
                .execute(Tuple.of(queueName)).replaceWithVoid();
    }

    /**
     * Creates a new unlogged queue with the specified name.
     * Unlogged queues do not persist data to disk, making them faster but less durable.
     *
     * @param queueName the name of the unlogged queue to create
     * @return a Uni that completes when the unlogged queue is created
     */
    public Uni<Void> createUnloggedQueue(String queueName) {
        return client.preparedQuery("SELECT pgmq.create_unlogged($1::text)")
                .execute(Tuple.of(queueName)).replaceWithVoid();
    }

    /**
     * Creates a partitioned queue with the specified name, interval, and retention hours.
     *
     * @param queueName the name of the partitioned queue to create
     * @param partitionInterval the interval for partitioning (e.g., '1 day', '1 hour')
     * @param retentionInterval the number of messages to retain in the queue
     * @return a Uni that completes when the partitioned queue is created
     */
    public Uni<Void> createPartitionedQueue(String queueName, String partitionInterval, String retentionInterval) {
        return client.preparedQuery("SELECT pgmq.create_partitioned($1::text, $2::text, $3::text)")
                .execute(Tuple.of(queueName, partitionInterval, retentionInterval)).replaceWithVoid();
    }

    /**
     * Creates a partitioned queue with the specified name and default retention.
     *
     * @param queueName the name of the partitioned queue to create
     * @return a Uni that completes when the partitioned queue is created
     */
    public Uni<Void> createPartitionedQueue(String queueName) {
        return client.preparedQuery("SELECT pgmq.create_partitioned($1::text)")
                .execute(Tuple.of(queueName)).replaceWithVoid();
    }

    /**
     * Drops the specified queue.
     *
     * @param queueName the name of the queue to drop
     * @return a Uni that completes when the queue is dropped
     */
    public Uni<Void> dropQueue(String queueName) {
        return client.preparedQuery("SELECT pgmq.drop_queue($1::text)")
                .execute(Tuple.of(queueName)).replaceWithVoid();
    }

    // Message sending

    /**
     * Sends a message to the specified queue.
     *
     * @param queueName the name of the queue to send the message to
     * @param message the message to send, represented as a JsonObject
     * @return a Uni that completes with the message ID when the message is sent
     */
    public Uni<Long> send(String queueName, JsonObject message) {
        return client.preparedQuery("SELECT * FROM pgmq.send($1::text, $2::jsonb)")
                .execute(Tuple.of(queueName, message))
                .map(rows -> rows.iterator().next().getLong("send"));
    }

    /**
     * Sends a message to the specified queue.
     *
     * @param queueName the name of the queue to send the message to
     * @param message the message to send, represented as a JsonObject
     * @param delaySeconds the delay in seconds before the message becomes visible in the queue
     * @return a Uni that completes with the message ID when the message is sent
     */
    public Uni<Long> send(String queueName, JsonObject message, int delaySeconds) {
        return client.preparedQuery("SELECT * FROM pgmq.send($1::text, $2::jsonb, $3::integer)")
                .execute(Tuple.of(queueName, message, delaySeconds))
                .map(rows -> rows.iterator().next().getLong("send"));
    }

    /**
     * Sends a message to the specified queue.
     *
     * @param queueName the name of the queue to send the message to
     * @param message the message to send, represented as a JsonObject
     * @param visibleAt the timestamp at which the message should become visible in the queue
     * @return a Uni that completes with the message ID when the message is sent
     */
    public Uni<Long> send(String queueName, JsonObject message, OffsetDateTime visibleAt) {
        return client.preparedQuery("SELECT * FROM pgmq.send($1::text, $2::jsonb, $3::timestamptz)")
                .execute(Tuple.of(queueName, message, visibleAt))
                .map(rows -> rows.iterator().next().getLong("send"));
    }

    /**
     * Sends a batch of messages to the specified queue.
     *
     * @param queueName the name of the queue to send the messages to
     * @param messages a list of messages to send, each represented as a JsonObject
     * @return a Uni that completes with a list of message IDs when the messages are sent
     */
    public Uni<List<Long>> sendBatch(String queueName, List<JsonObject> messages) {
        return client.preparedQuery("SELECT * FROM pgmq.send_batch($1::text, $2::jsonb[])")
                .execute(Tuple.of(queueName, messages.toArray()))
                .map(rows -> streamRows(rows)
                        .map(r -> r.getLong("send_batch")).collect(Collectors.toList()));
    }

    /**
     * Sends a batch of messages to the specified queue.
     *
     * @param queueName the name of the queue to send the messages to
     * @param messages a list of messages to send, each represented as a JsonObject
     * @param delaySeconds the delay in seconds before the messages become visible in the queue
     * @return a Uni that completes with a list of message IDs when the messages are sent
     */
    public Uni<List<Long>> sendBatch(String queueName, List<JsonObject> messages, int delaySeconds) {
        return client.preparedQuery("SELECT * FROM pgmq.send_batch($1::text, $2::jsonb[], $3::integer)")
                .execute(Tuple.of(queueName, messages.toArray(), delaySeconds))
                .map(rows -> streamRows(rows)
                        .map(r -> r.getLong("send_batch")).collect(Collectors.toList()));
    }

    /**
     * Sends a batch of messages to the specified queue.
     *
     * @param queueName the name of the queue to send the messages to
     * @param messages a list of messages to send, each represented as a JsonObject
     * @param visibleAt the timestamp at which the messages should become visible in the queue
     * @return a Uni that completes with a list of message IDs when the messages are sent
     */
    public Uni<List<Long>> sendBatch(String queueName, List<JsonObject> messages, OffsetDateTime visibleAt) {
        return client.preparedQuery("SELECT * FROM pgmq.send_batch($1::text, $2::jsonb[], $3::timestamptz)")
                .execute(Tuple.of(queueName, messages.toArray(), visibleAt))
                .map(rows -> streamRows(rows)
                        .map(r -> r.getLong("send_batch")).collect(Collectors.toList()));
    }

    // Reading
    /**
     * Reads messages from the specified queue with a visibility timeout.
     *
     * @param queueName the name of the queue to read from
     * @param vtSeconds the visibility timeout in seconds
     * @param qty the maximum number of messages to read
     * @return a Uni that completes with a list of messages read from the queue
     */
    public Uni<List<PGMQMessage>> read(String queueName, int vtSeconds, int qty) {
        return client.preparedQuery("SELECT * FROM pgmq.read($1::text, $2::integer, $3::integer)")
                .execute(Tuple.of(queueName, vtSeconds, qty))
                .map(rows -> streamRows(rows).map(PGMQClientFunctions::rowToMessage).toList());
    }

    /**
     * Reads messages from the specified queue with a polling mechanism with default poll interval and max poll seconds.
     *
     * @param queueName the name of the queue to read from
     * @param vtSeconds the visibility timeout in seconds
     * @param qty the maximum number of messages to read
     * @return a Uni that completes with a list of messages read from the queue
     */
    public Uni<List<PGMQMessage>> readWithPoll(String queueName, int vtSeconds, int qty) {
        return client.preparedQuery("SELECT * FROM pgmq.read_with_poll($1::text, $2::integer, $3::integer)")
                .execute(Tuple.of(queueName, vtSeconds, qty))
                .map(rows -> streamRows(rows).map(PGMQClientFunctions::rowToMessage).collect(Collectors.toList()));
    }

    /**
     * Reads messages from the specified queue with a polling mechanism.
     *
     * @param queueName the name of the queue to read from
     * @param vtSeconds the visibility timeout in seconds
     * @param qty the maximum number of messages to read
     * @param maxPollSeconds the maximum time to wait for messages
     * @param pollIntervalMs Milliseconds between the internal poll operations.
     * @return a Uni that completes with a list of messages read from the queue
     */
    public Uni<List<PGMQMessage>> readWithPoll(String queueName, int vtSeconds, int qty, int maxPollSeconds,
            int pollIntervalMs) {
        return client
                .preparedQuery(
                        "SELECT * FROM pgmq.read_with_poll($1::text, $2::integer, $3::integer, $4::integer, $5::integer)")
                .execute(Tuple.of(queueName, vtSeconds, qty, maxPollSeconds, pollIntervalMs))
                .map(rows -> streamRows(rows).map(PGMQClientFunctions::rowToMessage).collect(Collectors.toList()));
    }

    /**
     * Pops a message from the specified queue.
     *
     * @param queueName the name of the queue to pop a message from
     * @return a Uni that completes with an Optional containing the popped message, or empty if no message was available
     */
    public Uni<Optional<PGMQMessage>> pop(String queueName) {
        return client.preparedQuery("SELECT * FROM pgmq.pop($1::text)")
                .execute(Tuple.of(queueName))
                .map(rows -> rows.size() == 0 ? Optional.empty() : Optional.of(rowToMessage(rows.iterator().next())));
    }

    // Message deletion and visibility

    /**
     * Deletes a message from the specified queue.
     *
     * @param queueName the name of the queue to delete the message from
     * @param msgId the ID of the message to delete
     * @return a Uni that completes with true if the message was deleted, false otherwise
     */
    public Uni<Boolean> delete(String queueName, long msgId) {
        return client.preparedQuery("SELECT pgmq.delete($1::text, $2::bigint)")
                .execute(Tuple.of(queueName, msgId))
                .map(rows -> rows.iterator().next().getBoolean("delete"));
    }

    /**
     * Deletes a message from the specified queue.
     *
     * @param queueName the name of the queue to delete the message from
     * @param msgIds the list of message IDs to delete
     * @return a Uni that completes with the list of message ids if the message was deleted
     */
    public Uni<List<Long>> delete(String queueName, List<Long> msgIds) {
        return client.preparedQuery("SELECT pgmq.delete($1::text, $2::bigint[])")
                .execute(Tuple.of(queueName, msgIds.toArray(new Long[0])))
                .map(rows -> streamRows(rows).map(r -> r.getLong("delete")).toList());
    }

    /**
     * Deletes a batch of messages from the specified queue.
     *
     * @param queueName the name of the queue to delete messages from
     * @param msgId a list of message IDs to delete
     * @return a Uni that completes with a list of message IDs that were successfully deleted
     */
    public Uni<PGMQMessage> setVisibilityTimeout(String queueName, long msgId, int vtOffsetSeconds) {
        return client.preparedQuery("SELECT * FROM pgmq.set_vt($1::text, $2::bigint, $3::integer)")
                .execute(Tuple.of(queueName, msgId, vtOffsetSeconds))
                .map(rows -> rowToMessage(rows.iterator().next()));
    }

    // Archive functions

    /**
     * Archives a message in the specified queue.
     *
     * @param queueName the name of the queue to archive the message from
     * @param msgId the ID of the message to archive
     * @return a Uni that completes with true if the message was archived, false otherwise
     */
    public Uni<Boolean> archive(String queueName, long msgId) {
        return client.preparedQuery("SELECT pgmq.archive($1::text, $2::bigint)")
                .execute(Tuple.of(queueName, msgId))
                .map(rows -> rows.iterator().next().getBoolean("archive"));
    }

    /**
     * Archives a batch of messages in the specified queue.
     *
     * @param queueName the name of the queue to archive messages from
     * @param msgIds a list of message IDs to archive
     * @return a Uni that completes with a list of message IDs that were successfully archived
     */
    public Uni<List<Long>> archive(String queueName, List<Long> msgIds) {
        return client.preparedQuery("SELECT * FROM pgmq.archive($1::text, $2::bigint[])")
                .execute(Tuple.of(queueName, msgIds.toArray(new Long[0])))
                .map(rows -> streamRows(rows)
                        .map(r -> r.getLong("archive")).collect(Collectors.toList()));
    }

    /**
     * Purges all messages from the specified queue.
     *
     * @param queueName the name of the queue to purge
     * @return a Uni that completes when the queue is purged
     */
    public Uni<Void> purgeQueue(String queueName) {
        return client.preparedQuery("SELECT pgmq.purge_queue($1::text)")
                .execute(Tuple.of(queueName)).replaceWithVoid();
    }

    /**
     * Detaches the archive for the specified queue.
     *
     * @param queueName the name of the queue to detach the archive from
     * @return a Uni that completes when the archive is detached
     */
    public Uni<Void> detachArchive(String queueName) {
        return client.preparedQuery("SELECT pgmq.detach_archive($1::text)")
                .execute(Tuple.of(queueName)).replaceWithVoid();
    }

    // Queue inspection

    /**
     * Lists all queues in the system.
     *
     * @return a Uni that completes with a list of queue names
     */
    public Uni<List<PGMQQueue>> listQueues() {
        return client.query("SELECT * FROM pgmq.list_queues()")
                .execute()
                .map(rows -> streamRows(rows).map(PGMQClientFunctions::rowToQueue).collect(Collectors.toList()));
    }

    /**
     * Gets the metrics for a specific queue.
     *
     * @param queueName the name of the queue to get metrics for
     * @return a Uni that completes with a JsonObject containing the queue metrics
     */
    public Uni<PGMQMetrics> getQueueMetrics(String queueName) {
        return client.preparedQuery("SELECT * FROM pgmq.metrics($1::text)")
                .execute(Tuple.of(queueName))
                .map(rows -> rowToMetrics(rows.iterator().next()));
    }

    /**
     * Gets the metrics for all queues in the system.
     *
     * @return a Uni that completes with a list of JsonObjects, each containing metrics for a queue
     */
    public Uni<List<PGMQMetrics>> getAllQueueMetrics() {
        return client.query("SELECT * FROM pgmq.metrics_all()")
                .execute()
                .map(rows -> streamRows(rows).map(PGMQClientFunctions::rowToMetrics).collect(Collectors.toList()));
    }

    // Queue Management — additional

    /**
     * Creates a new non-partitioned queue with the specified name.
     *
     * @param queueName the name of the queue to create
     * @return a Uni that completes when the queue is created
     */
    public Uni<Void> createNonPartitioned(String queueName) {
        return client.preparedQuery("SELECT pgmq.create_non_partitioned($1::text)")
                .execute(Tuple.of(queueName)).replaceWithVoid();
    }

    /**
     * Converts an existing archive table to a partitioned table.
     *
     * @param tableName the name of the table to convert
     * @param partitionInterval the partition interval (e.g., '1 day')
     * @param retentionInterval the retention interval (e.g., '30 days')
     * @param leadingPartition the number of leading partitions to create
     * @return a Uni that completes when the conversion is done
     */
    public Uni<Void> convertArchivePartitioned(String tableName, String partitionInterval, String retentionInterval,
            int leadingPartition) {
        return client
                .preparedQuery(
                        "SELECT pgmq.convert_archive_partitioned($1::text, $2::text, $3::text, $4::integer)")
                .execute(Tuple.of(tableName, partitionInterval, retentionInterval, leadingPartition)).replaceWithVoid();
    }

    /**
     * Converts an existing archive table to a partitioned table with default settings.
     *
     * @param tableName the name of the table to convert
     * @return a Uni that completes when the conversion is done
     */
    public Uni<Void> convertArchivePartitioned(String tableName) {
        return client.preparedQuery("SELECT pgmq.convert_archive_partitioned($1::text)")
                .execute(Tuple.of(tableName)).replaceWithVoid();
    }

    // Sending — headers overloads

    /**
     * Sends a message with headers to the specified queue.
     *
     * @param queueName the name of the queue to send the message to
     * @param message the message to send
     * @param headers the headers to attach to the message
     * @return a Uni that completes with the message ID
     */
    public Uni<Long> send(String queueName, JsonObject message, JsonObject headers) {
        return client.preparedQuery("SELECT * FROM pgmq.send($1::text, $2::jsonb, $3::jsonb)")
                .execute(Tuple.of(queueName, message, headers))
                .map(rows -> rows.iterator().next().getLong("send"));
    }

    /**
     * Sends a message with headers to the specified queue with a delay.
     *
     * @param queueName the name of the queue to send the message to
     * @param message the message to send
     * @param headers the headers to attach to the message
     * @param delaySeconds the delay in seconds before the message becomes visible
     * @return a Uni that completes with the message ID
     */
    public Uni<Long> send(String queueName, JsonObject message, JsonObject headers, int delaySeconds) {
        return client.preparedQuery("SELECT * FROM pgmq.send($1::text, $2::jsonb, $3::jsonb, $4::integer)")
                .execute(Tuple.of(queueName, message, headers, delaySeconds))
                .map(rows -> rows.iterator().next().getLong("send"));
    }

    /**
     * Sends a message with headers to the specified queue with a visibility timestamp.
     *
     * @param queueName the name of the queue to send the message to
     * @param message the message to send
     * @param headers the headers to attach to the message
     * @param visibleAt the timestamp at which the message should become visible
     * @return a Uni that completes with the message ID
     */
    public Uni<Long> send(String queueName, JsonObject message, JsonObject headers, OffsetDateTime visibleAt) {
        return client.preparedQuery("SELECT * FROM pgmq.send($1::text, $2::jsonb, $3::jsonb, $4::timestamptz)")
                .execute(Tuple.of(queueName, message, headers, visibleAt))
                .map(rows -> rows.iterator().next().getLong("send"));
    }

    /**
     * Sends a batch of messages with headers to the specified queue.
     *
     * @param queueName the name of the queue to send the messages to
     * @param messages a list of messages to send
     * @param headers a list of headers corresponding to each message
     * @return a Uni that completes with a list of message IDs
     */
    public Uni<List<Long>> sendBatch(String queueName, List<JsonObject> messages, List<JsonObject> headers) {
        return client.preparedQuery("SELECT * FROM pgmq.send_batch($1::text, $2::jsonb[], $3::jsonb[])")
                .execute(Tuple.of(queueName, messages.toArray(), headers.toArray()))
                .map(rows -> streamRows(rows)
                        .map(r -> r.getLong("send_batch")).collect(Collectors.toList()));
    }

    /**
     * Sends a batch of messages with headers to the specified queue with a delay.
     *
     * @param queueName the name of the queue to send the messages to
     * @param messages a list of messages to send
     * @param headers a list of headers corresponding to each message
     * @param delaySeconds the delay in seconds before the messages become visible
     * @return a Uni that completes with a list of message IDs
     */
    public Uni<List<Long>> sendBatch(String queueName, List<JsonObject> messages, List<JsonObject> headers,
            int delaySeconds) {
        return client
                .preparedQuery("SELECT * FROM pgmq.send_batch($1::text, $2::jsonb[], $3::jsonb[], $4::integer)")
                .execute(Tuple.of(queueName, messages.toArray(), headers.toArray(), delaySeconds))
                .map(rows -> streamRows(rows)
                        .map(r -> r.getLong("send_batch")).collect(Collectors.toList()));
    }

    /**
     * Sends a batch of messages with headers to the specified queue with a visibility timestamp.
     *
     * @param queueName the name of the queue to send the messages to
     * @param messages a list of messages to send
     * @param headers a list of headers corresponding to each message
     * @param visibleAt the timestamp at which the messages should become visible
     * @return a Uni that completes with a list of message IDs
     */
    public Uni<List<Long>> sendBatch(String queueName, List<JsonObject> messages, List<JsonObject> headers,
            OffsetDateTime visibleAt) {
        return client
                .preparedQuery("SELECT * FROM pgmq.send_batch($1::text, $2::jsonb[], $3::jsonb[], $4::timestamptz)")
                .execute(Tuple.of(queueName, messages.toArray(), headers.toArray(), visibleAt))
                .map(rows -> streamRows(rows)
                        .map(r -> r.getLong("send_batch")).collect(Collectors.toList()));
    }

    // Topic-Based Routing

    /**
     * Sends a message to queues matching the routing key.
     *
     * @param routingKey the routing key to match against topic patterns
     * @param message the message to send
     * @return a Uni that completes with the number of queues the message was sent to
     */
    public Uni<Integer> sendTopic(String routingKey, JsonObject message) {
        return client.preparedQuery("SELECT pgmq.send_topic($1::text, $2::jsonb)")
                .execute(Tuple.of(routingKey, message))
                .map(rows -> rows.iterator().next().getInteger("send_topic"));
    }

    /**
     * Sends a message to queues matching the routing key with a delay.
     *
     * @param routingKey the routing key to match against topic patterns
     * @param message the message to send
     * @param delaySeconds the delay in seconds before the message becomes visible
     * @return a Uni that completes with the number of queues the message was sent to
     */
    public Uni<Integer> sendTopic(String routingKey, JsonObject message, int delaySeconds) {
        return client.preparedQuery("SELECT pgmq.send_topic($1::text, $2::jsonb, $3::integer)")
                .execute(Tuple.of(routingKey, message, delaySeconds))
                .map(rows -> rows.iterator().next().getInteger("send_topic"));
    }

    /**
     * Sends a message with headers and delay to queues matching the routing key.
     *
     * @param routingKey the routing key to match against topic patterns
     * @param message the message to send
     * @param headers the headers to attach to the message
     * @param delaySeconds the delay in seconds before the message becomes visible
     * @return a Uni that completes with the number of queues the message was sent to
     */
    public Uni<Integer> sendTopic(String routingKey, JsonObject message, JsonObject headers, int delaySeconds) {
        return client
                .preparedQuery("SELECT pgmq.send_topic($1::text, $2::jsonb, $3::jsonb, $4::integer)")
                .execute(Tuple.of(routingKey, message, headers, delaySeconds))
                .map(rows -> rows.iterator().next().getInteger("send_topic"));
    }

    /**
     * Sends a batch of messages to queues matching the routing key.
     *
     * @param routingKey the routing key to match against topic patterns
     * @param messages a list of messages to send
     * @return a Uni that completes with a list of topic results
     */
    public Uni<List<PGMQTopicResult>> sendBatchTopic(String routingKey, List<JsonObject> messages) {
        return client.preparedQuery("SELECT * FROM pgmq.send_batch_topic($1::text, $2::jsonb[])")
                .execute(Tuple.of(routingKey, messages.toArray()))
                .map(rows -> streamRows(rows)
                        .map(PGMQClientFunctions::rowToTopicResult)
                        .collect(Collectors.toList()));
    }

    /**
     * Sends a batch of messages with headers to queues matching the routing key.
     *
     * @param routingKey the routing key to match against topic patterns
     * @param messages a list of messages to send
     * @param headers a list of headers corresponding to each message
     * @return a Uni that completes with a list of topic results
     */
    public Uni<List<PGMQTopicResult>> sendBatchTopic(String routingKey, List<JsonObject> messages,
            List<JsonObject> headers) {
        return client.preparedQuery("SELECT * FROM pgmq.send_batch_topic($1::text, $2::jsonb[], $3::jsonb[])")
                .execute(Tuple.of(routingKey, messages.toArray(), headers.toArray()))
                .map(rows -> streamRows(rows)
                        .map(PGMQClientFunctions::rowToTopicResult)
                        .collect(Collectors.toList()));
    }

    /**
     * Binds a topic pattern to a queue.
     *
     * @param pattern the topic pattern to bind (supports wildcards)
     * @param queueName the name of the queue to bind to the pattern
     * @return a Uni that completes when the binding is created
     */
    public Uni<Void> bindTopic(String pattern, String queueName) {
        return client.preparedQuery("SELECT pgmq.bind_topic($1::text, $2::text)")
                .execute(Tuple.of(pattern, queueName)).replaceWithVoid();
    }

    /**
     * Unbinds a topic pattern from a queue.
     *
     * @param pattern the topic pattern to unbind
     * @param queueName the name of the queue to unbind from the pattern
     * @return a Uni that completes with true if the binding was removed, false otherwise
     */
    public Uni<Boolean> unbindTopic(String pattern, String queueName) {
        return client.preparedQuery("SELECT pgmq.unbind_topic($1::text, $2::text)")
                .execute(Tuple.of(pattern, queueName))
                .map(rows -> rows.iterator().next().getBoolean("unbind_topic"));
    }

    /**
     * Tests which queues a routing key would match.
     *
     * @param routingKey the routing key to test
     * @return a Uni that completes with a list of routing matches
     */
    public Uni<List<PGMQRoutingMatch>> testRouting(String routingKey) {
        return client.preparedQuery("SELECT * FROM pgmq.test_routing($1::text)")
                .execute(Tuple.of(routingKey))
                .map(rows -> streamRows(rows)
                        .map(PGMQClientFunctions::rowToRoutingMatch)
                        .collect(Collectors.toList()));
    }

    // Reading — conditional & grouped

    /**
     * Reads messages from the specified queue with a conditional filter.
     *
     * @param queueName the name of the queue to read from
     * @param vtSeconds the visibility timeout in seconds
     * @param qty the maximum number of messages to read
     * @param conditional a SQL conditional expression to filter messages
     * @return a Uni that completes with a list of messages matching the condition
     */
    public Uni<List<PGMQMessage>> read(String queueName, int vtSeconds, int qty, String conditional) {
        return client
                .preparedQuery("SELECT * FROM pgmq.read($1::text, $2::integer, $3::integer, $4::jsonb)")
                .execute(Tuple.of(queueName, vtSeconds, qty, new JsonObject(conditional)))
                .map(rows -> streamRows(rows).map(PGMQClientFunctions::rowToMessage).toList());
    }

    /**
     * Reads messages from the specified queue with a polling mechanism, poll settings, and a conditional filter.
     *
     * @param queueName the name of the queue to read from
     * @param vtSeconds the visibility timeout in seconds
     * @param qty the maximum number of messages to read
     * @param maxPollSeconds the maximum time to wait for messages
     * @param pollIntervalMs milliseconds between internal poll operations
     * @param conditional a SQL conditional expression to filter messages
     * @return a Uni that completes with a list of messages matching the condition
     */
    public Uni<List<PGMQMessage>> readWithPoll(String queueName, int vtSeconds, int qty, int maxPollSeconds,
            int pollIntervalMs, String conditional) {
        return client
                .preparedQuery(
                        "SELECT * FROM pgmq.read_with_poll($1::text, $2::integer, $3::integer, $4::integer, $5::integer, $6::jsonb)")
                .execute(Tuple.of(queueName, vtSeconds, qty, maxPollSeconds, pollIntervalMs,
                        new JsonObject(conditional)))
                .map(rows -> streamRows(rows).map(PGMQClientFunctions::rowToMessage).collect(Collectors.toList()));
    }

    /**
     * Reads messages from the specified queue grouped by message key.
     *
     * @param queueName the name of the queue to read from
     * @param vtSeconds the visibility timeout in seconds
     * @param qty the maximum number of messages to read
     * @return a Uni that completes with a list of grouped messages
     */
    public Uni<List<PGMQMessage>> readGrouped(String queueName, int vtSeconds, int qty) {
        return client.preparedQuery("SELECT * FROM pgmq.read_grouped($1::text, $2::integer, $3::integer)")
                .execute(Tuple.of(queueName, vtSeconds, qty))
                .map(rows -> streamRows(rows).map(PGMQClientFunctions::rowToMessage).toList());
    }

    /**
     * Reads messages from the specified queue grouped by message key with polling.
     *
     * @param queueName the name of the queue to read from
     * @param vtSeconds the visibility timeout in seconds
     * @param qty the maximum number of messages to read
     * @return a Uni that completes with a list of grouped messages
     */
    public Uni<List<PGMQMessage>> readGroupedWithPoll(String queueName, int vtSeconds, int qty) {
        return client
                .preparedQuery("SELECT * FROM pgmq.read_grouped_with_poll($1::text, $2::integer, $3::integer)")
                .execute(Tuple.of(queueName, vtSeconds, qty))
                .map(rows -> streamRows(rows).map(PGMQClientFunctions::rowToMessage).collect(Collectors.toList()));
    }

    /**
     * Reads messages from the specified queue grouped by message key with polling and poll settings.
     *
     * @param queueName the name of the queue to read from
     * @param vtSeconds the visibility timeout in seconds
     * @param qty the maximum number of messages to read
     * @param maxPollSeconds the maximum time to wait for messages
     * @param pollIntervalMs milliseconds between internal poll operations
     * @return a Uni that completes with a list of grouped messages
     */
    public Uni<List<PGMQMessage>> readGroupedWithPoll(String queueName, int vtSeconds, int qty, int maxPollSeconds,
            int pollIntervalMs) {
        return client
                .preparedQuery(
                        "SELECT * FROM pgmq.read_grouped_with_poll($1::text, $2::integer, $3::integer, $4::integer, $5::integer)")
                .execute(Tuple.of(queueName, vtSeconds, qty, maxPollSeconds, pollIntervalMs))
                .map(rows -> streamRows(rows).map(PGMQClientFunctions::rowToMessage).collect(Collectors.toList()));
    }

    /**
     * Reads messages from the specified queue in a grouped round-robin fashion.
     *
     * @param queueName the name of the queue to read from
     * @param vtSeconds the visibility timeout in seconds
     * @param qty the maximum number of messages to read
     * @return a Uni that completes with a list of messages
     */
    public Uni<List<PGMQMessage>> readGroupedRR(String queueName, int vtSeconds, int qty) {
        return client.preparedQuery("SELECT * FROM pgmq.read_grouped_rr($1::text, $2::integer, $3::integer)")
                .execute(Tuple.of(queueName, vtSeconds, qty))
                .map(rows -> streamRows(rows).map(PGMQClientFunctions::rowToMessage).toList());
    }

    /**
     * Reads messages from the specified queue in a grouped round-robin fashion with polling.
     *
     * @param queueName the name of the queue to read from
     * @param vtSeconds the visibility timeout in seconds
     * @param qty the maximum number of messages to read
     * @return a Uni that completes with a list of messages
     */
    public Uni<List<PGMQMessage>> readGroupedRRWithPoll(String queueName, int vtSeconds, int qty) {
        return client
                .preparedQuery("SELECT * FROM pgmq.read_grouped_rr_with_poll($1::text, $2::integer, $3::integer)")
                .execute(Tuple.of(queueName, vtSeconds, qty))
                .map(rows -> streamRows(rows).map(PGMQClientFunctions::rowToMessage).collect(Collectors.toList()));
    }

    /**
     * Reads messages from the specified queue in a grouped round-robin fashion with polling and poll settings.
     *
     * @param queueName the name of the queue to read from
     * @param vtSeconds the visibility timeout in seconds
     * @param qty the maximum number of messages to read
     * @param maxPollSeconds the maximum time to wait for messages
     * @param pollIntervalMs milliseconds between internal poll operations
     * @return a Uni that completes with a list of messages
     */
    public Uni<List<PGMQMessage>> readGroupedRRWithPoll(String queueName, int vtSeconds, int qty, int maxPollSeconds,
            int pollIntervalMs) {
        return client
                .preparedQuery(
                        "SELECT * FROM pgmq.read_grouped_rr_with_poll($1::text, $2::integer, $3::integer, $4::integer, $5::integer)")
                .execute(Tuple.of(queueName, vtSeconds, qty, maxPollSeconds, pollIntervalMs))
                .map(rows -> streamRows(rows).map(PGMQClientFunctions::rowToMessage).collect(Collectors.toList()));
    }

    // Pop — batch overload

    /**
     * Pops multiple messages from the specified queue.
     *
     * @param queueName the name of the queue to pop messages from
     * @param qty the number of messages to pop
     * @return a Uni that completes with a list of popped messages
     */
    public Uni<List<PGMQMessage>> pop(String queueName, int qty) {
        return client.preparedQuery("SELECT * FROM pgmq.pop($1::text, $2::integer)")
                .execute(Tuple.of(queueName, qty))
                .map(rows -> streamRows(rows).map(PGMQClientFunctions::rowToMessage).collect(Collectors.toList()));
    }

    // Visibility timeout — additional overloads

    /**
     * Sets the visibility timeout for a message using an absolute timestamp.
     *
     * @param queueName the name of the queue
     * @param msgId the ID of the message
     * @param visibleAt the timestamp at which the message should become visible
     * @return a Uni that completes with the updated message
     */
    public Uni<PGMQMessage> setVisibilityTimeout(String queueName, long msgId, OffsetDateTime visibleAt) {
        return client.preparedQuery("SELECT * FROM pgmq.set_vt($1::text, $2::bigint, $3::timestamptz)")
                .execute(Tuple.of(queueName, msgId, visibleAt))
                .map(rows -> rowToMessage(rows.iterator().next()));
    }

    /**
     * Sets the visibility timeout for a batch of messages using an offset in seconds.
     *
     * @param queueName the name of the queue
     * @param msgIds the list of message IDs
     * @param vtOffsetSeconds the visibility timeout offset in seconds
     * @return a Uni that completes with a list of updated messages
     */
    public Uni<List<PGMQMessage>> setVisibilityTimeout(String queueName, List<Long> msgIds, int vtOffsetSeconds) {
        return client.preparedQuery("SELECT * FROM pgmq.set_vt($1::text, $2::bigint[], $3::integer)")
                .execute(Tuple.of(queueName, msgIds.toArray(new Long[0]), vtOffsetSeconds))
                .map(rows -> streamRows(rows).map(PGMQClientFunctions::rowToMessage).collect(Collectors.toList()));
    }

    /**
     * Sets the visibility timeout for a batch of messages using an absolute timestamp.
     *
     * @param queueName the name of the queue
     * @param msgIds the list of message IDs
     * @param visibleAt the timestamp at which the messages should become visible
     * @return a Uni that completes with a list of updated messages
     */
    public Uni<List<PGMQMessage>> setVisibilityTimeout(String queueName, List<Long> msgIds,
            OffsetDateTime visibleAt) {
        return client.preparedQuery("SELECT * FROM pgmq.set_vt($1::text, $2::bigint[], $3::timestamptz)")
                .execute(Tuple.of(queueName, msgIds.toArray(new Long[0]), visibleAt))
                .map(rows -> streamRows(rows).map(PGMQClientFunctions::rowToMessage).collect(Collectors.toList()));
    }

    // Notify

    /**
     * Enables notify on insert for the specified queue with a throttle interval.
     *
     * @param queueName the name of the queue
     * @param throttleIntervalMs the throttle interval in milliseconds
     * @return a Uni that completes when notify is enabled
     */
    public Uni<Void> enableNotifyInsert(String queueName, int throttleIntervalMs) {
        return client.preparedQuery("SELECT pgmq.enable_notify_insert($1::text, $2::integer)")
                .execute(Tuple.of(queueName, throttleIntervalMs)).replaceWithVoid();
    }

    /**
     * Enables notify on insert for the specified queue with default throttle.
     *
     * @param queueName the name of the queue
     * @return a Uni that completes when notify is enabled
     */
    public Uni<Void> enableNotifyInsert(String queueName) {
        return client.preparedQuery("SELECT pgmq.enable_notify_insert($1::text)")
                .execute(Tuple.of(queueName)).replaceWithVoid();
    }

    /**
     * Disables notify on insert for the specified queue.
     *
     * @param queueName the name of the queue
     * @return a Uni that completes when notify is disabled
     */
    public Uni<Void> disableNotifyInsert(String queueName) {
        return client.preparedQuery("SELECT pgmq.disable_notify_insert($1::text)")
                .execute(Tuple.of(queueName)).replaceWithVoid();
    }

    /**
     * Updates the notify insert throttle interval for the specified queue.
     *
     * @param queueName the name of the queue
     * @param throttleIntervalMs the new throttle interval in milliseconds
     * @return a Uni that completes when the throttle is updated
     */
    public Uni<Void> updateNotifyInsert(String queueName, int throttleIntervalMs) {
        return client.preparedQuery("SELECT pgmq.update_notify_insert($1::text, $2::integer)")
                .execute(Tuple.of(queueName, throttleIntervalMs)).replaceWithVoid();
    }

    /**
     * Lists all notify insert throttle configurations.
     *
     * @return a Uni that completes with a list of notify throttle configurations
     */
    public Uni<List<PGMQNotifyThrottle>> listNotifyInsertThrottles() {
        return client.query("SELECT * FROM pgmq.list_notify_insert_throttles()")
                .execute()
                .map(rows -> streamRows(rows)
                        .map(PGMQClientFunctions::rowToNotifyThrottle)
                        .collect(Collectors.toList()));
    }

    // FIFO Index

    /**
     * Creates a FIFO index on the specified queue for ordered message consumption.
     *
     * @param queueName the name of the queue
     * @return a Uni that completes when the FIFO index is created
     */
    public Uni<Void> createFifoIndex(String queueName) {
        return client.preparedQuery("SELECT pgmq.create_fifo_index($1::text)")
                .execute(Tuple.of(queueName)).replaceWithVoid();
    }

    /**
     * Creates FIFO indexes on all existing queues.
     *
     * @return a Uni that completes when all FIFO indexes are created
     */
    public Uni<Void> createFifoIndexesAll() {
        return client.query("SELECT pgmq.create_fifo_indexes_all()")
                .execute().replaceWithVoid();
    }

}
