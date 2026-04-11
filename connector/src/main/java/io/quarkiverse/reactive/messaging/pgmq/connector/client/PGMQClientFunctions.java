package io.quarkiverse.reactive.messaging.pgmq.connector.client;

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
        return new PGMQMessage(
                row.getLong("msg_id"),
                row.getInteger("read_ct"),
                row.getOffsetDateTime("enqueued_at"),
                row.getOffsetDateTime("vt"),
                row.get(JsonObject.class, row.getColumnIndex("message")));
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
        return client.preparedQuery("SELECT pgmq.create($1)")
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
        return client.preparedQuery("SELECT pgmq.create_unlogged($1)")
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
        return client.preparedQuery("SELECT pgmq.create_partitioned($1, $2, $3)")
                .execute(Tuple.of(queueName, partitionInterval, retentionInterval)).replaceWithVoid();
    }

    /**
     * Creates a partitioned queue with the specified name and default retention.
     *
     * @param queueName the name of the partitioned queue to create
     * @return a Uni that completes when the partitioned queue is created
     */
    public Uni<Void> createPartitionedQueue(String queueName) {
        return client.preparedQuery("SELECT pgmq.create_partitioned($1)")
                .execute(Tuple.of(queueName)).replaceWithVoid();
    }

    /**
     * Drops the specified queue.
     *
     * @param queueName the name of the queue to drop
     * @return a Uni that completes when the queue is dropped
     */
    public Uni<Void> dropQueue(String queueName) {
        return client.preparedQuery("SELECT pgmq.drop_queue($1)")
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
        return client.preparedQuery("SELECT * FROM pgmq.send_batch($1, $2, $3::integer)")
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
        return client.preparedQuery("SELECT * FROM pgmq.read($1, $2, $3)")
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
        return client.preparedQuery("SELECT * FROM pgmq.read_with_poll($1, $2, $3)")
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
        return client.preparedQuery("SELECT * FROM pgmq.read_with_poll($1, $2, $3, $4, $5)")
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
        return client.preparedQuery("SELECT * FROM pgmq.pop($1)")
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
        return client.preparedQuery("SELECT * FROM pgmq.set_vt($1, $2, $3)")
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
        return client.preparedQuery("SELECT pgmq.purge_queue($1)")
                .execute(Tuple.of(queueName)).replaceWithVoid();
    }

    /**
     * Detaches the archive for the specified queue.
     *
     * @param queueName the name of the queue to detach the archive from
     * @return a Uni that completes when the archive is detached
     */
    public Uni<Void> detachArchive(String queueName) {
        return client.preparedQuery("SELECT pgmq.detach_archive($1)")
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
        return client.preparedQuery("SELECT * FROM pgmq.metrics($1)")
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

}
