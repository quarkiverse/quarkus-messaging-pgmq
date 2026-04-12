package io.quarkiverse.messaging.pgmq.client;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import java.time.OffsetDateTime;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import io.vertx.core.json.JsonObject;
import io.vertx.mutiny.sqlclient.Pool;
import io.vertx.mutiny.sqlclient.RowSet;

public class PGMQClientTest extends PGMQClientTestBase {

    Pool client;
    PGMQClient pgmq;
    final String QUEUE_NAME = "test_queue";

    @BeforeEach
    void createQueue() {
        pgmq = getClient();
        client = pgmq.getClient();
        pgmq.createQueue(QUEUE_NAME).await().indefinitely();
    }

    @AfterEach
    void dropQueue() {
        pgmq.dropQueue(QUEUE_NAME).await().indefinitely();
    }

    @Test
    void testSendAndRead() {
        JsonObject message = new JsonObject().put("hello", "world");

        long msgId = pgmq.send(QUEUE_NAME, message).await().indefinitely();
        List<PGMQMessage> consumed = pgmq.read(QUEUE_NAME, 60, 3).await().indefinitely();
        assertThat(consumed).hasSize(1).first().satisfies(msg -> {
            assertThat(msg).isNotNull();
            assertThat(msg.msgId()).isEqualTo(msgId);
            assertThat(msg.message().getString("hello")).isEqualTo("world");
        });
    }

    @Test
    void testSendDelayAndReadWithPoll() {
        JsonObject message = new JsonObject().put("hello", "world");

        long msgId = pgmq.send(QUEUE_NAME, message, 2).await().indefinitely();
        List<PGMQMessage> consumed = pgmq.readWithPoll(QUEUE_NAME, 60, 3, 5, 100).await().indefinitely();
        assertThat(consumed).hasSize(1).first().satisfies(msg -> {
            assertThat(msg).isNotNull();
            assertThat(msg.msgId()).isEqualTo(msgId);
            assertThat(msg.message().getString("hello")).isEqualTo("world");
        });
    }

    @Test
    void testSendTimestampAndReadWithPoll() {
        JsonObject message = new JsonObject().put("hello", "world");

        OffsetDateTime serverNow = serverNow();
        long msgId = pgmq.send(QUEUE_NAME, message, serverNow.plusSeconds(2L)).await().indefinitely();
        List<PGMQMessage> consumed = pgmq.readWithPoll(QUEUE_NAME, 60, 3, 5, 100).await().indefinitely();
        assertThat(consumed).hasSize(1).first().satisfies(msg -> {
            assertThat(msg).isNotNull();
            assertThat(msg.msgId()).isEqualTo(msgId);
            assertThat(msg.message().getString("hello")).isEqualTo("world");
        });
    }

    @Test
    void testSendBatchDelayAndReadWithPoll() {
        List<JsonObject> batch = Arrays.asList(
                new JsonObject().put("batch", "one"),
                new JsonObject().put("batch", "two"),
                new JsonObject().put("batch", "three"));

        List<Long> msgIds = pgmq.sendBatch(QUEUE_NAME, batch, 2).await().indefinitely();
        List<PGMQMessage> messages = pgmq.readWithPoll(QUEUE_NAME, 1, 3, 5, 100).await().indefinitely();
        assertThat(messages).hasSize(3)
                .anyMatch(msg -> msg.message().getString("batch").equals("one"))
                .anyMatch(msg -> msg.message().getString("batch").equals("two"))
                .anyMatch(msg -> msg.message().getString("batch").equals("three"))
                .extracting(PGMQMessage::msgId)
                .containsExactlyInAnyOrderElementsOf(msgIds);
    }

    @Test
    void testSendBatchTimestampAndReadWithPoll() {
        List<JsonObject> batch = Arrays.asList(
                new JsonObject().put("batch", "one"),
                new JsonObject().put("batch", "two"),
                new JsonObject().put("batch", "three"));

        OffsetDateTime serverNow = serverNow();
        List<Long> msgIds = pgmq.sendBatch(QUEUE_NAME, batch, serverNow.plusSeconds(2L)).await().indefinitely();
        List<PGMQMessage> messages = pgmq.readWithPoll(QUEUE_NAME, 1, 3, 5, 100).await().indefinitely();
        assertThat(messages).hasSize(3)
                .anyMatch(msg -> msg.message().getString("batch").equals("one"))
                .anyMatch(msg -> msg.message().getString("batch").equals("two"))
                .anyMatch(msg -> msg.message().getString("batch").equals("three"))
                .extracting(PGMQMessage::msgId)
                .containsExactlyInAnyOrderElementsOf(msgIds);
    }

    @Test
    void testDelete() {
        JsonObject message = new JsonObject().put("task", "delete_me");

        long msgId = pgmq.send(QUEUE_NAME, message).await().indefinitely();
        List<PGMQMessage> consumed = pgmq.read(QUEUE_NAME, 60, 1).await().indefinitely();
        assertThat(consumed).hasSize(1);

        boolean deleted = pgmq.delete(QUEUE_NAME, consumed.get(0).msgId()).await().indefinitely();
        assertThat(deleted).isTrue();
    }

    @Test
    void testArchiveAndMetrics() {
        JsonObject message = new JsonObject().put("type", "archivable");

        long msgId = pgmq.send(QUEUE_NAME, message).await().indefinitely();

        boolean archived = pgmq.archive(QUEUE_NAME, msgId).await().indefinitely();
        assertThat(archived).isTrue();
        PGMQMetrics metrics = pgmq.getQueueMetrics(QUEUE_NAME).await().indefinitely();
        assertThat(metrics).isNotNull();
        assertThat(metrics.totalMessages()).isGreaterThan(0);
    }

    @Test
    void testAllMetrics() {
        JsonObject message = new JsonObject().put("type", "archivable");

        long msgId = pgmq.send(QUEUE_NAME, message).await().indefinitely();

        boolean archived = pgmq.archive(QUEUE_NAME, msgId).await().indefinitely();
        pgmq.send(QUEUE_NAME, message).await().indefinitely();
        pgmq.send(QUEUE_NAME, message).await().indefinitely();
        assertThat(archived).isTrue();
        List<PGMQMetrics> metrics = pgmq.getAllQueueMetrics().await().indefinitely();
        System.out.println(metrics);
        PGMQMetrics pgmqMetrics = metrics.get(0);
        assertThat(pgmqMetrics.queueLength()).isEqualTo(2L);
        assertThat(pgmqMetrics.totalMessages()).isEqualTo(3L);
    }

    @Test
    void testReadWithPoll() {
        JsonObject message1 = new JsonObject().put("key", "value1");
        JsonObject message2 = new JsonObject().put("key", "value2");
        JsonObject message3 = new JsonObject().put("key", "value3");

        List<PGMQMessage> consumed = new CopyOnWriteArrayList<>();
        pgmq.readWithPoll(QUEUE_NAME, 60, 3, 10, 100).subscribe().with(consumed::addAll);
        long msgId = pgmq.send(QUEUE_NAME, message1).await().indefinitely();
        long msg2Id = pgmq.send(QUEUE_NAME, message2).await().indefinitely();
        long msg3Id = pgmq.send(QUEUE_NAME, message3).await().indefinitely();

        await().untilAsserted(() -> assertThat(consumed).hasSize(3)
                .anySatisfy(msg -> {
                    assertThat(msg.msgId()).isEqualTo(msgId);
                    assertThat(msg.message().getString("key")).isEqualTo("value1");
                })
                .anySatisfy(msg -> {
                    assertThat(msg.msgId()).isEqualTo(msg2Id);
                    assertThat(msg.message().getString("key")).isEqualTo("value2");
                })
                .anySatisfy(msg -> {
                    assertThat(msg.msgId()).isEqualTo(msg3Id);
                    assertThat(msg.message().getString("key")).isEqualTo("value3");
                }));
    }

    @Test
    void testPop() {
        JsonObject message = new JsonObject().put("pop", "now");
        JsonObject message2 = new JsonObject().put("pop", "now2");

        long msgId = pgmq.send(QUEUE_NAME, message).await().indefinitely();
        long msgId2 = pgmq.send(QUEUE_NAME, message2).await().indefinitely();

        Optional<PGMQMessage> msgMaybe = pgmq.pop(QUEUE_NAME).await().indefinitely();
        assertThat(msgMaybe).isPresent()
                .satisfies(msg -> assertThat(msg.get().message().getString("pop")).isEqualTo("now"));
        Optional<PGMQMessage> msg2Maybe = pgmq.pop(QUEUE_NAME).await().indefinitely();
        assertThat(msg2Maybe).isPresent()
                .satisfies(msg -> assertThat(msg.get().message().getString("pop")).isEqualTo("now2"));
    }

    @Test
    void testPurge() {
        JsonObject message = new JsonObject().put("purge", "this");

        long msgId = pgmq.send(QUEUE_NAME, message).await().indefinitely();
        pgmq.purgeQueue(QUEUE_NAME).await().indefinitely();
        Optional<PGMQMessage> maybe = pgmq.pop(QUEUE_NAME).await().indefinitely();
        assertThat(maybe).isEmpty();
    }

    @Test
    void testCreateAndDropPartitionedQueueDefault() {
        String partitionedQueue = "partitioned_test_queue";
        pgmq.createPartitionedQueue(partitionedQueue).await().indefinitely();
        pgmq.dropQueue(partitionedQueue).await().indefinitely();
    }

    @Test
    void testCreateAndDropPartitionedQueue() {
        String partitionedQueue = "partitioned_test_queue";
        pgmq.createPartitionedQueue(partitionedQueue, "100000", "10000000").await().indefinitely();
        pgmq.dropQueue(partitionedQueue).await().indefinitely();
    }

    @Test
    void testCreateUnloggedQueue() {
        String unloggedQueue = "unlogged_test_queue";
        pgmq.createUnloggedQueue(unloggedQueue).await().indefinitely();
        pgmq.dropQueue(unloggedQueue).await().indefinitely();
    }

    @Test
    void testSendBatchDelete() {
        List<JsonObject> batch = Arrays.asList(
                new JsonObject().put("batch", "one"),
                new JsonObject().put("batch", "two"),
                new JsonObject().put("batch", "three"));

        List<Long> msgIds = pgmq.sendBatch(QUEUE_NAME, batch).await().indefinitely();
        List<PGMQMessage> messages = pgmq.readWithPoll(QUEUE_NAME, 1, 3).await().indefinitely();
        assertThat(messages).hasSize(3)
                .anyMatch(msg -> msg.message().getString("batch").equals("one"))
                .anyMatch(msg -> msg.message().getString("batch").equals("two"))
                .anyMatch(msg -> msg.message().getString("batch").equals("three"))
                .extracting(PGMQMessage::msgId)
                .containsExactlyInAnyOrderElementsOf(msgIds);

        List<Long> deleted = pgmq.delete(QUEUE_NAME, msgIds).await().indefinitely();
        assertThat(deleted).containsExactlyInAnyOrderElementsOf(msgIds);

        await().pollDelay(3, TimeUnit.SECONDS)
                .untilAsserted(() -> assertThat(pgmq.read(QUEUE_NAME, 1, 3).await().indefinitely()).isEmpty());
    }

    @Test
    void testSendBatchArchive() {
        List<JsonObject> batch = Arrays.asList(
                new JsonObject().put("batch", "one"),
                new JsonObject().put("batch", "two"),
                new JsonObject().put("batch", "three"));

        List<Long> msgIds = pgmq.sendBatch(QUEUE_NAME, batch).await().indefinitely();
        List<PGMQMessage> messages = pgmq.readWithPoll(QUEUE_NAME, 1, 3).await().indefinitely();
        assertThat(messages).hasSize(3)
                .anyMatch(msg -> msg.message().getString("batch").equals("one"))
                .anyMatch(msg -> msg.message().getString("batch").equals("two"))
                .anyMatch(msg -> msg.message().getString("batch").equals("three"))
                .extracting(PGMQMessage::msgId)
                .containsExactlyInAnyOrderElementsOf(msgIds);

        List<Long> archived = pgmq.archive(QUEUE_NAME, msgIds).await().indefinitely();
        assertThat(archived).containsExactlyInAnyOrderElementsOf(msgIds);

        await().pollDelay(3, TimeUnit.SECONDS)
                .untilAsserted(() -> assertThat(pgmq.read(QUEUE_NAME, 1, 3).await().indefinitely()).isEmpty());

        //        pgmq.detachArchive(QUEUE_NAME).await().indefinitely();

        List<PGMQMessage> archive = client.query("SELECT * FROM pgmq.a_" + QUEUE_NAME).execute()
                .onItem().transformToMulti(RowSet::toMulti)
                .map(PGMQClient::rowToMessage)
                .collect().asList().await().indefinitely();
        assertThat(archive).hasSize(3)
                .allSatisfy(msg -> assertThat(msg.message().getString("batch")).isIn("one", "two", "three"));
    }

    @Test
    void testSetVisibilityTimeout() {
        JsonObject message = new JsonObject().put("job", "delayed_ack");

        long msgId = pgmq.send(QUEUE_NAME, message).await().indefinitely();
        List<PGMQMessage> msgs = pgmq.read(QUEUE_NAME, 5, 1).await().indefinitely();
        assertThat(msgs).hasSize(1);
        OffsetDateTime vt = msgs.get(0).visibilityTimeout();
        PGMQMessage msg = pgmq.setVisibilityTimeout(QUEUE_NAME, msgId, 60).await().indefinitely();
        assertThat(msg.visibilityTimeout()).isAfter(vt);
    }

    @Test
    void testListQueues() {
        pgmq.createPartitionedQueue("partitioned_test_queue").await().indefinitely();
        pgmq.createUnloggedQueue("unlogged_test_queue").await().indefinitely();

        List<PGMQQueue> queues = pgmq.listQueues().await().indefinitely();

        assertThat(queues).hasSize(3)
                .anySatisfy(q -> {
                    assertThat(q.name()).isEqualTo(QUEUE_NAME);
                    assertThat(q.partitioned()).isFalse();
                    assertThat(q.unlogged()).isFalse();
                })
                .anySatisfy(q -> {
                    assertThat(q.name()).isEqualTo("partitioned_test_queue");
                    assertThat(q.partitioned()).isTrue();
                    assertThat(q.unlogged()).isFalse();
                })
                .anySatisfy(q -> {
                    assertThat(q.name()).isEqualTo("unlogged_test_queue");
                    assertThat(q.partitioned()).isFalse();
                    assertThat(q.unlogged()).isTrue();
                });

        pgmq.dropQueue("unlogged_test_queue").await().indefinitely();
        pgmq.dropQueue("partitioned_test_queue").await().indefinitely();
    }

    // --- Headers ---

    @Test
    void testSendWithHeaders() {
        JsonObject message = new JsonObject().put("hello", "world");
        JsonObject headers = new JsonObject().put("x-source", "test");

        long msgId = pgmq.send(QUEUE_NAME, message, headers).await().indefinitely();
        List<PGMQMessage> consumed = pgmq.read(QUEUE_NAME, 60, 1).await().indefinitely();
        assertThat(consumed).hasSize(1).first().satisfies(msg -> {
            assertThat(msg.msgId()).isEqualTo(msgId);
            assertThat(msg.message().getString("hello")).isEqualTo("world");
            assertThat(msg.headers()).isNotNull();
            assertThat(msg.headers().getString("x-source")).isEqualTo("test");
        });
    }

    @Test
    void testSendWithoutHeadersReturnsNullHeaders() {
        JsonObject message = new JsonObject().put("hello", "world");

        pgmq.send(QUEUE_NAME, message).await().indefinitely();
        List<PGMQMessage> consumed = pgmq.read(QUEUE_NAME, 60, 1).await().indefinitely();
        assertThat(consumed).hasSize(1).first().satisfies(msg -> {
            assertThat(msg.headers()).isNull();
        });
    }

    @Test
    void testSendBatchWithHeaders() {
        List<JsonObject> messages = Arrays.asList(
                new JsonObject().put("batch", "one"),
                new JsonObject().put("batch", "two"));
        List<JsonObject> headers = Arrays.asList(
                new JsonObject().put("x-index", "0"),
                new JsonObject().put("x-index", "1"));

        List<Long> msgIds = pgmq.sendBatch(QUEUE_NAME, messages, headers).await().indefinitely();
        assertThat(msgIds).hasSize(2);
        List<PGMQMessage> consumed = pgmq.read(QUEUE_NAME, 60, 2).await().indefinitely();
        assertThat(consumed).hasSize(2)
                .anySatisfy(msg -> {
                    assertThat(msg.message().getString("batch")).isEqualTo("one");
                    assertThat(msg.headers().getString("x-index")).isEqualTo("0");
                })
                .anySatisfy(msg -> {
                    assertThat(msg.message().getString("batch")).isEqualTo("two");
                    assertThat(msg.headers().getString("x-index")).isEqualTo("1");
                });
    }

    // --- Pop with qty ---

    @Test
    void testPopWithQty() {
        pgmq.send(QUEUE_NAME, new JsonObject().put("pop", "1")).await().indefinitely();
        pgmq.send(QUEUE_NAME, new JsonObject().put("pop", "2")).await().indefinitely();
        pgmq.send(QUEUE_NAME, new JsonObject().put("pop", "3")).await().indefinitely();

        List<PGMQMessage> popped = pgmq.pop(QUEUE_NAME, 2).await().indefinitely();
        assertThat(popped).hasSize(2);

        // One message should remain
        Optional<PGMQMessage> remaining = pgmq.pop(QUEUE_NAME).await().indefinitely();
        assertThat(remaining).isPresent();
    }

    // --- Set Visibility Timeout — additional overloads ---

    @Test
    void testSetVisibilityTimeoutBatch() {
        long msgId1 = pgmq.send(QUEUE_NAME, new JsonObject().put("key", "v1")).await().indefinitely();
        long msgId2 = pgmq.send(QUEUE_NAME, new JsonObject().put("key", "v2")).await().indefinitely();

        List<PGMQMessage> msgs = pgmq.read(QUEUE_NAME, 5, 2).await().indefinitely();
        assertThat(msgs).hasSize(2);
        OffsetDateTime originalVt = msgs.get(0).visibilityTimeout();

        List<PGMQMessage> updated = pgmq.setVisibilityTimeout(QUEUE_NAME, List.of(msgId1, msgId2), 120).await()
                .indefinitely();
        assertThat(updated).hasSize(2);
        assertThat(updated).allSatisfy(msg -> assertThat(msg.visibilityTimeout()).isAfter(originalVt));
    }

    @Test
    void testSetVisibilityTimeoutTimestamp() {
        long msgId = pgmq.send(QUEUE_NAME, new JsonObject().put("key", "v1")).await().indefinitely();
        List<PGMQMessage> msgs = pgmq.read(QUEUE_NAME, 5, 1).await().indefinitely();
        assertThat(msgs).hasSize(1);

        OffsetDateTime futureTime = OffsetDateTime.now().plusHours(1);
        PGMQMessage updated = pgmq.setVisibilityTimeout(QUEUE_NAME, msgId, futureTime).await().indefinitely();
        assertThat(updated.visibilityTimeout()).isAfterOrEqualTo(futureTime.minusSeconds(1));
    }

    // --- Create Non-Partitioned ---

    @Test
    void testCreateNonPartitioned() {
        String queue = "non_partitioned_test";
        pgmq.createNonPartitioned(queue).await().indefinitely();

        List<PGMQQueue> queues = pgmq.listQueues().await().indefinitely();
        assertThat(queues).anySatisfy(q -> {
            assertThat(q.name()).isEqualTo(queue);
            assertThat(q.partitioned()).isFalse();
        });

        pgmq.dropQueue(queue).await().indefinitely();
    }

    // --- Read with conditional ---

    @Test
    void testReadWithConditional() {
        pgmq.send(QUEUE_NAME, new JsonObject().put("type", "a").put("value", 1)).await().indefinitely();
        pgmq.send(QUEUE_NAME, new JsonObject().put("type", "b").put("value", 2)).await().indefinitely();
        pgmq.send(QUEUE_NAME, new JsonObject().put("type", "a").put("value", 3)).await().indefinitely();

        String conditional = "{\"type\": \"a\"}";
        List<PGMQMessage> filtered = pgmq.read(QUEUE_NAME, 60, 10, conditional).await().indefinitely();
        assertThat(filtered).hasSize(2)
                .allSatisfy(msg -> assertThat(msg.message().getString("type")).isEqualTo("a"));
    }

    @Test
    void testReadWithPollConditional() {
        pgmq.send(QUEUE_NAME, new JsonObject().put("type", "x")).await().indefinitely();
        pgmq.send(QUEUE_NAME, new JsonObject().put("type", "y")).await().indefinitely();

        String conditional = "{\"type\": \"y\"}";
        List<PGMQMessage> filtered = pgmq.readWithPoll(QUEUE_NAME, 60, 10, 5, 100, conditional).await().indefinitely();
        assertThat(filtered).hasSize(1)
                .first().satisfies(msg -> assertThat(msg.message().getString("type")).isEqualTo("y"));
    }

    // --- Grouped reads ---

    @Test
    void testReadGrouped() {
        pgmq.send(QUEUE_NAME, new JsonObject().put("order", 1),
                new JsonObject().put("x-pgmq-group", "group-a")).await().indefinitely();
        pgmq.send(QUEUE_NAME, new JsonObject().put("order", 2),
                new JsonObject().put("x-pgmq-group", "group-a")).await().indefinitely();
        pgmq.send(QUEUE_NAME, new JsonObject().put("order", 3),
                new JsonObject().put("x-pgmq-group", "group-b")).await().indefinitely();

        pgmq.createFifoIndex(QUEUE_NAME).await().indefinitely();

        List<PGMQMessage> grouped = pgmq.readGrouped(QUEUE_NAME, 60, 10).await().indefinitely();
        assertThat(grouped).isNotEmpty();
    }

    @Test
    void testReadGroupedWithPoll() {
        pgmq.send(QUEUE_NAME, new JsonObject().put("order", 1),
                new JsonObject().put("x-pgmq-group", "group-a")).await().indefinitely();
        pgmq.send(QUEUE_NAME, new JsonObject().put("order", 2),
                new JsonObject().put("x-pgmq-group", "group-b")).await().indefinitely();

        pgmq.createFifoIndex(QUEUE_NAME).await().indefinitely();

        List<PGMQMessage> grouped = pgmq.readGroupedWithPoll(QUEUE_NAME, 60, 10, 2, 100).await().indefinitely();
        assertThat(grouped).isNotEmpty();
    }

    @Test
    void testReadGroupedRR() {
        pgmq.send(QUEUE_NAME, new JsonObject().put("order", 1),
                new JsonObject().put("x-pgmq-group", "group-a")).await().indefinitely();
        pgmq.send(QUEUE_NAME, new JsonObject().put("order", 2),
                new JsonObject().put("x-pgmq-group", "group-a")).await().indefinitely();
        pgmq.send(QUEUE_NAME, new JsonObject().put("order", 3),
                new JsonObject().put("x-pgmq-group", "group-b")).await().indefinitely();

        pgmq.createFifoIndex(QUEUE_NAME).await().indefinitely();

        List<PGMQMessage> grouped = pgmq.readGroupedRR(QUEUE_NAME, 60, 10).await().indefinitely();
        assertThat(grouped).isNotEmpty();
    }

    @Test
    void testReadGroupedRRWithPoll() {
        pgmq.send(QUEUE_NAME, new JsonObject().put("order", 1),
                new JsonObject().put("x-pgmq-group", "group-a")).await().indefinitely();
        pgmq.send(QUEUE_NAME, new JsonObject().put("order", 2),
                new JsonObject().put("x-pgmq-group", "group-b")).await().indefinitely();

        pgmq.createFifoIndex(QUEUE_NAME).await().indefinitely();

        List<PGMQMessage> grouped = pgmq.readGroupedRRWithPoll(QUEUE_NAME, 60, 10, 2, 100).await().indefinitely();
        assertThat(grouped).isNotEmpty();
    }

    // --- FIFO Index ---

    @Test
    void testCreateFifoIndex() {
        pgmq.createFifoIndex(QUEUE_NAME).await().indefinitely();
        var rows = client.query(
                "SELECT indexname FROM pg_indexes WHERE tablename = 'q_" + QUEUE_NAME
                        + "' AND indexname LIKE '%fifo%'")
                .execute().await().indefinitely();
        assertThat(rows.size()).isGreaterThan(0);
    }

    @Test
    void testCreateFifoIndexesAll() {
        pgmq.createFifoIndexesAll().await().indefinitely();
    }

    // --- Enable/Disable Notify Insert ---

    @Test
    void testEnableAndDisableNotifyInsert() {
        pgmq.enableNotifyInsert(QUEUE_NAME).await().indefinitely();
        pgmq.disableNotifyInsert(QUEUE_NAME).await().indefinitely();
    }

    @Test
    void testEnableNotifyInsertWithThrottle() {
        pgmq.enableNotifyInsert(QUEUE_NAME, 500).await().indefinitely();
        await().untilAsserted(() -> {
            assertThat(pgmq.listNotifyInsertThrottles().await().indefinitely())
                    .anySatisfy(t -> {
                        assertThat(t.queueName()).isEqualTo(QUEUE_NAME);
                        assertThat(t.throttleIntervalMs()).isEqualTo(500);
                    });
        });
        pgmq.disableNotifyInsert(QUEUE_NAME).await().indefinitely();
    }

    // --- Topic-Based Routing ---

    @Test
    void testBindAndUnbindTopic() {
        pgmq.bindTopic("orders.*", QUEUE_NAME).await().indefinitely();

        List<PGMQRoutingMatch> matches = pgmq.testRouting("orders.created").await().indefinitely();
        assertThat(matches).anySatisfy(m -> {
            assertThat(m.queueName()).isEqualTo(QUEUE_NAME);
        });

        pgmq.unbindTopic("orders.*", QUEUE_NAME).await().indefinitely();

        List<PGMQRoutingMatch> afterUnbind = pgmq.testRouting("orders.created").await().indefinitely();
        assertThat(afterUnbind).noneSatisfy(m -> assertThat(m.queueName()).isEqualTo(QUEUE_NAME));
    }

    @Test
    void testSendTopic() {
        pgmq.bindTopic("events.*", QUEUE_NAME).await().indefinitely();

        JsonObject message = new JsonObject().put("event", "test");
        int matchedQueues = pgmq.sendTopic("events.created", message).await().indefinitely();
        assertThat(matchedQueues).isGreaterThan(0);

        List<PGMQMessage> consumed = pgmq.read(QUEUE_NAME, 60, 1).await().indefinitely();
        assertThat(consumed).hasSize(1)
                .first().satisfies(msg -> assertThat(msg.message().getString("event")).isEqualTo("test"));

        pgmq.unbindTopic("events.*", QUEUE_NAME).await().indefinitely();
    }

    @Test
    void testSendTopicWithHeaders() {
        pgmq.bindTopic("notify.*", QUEUE_NAME).await().indefinitely();

        JsonObject message = new JsonObject().put("data", "value");
        JsonObject headers = new JsonObject().put("x-source", "topic-test");
        int matchedQueues = pgmq.sendTopic("notify.alert", message, headers, 0).await().indefinitely();
        assertThat(matchedQueues).isGreaterThan(0);

        List<PGMQMessage> consumed = pgmq.read(QUEUE_NAME, 60, 1).await().indefinitely();
        assertThat(consumed).hasSize(1).first().satisfies(msg -> {
            assertThat(msg.message().getString("data")).isEqualTo("value");
            assertThat(msg.headers().getString("x-source")).isEqualTo("topic-test");
        });

        pgmq.unbindTopic("notify.*", QUEUE_NAME).await().indefinitely();
    }

    @Test
    void testSendBatchTopic() {
        pgmq.bindTopic("batch.*", QUEUE_NAME).await().indefinitely();

        List<JsonObject> messages = Arrays.asList(
                new JsonObject().put("item", "one"),
                new JsonObject().put("item", "two"));

        List<PGMQTopicResult> results = pgmq.sendBatchTopic("batch.process", messages).await().indefinitely();
        assertThat(results).hasSize(2);

        List<PGMQMessage> consumed = pgmq.read(QUEUE_NAME, 60, 10).await().indefinitely();
        assertThat(consumed).hasSize(2)
                .anySatisfy(msg -> assertThat(msg.message().getString("item")).isEqualTo("one"))
                .anySatisfy(msg -> assertThat(msg.message().getString("item")).isEqualTo("two"));

        pgmq.unbindTopic("batch.*", QUEUE_NAME).await().indefinitely();
    }

    @Test
    void testTestRouting() {
        pgmq.bindTopic("user.*", QUEUE_NAME).await().indefinitely();

        List<PGMQRoutingMatch> matches = pgmq.testRouting("user.signup").await().indefinitely();
        assertThat(matches).isNotEmpty().anySatisfy(m -> {
            assertThat(m.pattern()).isEqualTo("user.*");
            assertThat(m.queueName()).isEqualTo(QUEUE_NAME);
            assertThat(m.compiledRegex()).isNotNull();
        });

        pgmq.unbindTopic("user.*", QUEUE_NAME).await().indefinitely();
    }
}
