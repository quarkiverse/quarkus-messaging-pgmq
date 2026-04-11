package io.quarkiverse.reactive.messaging.pgmq.connector.client;

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

public class PGMQClientTest extends PGMQTestBase {

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

        long msgId = pgmq.send(QUEUE_NAME, message, OffsetDateTime.now().plusSeconds(2L)).await().indefinitely();
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

        List<Long> msgIds = pgmq.sendBatch(QUEUE_NAME, batch, OffsetDateTime.now().plusSeconds(2L)).await().indefinitely();
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
}
