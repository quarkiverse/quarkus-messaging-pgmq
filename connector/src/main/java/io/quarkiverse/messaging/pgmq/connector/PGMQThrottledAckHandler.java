package io.quarkiverse.messaging.pgmq.connector;

import java.util.ArrayList;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BiFunction;

import org.eclipse.microprofile.reactive.messaging.Metadata;

import io.quarkiverse.messaging.pgmq.client.PGMQClient;
import io.quarkiverse.messaging.pgmq.connector.i18n.PGMQLogging;
import io.smallrye.mutiny.Uni;
import io.smallrye.mutiny.helpers.queues.Queues;
import io.vertx.mutiny.core.Context;
import io.vertx.mutiny.core.Vertx;

/**
 * Throttled batch ack handler that queues DELETE and ARCHIVE ack IDs
 * and flushes them as batch SQL calls, either when a size threshold is
 * reached or on a periodic timer.
 * <p>
 * VISIBILITY and IGNORE strategies are delegated to the parent handler immediately.
 */
public class PGMQThrottledAckHandler extends PGMQAckHandler {

    private final int batchSize;
    private final Vertx vertx;
    private final Context context;
    private final AtomicBoolean closed = new AtomicBoolean(false);

    private final AckQueue deleteQueue;
    private final AckQueue archiveQueue;
    private volatile long timerId = -1;

    PGMQThrottledAckHandler(PGMQConnectorIncomingConfiguration cfg, PGMQClient client, Vertx vertx, Context context) {
        super(cfg, client);
        this.batchSize = cfg.getAckBatchSize();
        this.vertx = vertx;
        this.context = context;
        this.deleteQueue = new AckQueue(client::delete);
        this.archiveQueue = new AckQueue(client::archive);
        scheduleTimer(cfg.getAckBatchDelay());
    }

    @Override
    public CompletionStage<Void> handle(IncomingPGMQMessage msg, Metadata metadata,
            PgmqAcknowledgement.Strategy strategy, int visibilityTimeout) {
        return switch (strategy) {
            case DELETE -> deleteQueue.offer(msg.getMessage().msgId());
            case ARCHIVE -> archiveQueue.offer(msg.getMessage().msgId());
            case VISIBILITY, IGNORE -> super.handle(msg, metadata, strategy, visibilityTimeout);
        };
    }

    private void scheduleTimer(long batchDelayMs) {
        if (closed.get()) {
            return;
        }
        timerId = vertx.setTimer(batchDelayMs, id -> flush().subscribe().with(
                v -> scheduleTimer(batchDelayMs),
                t -> scheduleTimer(batchDelayMs)));
    }

    private Uni<Void> flush() {
        return Uni.combine().all()
                .unis(deleteQueue.flush(), archiveQueue.flush())
                .collectFailures()
                .discardItems();
    }

    @Override
    public void close() {
        if (!closed.compareAndSet(false, true)) {
            return;
        }
        if (timerId != -1) {
            vertx.cancelTimer(timerId);
        }
        int remaining = deleteQueue.size() + archiveQueue.size();
        PGMQLogging.log.throttledAckHandlerClosing(queueName, remaining);
        flush().await().indefinitely();
    }

    private class AckQueue {
        private final Queue<Long> queue = Queues.createMpscQueue();
        private final BiFunction<String, List<Long>, Uni<List<Long>>> operation;

        AckQueue(BiFunction<String, List<Long>, Uni<List<Long>>> operation) {
            this.operation = operation;
        }

        public int size() {
            return queue.size();
        }

        public CompletionStage<Void> offer(long msgId) {
            queue.offer(msgId);
            if (queue.size() >= batchSize) {
                flush().subscribeAsCompletionStage();
            }
            return CompletableFuture.completedFuture(null);
        }

        public Uni<Void> flush() {
            return Uni.createFrom().deferred(() -> {
                List<Long> ids = drain();
                if (ids.isEmpty()) {
                    return Uni.createFrom().voidItem();
                }
                PGMQLogging.log.flushingBatchAck(queueName, ids.size());
                return operation.apply(queueName, ids)
                        .onFailure().invoke(t -> queue.addAll(ids))
                        .onFailure().invoke(t -> PGMQLogging.log.batchAckFlushFailed(queueName, t))
                        .replaceWithVoid();
            }).runSubscriptionOn(context::runOnContext);
        }

        private List<Long> drain() {
            List<Long> ids = new ArrayList<>();
            Long id;
            while (ids.size() < batchSize && (id = queue.poll()) != null) {
                ids.add(id);
            }
            return ids;
        }
    }
}
