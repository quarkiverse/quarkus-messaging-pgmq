package io.quarkiverse.messaging.pgmq.connector;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.Function;

import jakarta.enterprise.context.ApplicationScoped;

import org.eclipse.microprofile.reactive.messaging.Acknowledgment;
import org.eclipse.microprofile.reactive.messaging.Incoming;
import org.eclipse.microprofile.reactive.messaging.Message;
import org.eclipse.microprofile.reactive.messaging.Outgoing;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;

import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.Uni;
import io.smallrye.reactive.messaging.providers.locals.LocalContextMetadata;
import io.vertx.core.json.JsonObject;
import io.vertx.mutiny.core.Vertx;

public class LocalPropagationAckTest extends PGMQTestBase {

    @Override
    @BeforeEach
    void setUp(TestInfo testInfo) {
        // Use shorter queue names to stay within PGMQ's 47-character limit
        queueName = "t_" + Math.abs(UUID.randomUUID().getMostSignificantBits());
    }

    private MapBasedConfig dataconfig() {
        return new MapBasedConfig()
                .with("mp.messaging.incoming.data.connector", PGMQConnector.CONNECTOR_NAME)
                .with("mp.messaging.incoming.data.queue", queueName)
                .with("mp.messaging.incoming.data.tracing-enabled", false);
    }

    private void produceMessages(int count) {
        for (int i = 1; i <= count; i++) {
            pgmq.send(queueName, JsonObject.of("value", i)).await().indefinitely();
        }
    }

    @Test
    void channelWithAckOnMessageContext() {
        pgmq.createQueue(queueName).await().indefinitely();

        IncomingChannelWithAckOnMessageContext bean = runApplication(dataconfig(),
                IncomingChannelWithAckOnMessageContext.class);
        bean.setMapper(i -> i + 1);
        produceMessages(5);

        await().until(() -> bean.getResults().size() >= 5);
        assertThat(bean.getResults()).containsExactly(2, 3, 4, 5, 6);
    }

    @Test
    void channelWithNackOnMessageContextIgnore() {
        pgmq.createQueue(queueName).await().indefinitely();

        IncomingChannelWithAckOnMessageContext bean = runApplication(dataconfig()
                .with("mp.messaging.incoming.data.failure-type", "ignore"),
                IncomingChannelWithAckOnMessageContext.class);
        bean.setMapper(i -> {
            throw new RuntimeException("boom");
        });
        produceMessages(5);

        await().until(() -> bean.getResults().size() >= 5);
        assertThat(bean.getResults()).containsExactly(1, 2, 3, 4, 5);
    }

    @Test
    void channelWithNackOnMessageContextDelete() {
        pgmq.createQueue(queueName).await().indefinitely();

        IncomingChannelWithAckOnMessageContext bean = runApplication(dataconfig()
                .with("mp.messaging.incoming.data.failure-type", "delete"),
                IncomingChannelWithAckOnMessageContext.class);
        bean.setMapper(i -> {
            throw new RuntimeException("boom");
        });
        produceMessages(5);

        await().until(() -> bean.getResults().size() >= 5);
        assertThat(bean.getResults()).containsExactly(1, 2, 3, 4, 5);
    }

    @ApplicationScoped
    public static class IncomingChannelWithAckOnMessageContext {

        private final List<Integer> list = new CopyOnWriteArrayList<>();

        Function<Integer, Integer> mapper;

        public void setMapper(Function<Integer, Integer> mapper) {
            this.mapper = mapper;
        }

        @Incoming("data")
        @Outgoing("sink")
        public Multi<Message<JsonObject>> process(Multi<Message<JsonObject>> incoming) {
            return incoming.onItem()
                    .transformToUniAndConcatenate(msg -> Uni.createFrom()
                            .item(() -> msg.withPayload(
                                    JsonObject.of("value", mapper.apply(msg.getPayload().getInteger("value")))))
                            .chain(m -> Uni.createFrom().completionStage(m.ack()).replaceWith(m))
                            .onFailure().recoverWithUni(t -> Uni.createFrom().completionStage(msg.nack(t))
                                    .onItemOrFailure().transform((unused, throwable) -> msg)));
        }

        @Incoming("sink")
        @Acknowledgment(Acknowledgment.Strategy.NONE)
        CompletionStage<Void> sink(Message<JsonObject> msg) {
            msg.getMetadata(LocalContextMetadata.class).map(LocalContextMetadata::context).ifPresent(context -> {
                if (Vertx.currentContext().getDelegate() == context) {
                    list.add(msg.getPayload().getInteger("value"));
                }
            });
            return CompletableFuture.completedFuture(null);
        }

        List<Integer> getResults() {
            return list;
        }
    }
}
