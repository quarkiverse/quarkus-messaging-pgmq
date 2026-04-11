package io.quarkiverse.reactive.messaging.pgmq.connector;

import java.util.concurrent.CompletionStage;
import java.util.function.Function;
import java.util.function.Supplier;

import org.eclipse.microprofile.reactive.messaging.Message;
import org.eclipse.microprofile.reactive.messaging.Metadata;

import io.smallrye.reactive.messaging.providers.locals.ContextAwareMessage;

public class PGMQOutgoingMessage<T> implements ContextAwareMessage<T> {

    private final T payload;
    private final Metadata metadata;

    private final Supplier<CompletionStage<Void>> ack;
    private final Function<Throwable, CompletionStage<Void>> nack;

    public static <T> PGMQOutgoingMessage<T> from(Message<T> message) {
        return new PGMQOutgoingMessage<>(message.getPayload(), message.getMetadata(), message.getAck(), message.getNack());
    }

    public PGMQOutgoingMessage(T payload, Metadata metadata,
            Supplier<CompletionStage<Void>> ack,
            Function<Throwable, CompletionStage<Void>> nack) {
        this.payload = payload;
        this.metadata = metadata;
        this.ack = ack;
        this.nack = nack;
    }

    public PGMQOutgoingMessage(T payload, String key, String topic,
            Supplier<CompletionStage<Void>> ack,
            Function<Throwable, CompletionStage<Void>> nack) {
        this(payload, Metadata.of(new PGMQOutgoingMetadata(topic, key)), ack, nack);
    }

    @Override
    public T getPayload() {
        return payload;
    }

    @Override
    public Metadata getMetadata() {
        return metadata;
    }

    @Override
    public Supplier<CompletionStage<Void>> getAck() {
        return this.ack;
    }

    @Override
    public Function<Throwable, CompletionStage<Void>> getNack() {
        return this.nack;
    }

    public PGMQOutgoingMessage<T> withKey(String key) {
        this.metadata.with(this.metadata.get(PGMQOutgoingMetadata.class)
                .map(m -> PGMQOutgoingMetadata.builder(m).withKey(key).build()));
        return this;
    }

    public PGMQOutgoingMessage<T> withTopic(String topic) {
        this.metadata.with(this.metadata.get(PGMQOutgoingMetadata.class)
                .map(m -> PGMQOutgoingMetadata.builder(m).withTopic(topic).build()));
        return this;
    }
}
