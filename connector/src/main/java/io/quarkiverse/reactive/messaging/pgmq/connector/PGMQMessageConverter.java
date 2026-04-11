package io.quarkiverse.reactive.messaging.pgmq.connector;

import java.lang.reflect.Type;

import jakarta.enterprise.context.ApplicationScoped;

import org.eclipse.microprofile.reactive.messaging.Message;

import io.quarkiverse.reactive.messaging.pgmq.connector.client.PGMQMessage;
import io.smallrye.reactive.messaging.MessageConverter;
import io.smallrye.reactive.messaging.providers.helpers.TypeUtils;

@ApplicationScoped
public class PGMQMessageConverter implements MessageConverter {
    @Override
    public boolean canConvert(Message<?> in, Type target) {
        return TypeUtils.isAssignable(target, PGMQMessage.class)
                && in.getMetadata(PGMQIncomingMetadata.class).isPresent();
    }

    @Override
    public Message<?> convert(Message<?> in, Type target) {
        return in.withPayload(in.getMetadata(PGMQIncomingMetadata.class)
                .map(PGMQIncomingMetadata::getMessage)
                .orElse(null));
    }
}
