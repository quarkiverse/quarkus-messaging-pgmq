package io.quarkiverse.messaging.pgmq.connector;

import java.lang.reflect.Type;

import jakarta.enterprise.context.ApplicationScoped;

import org.eclipse.microprofile.reactive.messaging.Message;

import io.quarkiverse.messaging.pgmq.client.PGMQMessage;
import io.smallrye.reactive.messaging.MessageConverter;
import io.smallrye.reactive.messaging.providers.helpers.TypeUtils;
import io.vertx.core.json.JsonObject;

@ApplicationScoped
public class PGMQJsonPayloadConverter implements MessageConverter {

    @Override
    public int getPriority() {
        return CONVERTER_DEFAULT_PRIORITY + 100;
    }

    @Override
    public boolean canConvert(Message<?> in, Type target) {
        return target instanceof Class<?>
                && in.getMetadata(PGMQIncomingMetadata.class).isPresent()
                && in.getPayload() instanceof JsonObject
                && !TypeUtils.isAssignable(target, JsonObject.class)
                && !TypeUtils.isAssignable(target, PGMQMessage.class);
    }

    @Override
    public Message<?> convert(Message<?> in, Type target) {
        return in.withPayload(((JsonObject) in.getPayload()).mapTo((Class<?>) target));
    }
}
