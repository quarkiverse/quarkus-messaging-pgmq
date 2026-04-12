package io.quarkiverse.messaging.pgmq.connector.tracing;

import java.util.Collections;
import java.util.List;

import io.opentelemetry.api.common.AttributesBuilder;
import io.opentelemetry.context.Context;
import io.opentelemetry.instrumentation.api.incubator.semconv.messaging.MessagingAttributesGetter;
import io.opentelemetry.instrumentation.api.instrumenter.AttributesExtractor;

public class PGMQAttributesExtractor implements AttributesExtractor<PGMQTrace, Void> {
    private final MessagingAttributesGetter<PGMQTrace, Void> messagingAttributesGetter;

    public PGMQAttributesExtractor() {
        this.messagingAttributesGetter = new PGMQMessagingAttributesGetter();
    }

    @Override
    public void onStart(final AttributesBuilder attributes, final Context parentContext, final PGMQTrace myTrace) {
        // fill in attributes from myTrace object
    }

    @Override
    public void onEnd(
            final AttributesBuilder attributes,
            final Context context,
            final PGMQTrace myTrace,
            final Void unused,
            final Throwable error) {

    }

    public MessagingAttributesGetter<PGMQTrace, Void> getMessagingAttributesGetter() {
        return messagingAttributesGetter;
    }

    private static final class PGMQMessagingAttributesGetter implements MessagingAttributesGetter<PGMQTrace, Void> {
        @Override
        public String getSystem(final PGMQTrace myTrace) {
            return "my";
        }

        @Override
        public String getDestination(final PGMQTrace myTrace) {
            return myTrace.getTopic();
        }

        @Override
        public boolean isTemporaryDestination(final PGMQTrace myTrace) {
            return false;
        }

        @Override
        public String getConversationId(final PGMQTrace myTrace) {
            return null;
        }

        @Override
        public String getMessageId(final PGMQTrace myTrace, final Void unused) {
            return null;
        }

        @Override
        public List<String> getMessageHeader(final PGMQTrace myTrace, final String name) {
            return Collections.emptyList();
        }

        @Override
        public String getDestinationTemplate(final PGMQTrace myTrace) {
            return null;
        }

        @Override
        public boolean isAnonymousDestination(final PGMQTrace myTrace) {
            return false;
        }

        @Override
        public Long getMessageBodySize(final PGMQTrace myTrace) {
            return null;
        }

        @Override
        public Long getMessageEnvelopeSize(final PGMQTrace myTrace) {
            return null;
        }

        @Override
        public String getClientId(final PGMQTrace myTrace) {
            return null;
        }

        @Override
        public Long getBatchMessageCount(final PGMQTrace myTrace, final Void unused) {
            return null;
        }
    }
}
