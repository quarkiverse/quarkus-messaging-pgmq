package io.quarkiverse.messaging.pgmq.connector.tracing;

import org.eclipse.microprofile.reactive.messaging.Message;

import io.opentelemetry.api.GlobalOpenTelemetry;
import io.opentelemetry.instrumentation.api.incubator.semconv.messaging.MessageOperation;
import io.opentelemetry.instrumentation.api.incubator.semconv.messaging.MessagingAttributesExtractor;
import io.opentelemetry.instrumentation.api.incubator.semconv.messaging.MessagingAttributesGetter;
import io.opentelemetry.instrumentation.api.incubator.semconv.messaging.MessagingSpanNameExtractor;
import io.opentelemetry.instrumentation.api.instrumenter.Instrumenter;
import io.opentelemetry.instrumentation.api.instrumenter.InstrumenterBuilder;
import io.smallrye.reactive.messaging.tracing.TracingUtils;

/**
 * Encapsulates the OpenTelemetry instrumentation API so that those classes are only needed if
 * users explicitly enable tracing.
 */
public class PGMQOpenTelemetryInstrumenter {

    private final Instrumenter<PGMQTrace, Void> instrumenter;

    private PGMQOpenTelemetryInstrumenter(Instrumenter<PGMQTrace, Void> instrumenter) {
        this.instrumenter = instrumenter;
    }

    public static PGMQOpenTelemetryInstrumenter createIncoming() {
        return new PGMQOpenTelemetryInstrumenter(createInstrumenter(true));
    }

    public static PGMQOpenTelemetryInstrumenter createOutgoing() {
        return new PGMQOpenTelemetryInstrumenter(createInstrumenter(false));
    }

    // <create-instrumenter>
    public static Instrumenter<PGMQTrace, Void> createInstrumenter(boolean incoming) {
        MessageOperation messageOperation = incoming ? MessageOperation.RECEIVE : MessageOperation.PUBLISH;

        PGMQAttributesExtractor myExtractor = new PGMQAttributesExtractor();
        MessagingAttributesGetter<PGMQTrace, Void> attributesGetter = myExtractor.getMessagingAttributesGetter();
        var spanNameExtractor = MessagingSpanNameExtractor.create(attributesGetter, messageOperation);
        InstrumenterBuilder<PGMQTrace, Void> builder = Instrumenter.builder(GlobalOpenTelemetry.get(),
                "io.smallrye.reactive.messaging", spanNameExtractor);
        var attributesExtractor = MessagingAttributesExtractor.create(attributesGetter, messageOperation);

        builder
                .addAttributesExtractor(attributesExtractor)
                .addAttributesExtractor(myExtractor);

        if (incoming) {
            return builder.buildConsumerInstrumenter(PGMQTraceTextMapGetter.INSTANCE);
        } else {
            return builder.buildProducerInstrumenter(PGMQTraceTextMapSetter.INSTANCE);
        }
    }
    // </create-instrumener>

    public Message<?> traceIncoming(Message<?> message, PGMQTrace myTrace, boolean makeCurrent) {
        return TracingUtils.traceIncoming(instrumenter, message, myTrace, makeCurrent);
    }

    public void traceOutgoing(Message<?> message, PGMQTrace myTrace) {
        TracingUtils.traceOutgoing(instrumenter, message, myTrace);
    }
}
