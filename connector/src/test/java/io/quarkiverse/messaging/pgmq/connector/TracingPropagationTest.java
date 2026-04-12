package io.quarkiverse.messaging.pgmq.connector;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import java.util.List;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;

import org.eclipse.microprofile.reactive.messaging.Channel;
import org.eclipse.microprofile.reactive.messaging.Emitter;
import org.eclipse.microprofile.reactive.messaging.Incoming;
import org.eclipse.microprofile.reactive.messaging.Message;
import org.eclipse.microprofile.reactive.messaging.Outgoing;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;

import io.opentelemetry.api.GlobalOpenTelemetry;
import io.opentelemetry.api.trace.SpanKind;
import io.opentelemetry.api.trace.propagation.W3CTraceContextPropagator;
import io.opentelemetry.context.propagation.ContextPropagators;
import io.opentelemetry.sdk.OpenTelemetrySdk;
import io.opentelemetry.sdk.testing.exporter.InMemorySpanExporter;
import io.opentelemetry.sdk.trace.SdkTracerProvider;
import io.opentelemetry.sdk.trace.data.SpanData;
import io.opentelemetry.sdk.trace.export.SimpleSpanProcessor;
import io.vertx.core.json.JsonObject;

public class TracingPropagationTest extends PGMQTestBase {

    static InMemorySpanExporter spanExporter;

    @BeforeAll
    static void setupTracing() {
        spanExporter = InMemorySpanExporter.create();
        SdkTracerProvider tracerProvider = SdkTracerProvider.builder()
                .addSpanProcessor(SimpleSpanProcessor.create(spanExporter))
                .build();
        GlobalOpenTelemetry.resetForTest();
        OpenTelemetrySdk.builder()
                .setPropagators(ContextPropagators.create(W3CTraceContextPropagator.getInstance()))
                .setTracerProvider(tracerProvider)
                .buildAndRegisterGlobal();
    }

    @Override
    @BeforeEach
    void setUp(TestInfo testInfo) {
        queueName = "t_" + Math.abs(UUID.randomUUID().getMostSignificantBits());
        spanExporter.reset();
    }

    @AfterAll
    static void tearDownTracing() {
        GlobalOpenTelemetry.resetForTest();
    }

    @Test
    void testFromAppToPgmq() {
        pgmq.createQueue(queueName).await().indefinitely();

        MapBasedConfig config = new MapBasedConfig()
                .with("mp.messaging.outgoing.out.connector", PGMQConnector.CONNECTOR_NAME)
                .with("mp.messaging.outgoing.out.queue", queueName)
                .with("mp.messaging.outgoing.out.tracing-enabled", true)
                .with("mp.messaging.incoming.data.connector", PGMQConnector.CONNECTOR_NAME)
                .with("mp.messaging.incoming.data.queue", queueName)
                .with("mp.messaging.incoming.data.tracing-enabled", true);
        PropagationAppSendReceive app = runApplication(config, PropagationAppSendReceive.class);

        int messageCount = 10;
        for (int i = 0; i < messageCount; i++) {
            app.send(JsonObject.of("key", "value-" + i));
        }

        await().atMost(10, TimeUnit.SECONDS).until(() -> app.received().size() == messageCount);

        await().atMost(5, TimeUnit.SECONDS).untilAsserted(() -> {
            List<SpanData> spans = spanExporter.getFinishedSpanItems();

            // Should have both PRODUCER and CONSUMER spans
            List<SpanData> producerSpans = spans.stream()
                    .filter(s -> s.getKind() == SpanKind.PRODUCER)
                    .collect(Collectors.toList());
            List<SpanData> consumerSpans = spans.stream()
                    .filter(s -> s.getKind() == SpanKind.CONSUMER)
                    .collect(Collectors.toList());

            assertThat(producerSpans).hasSize(messageCount);
            assertThat(consumerSpans).hasSize(messageCount);

            // Each producer span should have a matching consumer span with the same traceId
            for (SpanData producer : producerSpans) {
                String traceId = producer.getSpanContext().getTraceId();
                assertThat(consumerSpans).anySatisfy(consumer -> {
                    assertThat(consumer.getSpanContext().getTraceId()).isEqualTo(traceId);
                    // Consumer's parent span ID should be the producer's span ID
                    assertThat(consumer.getParentSpanContext().getSpanId())
                            .isEqualTo(producer.getSpanContext().getSpanId());
                });
            }

            // Should have messageCount distinct trace IDs (one per message)
            long distinctTraceIds = spans.stream()
                    .map(s -> s.getSpanContext().getTraceId())
                    .distinct().count();
            assertThat(distinctTraceIds).isEqualTo(messageCount);
        });

        // Verify traceparent header is present in received messages
        assertThat(app.receivedHeaders()).allSatisfy(headers -> {
            assertThat(headers).isNotNull();
            assertThat(headers.containsKey("traceparent")).isTrue();
        });
    }

    @Test
    void testFromPgmqToAppToPgmq() {
        String inQueue = queueName;
        String outQueue = queueName + "_out";
        pgmq.createQueue(inQueue).await().indefinitely();
        pgmq.createQueue(outQueue).await().indefinitely();

        MapBasedConfig config = new MapBasedConfig()
                .with("mp.messaging.incoming.in.connector", PGMQConnector.CONNECTOR_NAME)
                .with("mp.messaging.incoming.in.queue", inQueue)
                .with("mp.messaging.incoming.in.tracing-enabled", true)
                .with("mp.messaging.outgoing.out.connector", PGMQConnector.CONNECTOR_NAME)
                .with("mp.messaging.outgoing.out.queue", outQueue)
                .with("mp.messaging.outgoing.out.tracing-enabled", true);
        PropagationAppProcessAndForward app = runApplication(config, PropagationAppProcessAndForward.class);

        // Send messages directly to the queue (no trace headers)
        pgmq.send(inQueue, JsonObject.of("key", "value")).await().indefinitely();

        await().atMost(10, TimeUnit.SECONDS).until(() -> app.processed().size() == 1);

        await().atMost(5, TimeUnit.SECONDS).untilAsserted(() -> {
            List<SpanData> spans = spanExporter.getFinishedSpanItems();

            List<SpanData> consumerSpans = spans.stream()
                    .filter(s -> s.getKind() == SpanKind.CONSUMER)
                    .collect(Collectors.toList());
            List<SpanData> producerSpans = spans.stream()
                    .filter(s -> s.getKind() == SpanKind.PRODUCER)
                    .collect(Collectors.toList());

            assertThat(consumerSpans).hasSize(1);
            assertThat(producerSpans).hasSize(1);

            SpanData consumer = consumerSpans.get(0);
            SpanData producer = producerSpans.get(0);

            // Consumer span is root (no trace parent injected)
            assertThat(consumer.getParentSpanContext().isValid()).isFalse();

            // Producer span should be child of the consumer span (same trace)
            assertThat(producer.getSpanContext().getTraceId())
                    .isEqualTo(consumer.getSpanContext().getTraceId());
            assertThat(producer.getParentSpanContext().getSpanId())
                    .isEqualTo(consumer.getSpanContext().getSpanId());
        });
    }

    @Test
    void testFromPgmqToAppWithParentSpan() {
        pgmq.createQueue(queueName).await().indefinitely();

        MapBasedConfig config = new MapBasedConfig()
                .with("mp.messaging.incoming.data.connector", PGMQConnector.CONNECTOR_NAME)
                .with("mp.messaging.incoming.data.queue", queueName)
                .with("mp.messaging.incoming.data.tracing-enabled", true);
        PropagationAppReceiveOnly app = runApplication(config, PropagationAppReceiveOnly.class);

        // Inject a known traceparent header
        String parentTraceId = "0af7651916cd43dd8448eb211c80319c";
        String parentSpanId = "b7ad6b7169203331";
        String traceparent = "00-" + parentTraceId + "-" + parentSpanId + "-01";
        JsonObject headers = JsonObject.of("traceparent", traceparent);

        int messageCount = 10;
        for (int i = 0; i < messageCount; i++) {
            pgmq.send(queueName, JsonObject.of("key", "value-" + i), headers).await().indefinitely();
        }

        await().atMost(10, TimeUnit.SECONDS).until(() -> app.received().size() == messageCount);

        await().atMost(5, TimeUnit.SECONDS).untilAsserted(() -> {
            List<SpanData> spans = spanExporter.getFinishedSpanItems();

            List<SpanData> consumerSpans = spans.stream()
                    .filter(s -> s.getKind() == SpanKind.CONSUMER)
                    .collect(Collectors.toList());
            assertThat(consumerSpans).hasSize(messageCount);

            // All consumer spans share the same trace ID as the injected parent
            assertThat(consumerSpans).allSatisfy(span -> {
                assertThat(span.getSpanContext().getTraceId()).isEqualTo(parentTraceId);
                // Each consumer span should be a child of the external parent span
                assertThat(span.getParentSpanContext().getSpanId()).isEqualTo(parentSpanId);
            });
        });
    }

    @Test
    void testFromPgmqToAppWithNoParent() {
        pgmq.createQueue(queueName).await().indefinitely();

        MapBasedConfig config = new MapBasedConfig()
                .with("mp.messaging.incoming.data.connector", PGMQConnector.CONNECTOR_NAME)
                .with("mp.messaging.incoming.data.queue", queueName)
                .with("mp.messaging.incoming.data.tracing-enabled", true);
        PropagationAppReceiveOnly app = runApplication(config, PropagationAppReceiveOnly.class);

        int messageCount = 10;
        for (int i = 0; i < messageCount; i++) {
            pgmq.send(queueName, JsonObject.of("key", "value-" + i)).await().indefinitely();
        }

        await().atMost(10, TimeUnit.SECONDS).until(() -> app.received().size() == messageCount);

        await().atMost(5, TimeUnit.SECONDS).untilAsserted(() -> {
            List<SpanData> spans = spanExporter.getFinishedSpanItems();

            List<SpanData> consumerSpans = spans.stream()
                    .filter(s -> s.getKind() == SpanKind.CONSUMER)
                    .collect(Collectors.toList());
            assertThat(consumerSpans).hasSize(messageCount);

            // All spans should be root spans (no parent)
            assertThat(consumerSpans).allSatisfy(span -> {
                assertThat(span.getParentSpanContext().isValid()).isFalse();
            });

            // Each span should have a unique trace ID
            long distinctTraceIds = consumerSpans.stream()
                    .map(s -> s.getSpanContext().getTraceId())
                    .distinct().count();
            assertThat(distinctTraceIds).isEqualTo(messageCount);
        });
    }

    // --- App beans ---

    @ApplicationScoped
    public static class PropagationAppSendReceive {
        @Inject
        @Channel("out")
        Emitter<JsonObject> emitter;

        final List<String> received = new CopyOnWriteArrayList<>();
        final List<JsonObject> receivedHeaders = new CopyOnWriteArrayList<>();

        @Incoming("data")
        java.util.concurrent.CompletionStage<Void> consume(Message<JsonObject> msg) {
            received.add(msg.getPayload().toString());
            msg.getMetadata(PGMQIncomingMetadata.class).ifPresent(meta -> {
                JsonObject h = meta.getHeaders();
                receivedHeaders.add(h != null ? h : new JsonObject());
            });
            return msg.ack();
        }

        public void send(JsonObject msg) {
            emitter.send(msg);
        }

        public List<String> received() {
            return received;
        }

        public List<JsonObject> receivedHeaders() {
            return receivedHeaders;
        }
    }

    @ApplicationScoped
    public static class PropagationAppProcessAndForward {
        final List<String> processed = new CopyOnWriteArrayList<>();

        @Incoming("in")
        @Outgoing("out")
        JsonObject process(JsonObject msg) {
            processed.add(msg.toString());
            return msg;
        }

        public List<String> processed() {
            return processed;
        }
    }

    @ApplicationScoped
    public static class PropagationAppReceiveOnly {
        final List<String> received = new CopyOnWriteArrayList<>();

        @Incoming("data")
        void consume(JsonObject msg) {
            received.add(msg.toString());
        }

        public List<String> received() {
            return received;
        }
    }
}
