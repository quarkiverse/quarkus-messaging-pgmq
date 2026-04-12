package io.quarkiverse.messaging.pgmq.connector;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import java.util.List;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;

import org.eclipse.microprofile.reactive.messaging.Channel;
import org.eclipse.microprofile.reactive.messaging.Emitter;
import org.eclipse.microprofile.reactive.messaging.Incoming;
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
import io.quarkiverse.messaging.pgmq.client.PGMQMetrics;
import io.vertx.core.json.JsonObject;

public class PGMQTracingTest extends PGMQTestBase {

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
        // Use shorter queue names to stay within PGMQ's 47-character limit
        String shortName = "t_" + Math.abs(UUID.randomUUID().getMostSignificantBits());
        queueName = shortName.replace("-", "_");
        spanExporter.reset();
    }

    @AfterAll
    static void tearDownTracing() {
        GlobalOpenTelemetry.resetForTest();
    }

    @Test
    void incomingChannelCreatesConsumerSpan() {
        pgmq.createQueue(queueName).await().indefinitely();

        MapBasedConfig config = new MapBasedConfig()
                .with("mp.messaging.incoming.data.connector", PGMQConnector.CONNECTOR_NAME)
                .with("mp.messaging.incoming.data.queue", queueName)
                .with("mp.messaging.incoming.data.tracing-enabled", true);
        TracingIncomingApp app = runApplication(config, TracingIncomingApp.class);

        pgmq.send(queueName, JsonObject.of("key", "value")).await().indefinitely();

        await().atMost(5, TimeUnit.SECONDS).until(() -> app.received().size() == 1);

        await().atMost(5, TimeUnit.SECONDS).untilAsserted(() -> {
            List<SpanData> spans = spanExporter.getFinishedSpanItems();
            assertThat(spans).anySatisfy(span -> {
                assertThat(span.getKind()).isEqualTo(SpanKind.CONSUMER);
                assertThat(span.getName()).contains(queueName).contains("receive");
                assertThat(span.getInstrumentationScopeInfo().getName())
                        .isEqualTo("io.smallrye.reactive.messaging");
                assertThat(span.getAttributes().asMap().values().stream()
                        .map(Object::toString))
                        .anySatisfy(v -> assertThat(v).isEqualTo(queueName));
            });
        });
    }

    @Test
    void outgoingChannelCreatesProducerSpan() {
        pgmq.createQueue(queueName).await().indefinitely();

        MapBasedConfig config = new MapBasedConfig()
                .with("mp.messaging.outgoing.out.connector", PGMQConnector.CONNECTOR_NAME)
                .with("mp.messaging.outgoing.out.queue", queueName)
                .with("mp.messaging.outgoing.out.tracing-enabled", true);
        TracingOutgoingApp app = runApplication(config, TracingOutgoingApp.class);

        app.send(JsonObject.of("key", "value"));

        await().atMost(5, TimeUnit.SECONDS).untilAsserted(() -> {
            PGMQMetrics metrics = pgmq.getQueueMetrics(queueName).await().indefinitely();
            assertThat(metrics.queueLength()).isEqualTo(1);
        });

        await().atMost(5, TimeUnit.SECONDS).untilAsserted(() -> {
            List<SpanData> spans = spanExporter.getFinishedSpanItems();
            assertThat(spans).anySatisfy(span -> {
                assertThat(span.getKind()).isEqualTo(SpanKind.PRODUCER);
                assertThat(span.getName()).contains(queueName).contains("publish");
                assertThat(span.getInstrumentationScopeInfo().getName())
                        .isEqualTo("io.smallrye.reactive.messaging");
                assertThat(span.getAttributes().asMap().values().stream()
                        .map(Object::toString))
                        .anySatisfy(v -> assertThat(v).isEqualTo(queueName));
            });
        });
    }

    @Test
    void tracingDisabledCreatesNoSpans() {
        pgmq.createQueue(queueName).await().indefinitely();

        MapBasedConfig config = new MapBasedConfig()
                .with("mp.messaging.outgoing.out.connector", PGMQConnector.CONNECTOR_NAME)
                .with("mp.messaging.outgoing.out.queue", queueName)
                .with("mp.messaging.outgoing.out.tracing-enabled", false)
                .with("mp.messaging.incoming.in.connector", PGMQConnector.CONNECTOR_NAME)
                .with("mp.messaging.incoming.in.queue", queueName)
                .with("mp.messaging.incoming.in.tracing-enabled", false);
        TracingBidirectionalApp app = runApplication(config, TracingBidirectionalApp.class);

        app.send(JsonObject.of("key", "value"));

        await().atMost(5, TimeUnit.SECONDS).until(() -> app.received().size() == 1);

        // Give a bit of time for any async span export that might happen
        try {
            Thread.sleep(500);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }

        List<SpanData> spans = spanExporter.getFinishedSpanItems();
        assertThat(spans).noneMatch(span -> span.getInstrumentationScopeInfo().getName()
                .equals("io.smallrye.reactive.messaging"));
    }

    // --- App beans ---

    @ApplicationScoped
    public static class TracingIncomingApp {
        List<String> received = new CopyOnWriteArrayList<>();

        @Incoming("data")
        void consume(JsonObject msg) {
            received.add(msg.toString());
        }

        public List<String> received() {
            return received;
        }
    }

    @ApplicationScoped
    public static class TracingOutgoingApp {
        @Inject
        @Channel("out")
        Emitter<JsonObject> emitter;

        public void send(JsonObject msg) {
            emitter.send(msg);
        }
    }

    @ApplicationScoped
    public static class TracingBidirectionalApp {
        @Inject
        @Channel("out")
        Emitter<JsonObject> emitter;

        List<String> received = new CopyOnWriteArrayList<>();

        @Incoming("in")
        void consume(JsonObject msg) {
            received.add(msg.toString());
        }

        public void send(JsonObject msg) {
            emitter.send(msg);
        }

        public List<String> received() {
            return received;
        }
    }
}
