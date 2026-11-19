package io.quarkiverse.messaging.pgmq.connector;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.Flow;
import java.util.concurrent.atomic.AtomicBoolean;

import org.eclipse.microprofile.reactive.messaging.Message;

import io.opentelemetry.instrumentation.api.instrumenter.Instrumenter;
import io.quarkiverse.messaging.pgmq.client.PGMQClient;
import io.quarkiverse.messaging.pgmq.connector.tracing.PGMQOpenTelemetryInstrumenter;
import io.quarkiverse.messaging.pgmq.connector.tracing.PGMQTrace;
import io.smallrye.mutiny.Multi;
import io.smallrye.reactive.messaging.health.HealthReport;
import io.smallrye.reactive.messaging.tracing.TracingUtils;
import io.vertx.core.internal.VertxInternal;
import io.vertx.core.json.JsonObject;
import io.vertx.mutiny.core.Context;
import io.vertx.mutiny.core.Vertx;

public class PGMQIncomingChannel {

    private final String channel;
    private final PGMQClient client;
    private final PGMQAckHandler ackHandler;
    private final AtomicBoolean closed = new AtomicBoolean(false);
    private final String queueName;
    private final int visibilityTimeout;
    private final int maxPollMessages;
    private final int maxPollSeconds;
    private final int pollIntervalMs;
    private final boolean fifoEnabled;
    private final String fifoMode;
    private final Flow.Publisher<? extends Message<?>> stream;

    public PGMQIncomingChannel(Vertx vertx, PGMQConnectorIncomingConfiguration cfg, PGMQClient client) {
        // create and configure the client with PGMQConnectorIncomingConfiguration
        this.channel = cfg.getChannel();
        this.queueName = cfg.getQueue().orElse(cfg.getChannel());
        this.visibilityTimeout = cfg.getVisibilityTimeout();
        this.maxPollMessages = cfg.getMaxPollMessages();
        this.maxPollSeconds = cfg.getMaxPollSeconds();
        this.pollIntervalMs = cfg.getPollInterval();
        this.fifoEnabled = cfg.getFifo();
        this.fifoMode = cfg.getFifoMode();
        this.client = client;
        Context context = Context.newInstance(((VertxInternal) vertx.getDelegate()).createEventLoopContext());
        if (cfg.getAckBatch()) {
            this.ackHandler = new PGMQThrottledAckHandler(cfg, client, vertx, context);
        } else {
            this.ackHandler = new PGMQAckHandler(cfg, client);
        }
        Multi<? extends Message<?>> receiveMulti = Multi.createBy().repeating()
                .uni(() -> {
                    if (fifoEnabled) {
                        return "round-robin".equals(fifoMode)
                                ? this.client.readGroupedRRWithPoll(queueName, visibilityTimeout, maxPollMessages,
                                        maxPollSeconds, pollIntervalMs)
                                : this.client.readGroupedWithPoll(queueName, visibilityTimeout, maxPollMessages,
                                        maxPollSeconds, pollIntervalMs);
                    }
                    return this.client.readWithPoll(queueName, visibilityTimeout, maxPollMessages, maxPollSeconds,
                            pollIntervalMs);
                })
                .until(__ -> closed.get())
                .filter(Objects::nonNull)
                .emitOn(context::runOnContext)
                .onItem().transformToMultiAndConcatenate(consumed -> Multi.createFrom().iterable(consumed))
                .map(consumed -> new IncomingPGMQMessage(consumed, ackHandler));

        Instrumenter<PGMQTrace, Void> instrumenter = PGMQOpenTelemetryInstrumenter.createInstrumenter(true);
        if (cfg.getTracingEnabled()) {
            receiveMulti = receiveMulti.map(message -> {
                Map<String, String> properties = new HashMap<>();
                JsonObject headers = ((IncomingPGMQMessage) message).getMessage().headers();
                if (headers != null) {
                    headers.forEach(entry -> {
                        if (entry.getValue() instanceof String) {
                            properties.put(entry.getKey(), (String) entry.getValue());
                        }
                    });
                }
                return TracingUtils.traceIncoming(instrumenter, message, new PGMQTrace.Builder()
                        .withQueue(queueName)
                        .withProperties(properties)
                        .build());
            });
        }
        this.stream = receiveMulti;
    }

    public String getChannel() {
        return channel;
    }

    public Flow.Publisher<? extends Message<?>> getStream() {
        return this.stream;
    }

    public void close() {
        closed.compareAndSet(false, true);
        ackHandler.close();
    }

    void isStarted(HealthReport.HealthReportBuilder builder) {
        builder.add(channel, true);
    }

    void isReady(HealthReport.HealthReportBuilder builder) {
        builder.add(channel, !closed.get());
    }

    void isAlive(HealthReport.HealthReportBuilder builder) {
        builder.add(channel, !closed.get());
    }

}
