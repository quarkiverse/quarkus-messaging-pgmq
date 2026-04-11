package io.quarkiverse.reactive.messaging.pgmq.connector;

import java.util.Objects;
import java.util.concurrent.Flow;
import java.util.concurrent.atomic.AtomicBoolean;

import org.eclipse.microprofile.reactive.messaging.Message;

import io.opentelemetry.instrumentation.api.instrumenter.Instrumenter;
import io.quarkiverse.reactive.messaging.pgmq.connector.client.PGMQClient;
import io.quarkiverse.reactive.messaging.pgmq.connector.tracing.PGMQOpenTelemetryInstrumenter;
import io.quarkiverse.reactive.messaging.pgmq.connector.tracing.PGMQTrace;
import io.smallrye.mutiny.Multi;
import io.smallrye.reactive.messaging.health.HealthReport;
import io.smallrye.reactive.messaging.tracing.TracingUtils;
import io.vertx.core.impl.VertxInternal;
import io.vertx.mutiny.core.Context;
import io.vertx.mutiny.core.Vertx;

public class PGMQIncomingChannel {

    private final String channel;
    private final PGMQClient client;
    private final Context context;
    private final PGMQAckHandler ackHandler;
    private final PGMQFailureHandler failureHandler;
    private final boolean tracingEnabled;
    private final AtomicBoolean closed = new AtomicBoolean(false);
    private final String queueName;
    private final int visibilityTimeout;
    private final int maxPollMessages;
    private final int maxPollSeconds;
    private final int pollIntervalMs;
    private Flow.Publisher<? extends Message<?>> stream;

    public PGMQIncomingChannel(Vertx vertx, PGMQConnectorIncomingConfiguration cfg, PGMQClient client) {
        // create and configure the client with PGMQConnectorIncomingConfiguration
        this.channel = cfg.getChannel();
        this.queueName = cfg.getQueue().orElse(cfg.getChannel());
        this.visibilityTimeout = cfg.getVisibilityTimeout();
        this.maxPollMessages = cfg.getMaxPollMessages();
        this.maxPollSeconds = cfg.getMaxPollSeconds();
        this.pollIntervalMs = cfg.getPollInterval();
        this.client = client;
        this.context = Context.newInstance(((VertxInternal) vertx.getDelegate()).createEventLoopContext());
        this.ackHandler = PGMQAckHandler.create(queueName, this.client);
        this.failureHandler = PGMQFailureHandler.create(queueName, this.client);
        this.tracingEnabled = cfg.getTracingEnabled();
        Multi<? extends Message<?>> receiveMulti = Multi.createBy().repeating()
                .uni(() -> this.client == null ? null
                        : this.client.readWithPoll(queueName, visibilityTimeout, maxPollMessages, maxPollSeconds,
                                pollIntervalMs))
                .until(__ -> closed.get())
                .filter(Objects::nonNull)
                .emitOn(context::runOnContext)
                .onItem().transformToMultiAndConcatenate(consumed -> Multi.createFrom().iterable(consumed))
                .map(consumed -> new IncomingPGMQMessage(consumed, ackHandler, failureHandler));

        Instrumenter<PGMQTrace, Void> instrumenter = PGMQOpenTelemetryInstrumenter.createInstrumenter(true);
        if (tracingEnabled) {
            receiveMulti = receiveMulti.map(message -> {
                return TracingUtils.traceIncoming(instrumenter, message, new PGMQTrace.Builder()
                        .withQueue(queueName)
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
    }

    void isReady(HealthReport.HealthReportBuilder healthReportBuilder) {
        // TODO implement
    }

}
