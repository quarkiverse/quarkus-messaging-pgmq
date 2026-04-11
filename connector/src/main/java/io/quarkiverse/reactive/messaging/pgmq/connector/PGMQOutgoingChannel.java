package io.quarkiverse.reactive.messaging.pgmq.connector;

import java.time.Duration;
import java.time.OffsetDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Flow;

import org.eclipse.microprofile.reactive.messaging.Message;

import io.opentelemetry.instrumentation.api.instrumenter.Instrumenter;
import io.quarkiverse.reactive.messaging.pgmq.connector.client.PGMQClient;
import io.quarkiverse.reactive.messaging.pgmq.connector.client.SendMessage;
import io.quarkiverse.reactive.messaging.pgmq.connector.i18n.PGMQLogging;
import io.quarkiverse.reactive.messaging.pgmq.connector.tracing.PGMQOpenTelemetryInstrumenter;
import io.quarkiverse.reactive.messaging.pgmq.connector.tracing.PGMQTrace;
import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.Uni;
import io.smallrye.reactive.messaging.OutgoingMessageMetadata;
import io.smallrye.reactive.messaging.providers.helpers.MultiUtils;
import io.smallrye.reactive.messaging.tracing.TracingUtils;
import io.vertx.core.json.JsonObject;

public class PGMQOutgoingChannel {

    private final String channel;
    private final Flow.Subscriber<? extends Message<?>> subscriber;
    private final List<Throwable> failures = new ArrayList<>();

    private final String queueName;
    private final boolean tracingEnabled;

    private final Instrumenter<PGMQTrace, Void> instrumenter;
    private final boolean batch;
    private final int batchSize;
    private final Duration batchDelay;

    public PGMQOutgoingChannel(PGMQConnectorOutgoingConfiguration oc, PGMQClient client) {
        this.channel = oc.getChannel();
        this.queueName = oc.getQueue().orElse(oc.getChannel());
        this.tracingEnabled = oc.getTracingEnabled();
        this.batch = oc.getBatch();
        this.batchSize = oc.getBatchSize();
        this.batchDelay = Duration.ofMillis(oc.getBatchDelay());
        if (tracingEnabled) {
            this.instrumenter = PGMQOpenTelemetryInstrumenter.createInstrumenter(false);
        } else {
            this.instrumenter = null;
        }
        this.subscriber = MultiUtils.via(multi -> multi
                .plug(stream -> {
                    if (batch) {
                        return stream.group().intoLists().of(batchSize, batchDelay)
                                .call(l -> publishMessage(client, l))
                                .onItem().transformToMultiAndConcatenate(l -> Multi.createFrom().iterable(l));
                    } else {
                        return stream.call(m -> publishMessage(client, m));
                    }
                })
                .onFailure().invoke(f -> {
                    PGMQLogging.log.unableToDispatch(channel, f);
                    reportFailure(f);
                }));

    }

    private synchronized void reportFailure(Throwable failure) {
        // Don't keep all the failures, there are only there for reporting.
        if (failures.size() == 10) {
            failures.remove(0);
        }
        failures.add(failure);
    }

    private Uni<Void> publishMessage(PGMQClient client, List<Message<?>> messageList) {
        List<JsonObject> payloads = new ArrayList<>();
        String queueName = this.queueName;
        int delay = 0;
        OffsetDateTime visibilityTimeout = null;
        for (Message<?> message : messageList) {
            payloads.add(getPayload(message));
        }
        return client.sendBatch(queueName, payloads)
                .onItemOrFailure().transformToUni((responses, t) -> {
                    if (t == null) {
                        if (responses.size() != messageList.size()) {
                            IllegalStateException failure = new IllegalStateException(
                                    "The number of responses does not match the number of messages sent: "
                                            + responses.size() + " vs " + messageList.size());
                            return Uni.combine().all().unis(messageList.stream()
                                    .map(m -> Uni.createFrom().completionStage(m.nack(failure))).toList())
                                    .discardItems();
                        }
                        for (int i = 0; i < messageList.size(); i++) {
                            Message<?> message = messageList.get(i);
                            long response = responses.get(i);
                            OutgoingMessageMetadata.setResultOnMessage(message, response);
                        }
                        return Uni.combine().all().unis(messageList.stream()
                                .map(m -> Uni.createFrom().completionStage(m.ack())).toList())
                                .discardItems();
                    } else {
                        return Uni.combine().all().unis(messageList.stream()
                                .map(m -> Uni.createFrom().completionStage(m.nack(t))).toList())
                                .discardItems();
                    }
                })
                .replaceWithVoid();
    }

    JsonObject getPayload(Message<?> message) {
        Object payload = message.getPayload();
        if (payload instanceof SendMessage send) {
            return JsonObject.mapFrom(send.getPayload());
        } else if (payload instanceof JsonObject) {
            return (JsonObject) payload;
        } else {
            return JsonObject.mapFrom(payload);
        }
    }

    private Uni<Void> publishMessage(PGMQClient client, Message<?> message) {
        // construct the outgoing message
        SendMessage<Object> sendMessage;
        String queueName = this.queueName;
        Object payload = message.getPayload();
        if (payload instanceof SendMessage) {
            sendMessage = (SendMessage<Object>) message.getPayload();
        } else {
            sendMessage = new SendMessage<>();
            sendMessage.setPayload(payload);
            sendMessage.setQueueName(this.queueName);
            message.getMetadata(PGMQOutgoingMetadata.class).ifPresent(out -> {
                sendMessage.setQueueName(out.getQueueName());
                //...
            });
        }
        if (tracingEnabled) {
            TracingUtils.traceOutgoing(instrumenter, message, new PGMQTrace.Builder()
                    .withQueue(queueName)
                    .build());
        }
        JsonObject msg = JsonObject.mapFrom(sendMessage.getPayload());
        return (sendMessage.getVisibilityTimeout() == null
                ? client.send(sendMessage.getQueueName(), msg, sendMessage.getDelay())
                : client.send(sendMessage.getQueueName(), msg, sendMessage.getVisibilityTimeout()))
                .onItemOrFailure().transformToUni((response, t) -> {
                    if (t == null) {
                        OutgoingMessageMetadata.setResultOnMessage(message, response);
                        return Uni.createFrom().completionStage(message.ack());
                    } else {
                        return Uni.createFrom().completionStage(message.nack(t));
                    }
                });
    }

    public Flow.Subscriber<? extends Message<?>> getSubscriber() {
        return this.subscriber;
    }

    public String getChannel() {
        return this.channel;
    }

    public void close() {
        // TODO: implement close logic if needed
    }
}
