package io.quarkiverse.messaging.pgmq.connector;

import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Flow;

import org.eclipse.microprofile.reactive.messaging.Message;

import io.opentelemetry.instrumentation.api.instrumenter.Instrumenter;
import io.quarkiverse.messaging.pgmq.client.PGMQClient;
import io.quarkiverse.messaging.pgmq.client.PGMQTopicResult;
import io.quarkiverse.messaging.pgmq.client.SendMessage;
import io.quarkiverse.messaging.pgmq.connector.i18n.PGMQLogging;
import io.quarkiverse.messaging.pgmq.connector.tracing.PGMQOpenTelemetryInstrumenter;
import io.quarkiverse.messaging.pgmq.connector.tracing.PGMQTrace;
import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.Uni;
import io.smallrye.reactive.messaging.OutgoingMessageMetadata;
import io.smallrye.reactive.messaging.health.HealthReport;
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
    private final String routingKey;

    public PGMQOutgoingChannel(PGMQConnectorOutgoingConfiguration oc, PGMQClient client) {
        this.channel = oc.getChannel();
        this.routingKey = oc.getRoutingKey().orElse(null);
        this.queueName = routingKey != null ? null : oc.getQueue().orElse(oc.getChannel());
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
        List<JsonObject> headersList = new ArrayList<>();
        String queueName = this.queueName;
        boolean hasHeaders = false;
        for (Message<?> message : messageList) {
            payloads.add(getPayload(message));
            JsonObject h = getHeaders(message);
            headersList.add(h);
            if (h != null) {
                hasHeaders = true;
            }
        }
        if (routingKey != null) {
            Uni<List<PGMQTopicResult>> topicUni = hasHeaders
                    ? client.sendBatchTopic(routingKey, payloads, headersList)
                    : client.sendBatchTopic(routingKey, payloads);
            return topicUni
                    .onItemOrFailure().transformToUni((responses, t) -> {
                        if (t == null) {
                            for (int i = 0; i < messageList.size(); i++) {
                                Message<?> message = messageList.get(i);
                                PGMQTopicResult response = responses.get(i);
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
        Uni<List<Long>> sendUni = hasHeaders
                ? client.sendBatch(queueName, payloads, headersList)
                : client.sendBatch(queueName, payloads);
        return sendUni
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
        if (payload instanceof SendMessage<?> send) {
            Object inner = send.getPayload();
            if (inner instanceof JsonObject) {
                return (JsonObject) inner;
            }
            return JsonObject.mapFrom(inner);
        } else if (payload instanceof JsonObject) {
            return (JsonObject) payload;
        } else {
            return JsonObject.mapFrom(payload);
        }
    }

    JsonObject getHeaders(Message<?> message) {
        Object payload = message.getPayload();
        if (payload instanceof SendMessage<?> send && send.getHeaders() != null) {
            return send.getHeaders();
        }
        return message.getMetadata(PGMQOutgoingMetadata.class)
                .map(PGMQOutgoingMetadata::getHeaders)
                .orElse(null);
    }

    @SuppressWarnings("unchecked")
    private Uni<Void> publishMessage(PGMQClient client, Message<?> message) {
        // construct the outgoing message
        SendMessage<Object> sendMessage;
        String queueName = this.queueName;
        Object payload = message.getPayload();
        if (payload instanceof SendMessage<?>) {
            sendMessage = (SendMessage<Object>) payload;
            if (sendMessage.getQueueName() == null) {
                sendMessage.setQueueName(this.queueName);
            }
        } else {
            sendMessage = new SendMessage<>();
            sendMessage.setPayload(payload);
            sendMessage.setQueueName(this.queueName);
            message.getMetadata(PGMQOutgoingMetadata.class).ifPresent(out -> {
                if (out.getQueueName() != null) {
                    sendMessage.setQueueName(out.getQueueName());
                }
            });
        }
        JsonObject msg = getPayload(message);
        JsonObject headers = getHeaders(message);
        if (tracingEnabled) {
            Map<String, String> traceProperties = new HashMap<>();
            TracingUtils.traceOutgoing(instrumenter, message, new PGMQTrace.Builder()
                    .withQueue(queueName)
                    .withProperties(traceProperties)
                    .build());
            if (!traceProperties.isEmpty()) {
                if (headers == null) {
                    headers = new JsonObject();
                }
                for (Map.Entry<String, String> entry : traceProperties.entrySet()) {
                    headers.put(entry.getKey(), entry.getValue());
                }
            }
        }
        if (routingKey != null) {
            Uni<Integer> topicUni;
            if (headers != null) {
                topicUni = client.sendTopic(routingKey, msg, headers, sendMessage.getDelay());
            } else if (sendMessage.getDelay() > 0) {
                topicUni = client.sendTopic(routingKey, msg, sendMessage.getDelay());
            } else {
                topicUni = client.sendTopic(routingKey, msg);
            }
            return topicUni
                    .onItemOrFailure().transformToUni((response, t) -> {
                        if (t == null) {
                            OutgoingMessageMetadata.setResultOnMessage(message, response);
                            return Uni.createFrom().completionStage(message.ack());
                        } else {
                            return Uni.createFrom().completionStage(message.nack(t));
                        }
                    });
        }
        Uni<Long> sendUni;
        if (headers != null) {
            sendUni = sendMessage.getVisibilityTimeout() == null
                    ? client.send(sendMessage.getQueueName(), msg, headers, sendMessage.getDelay())
                    : client.send(sendMessage.getQueueName(), msg, headers, sendMessage.getVisibilityTimeout());
        } else {
            sendUni = sendMessage.getVisibilityTimeout() == null
                    ? client.send(sendMessage.getQueueName(), msg, sendMessage.getDelay())
                    : client.send(sendMessage.getQueueName(), msg, sendMessage.getVisibilityTimeout());
        }
        return sendUni
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

    void isStarted(HealthReport.HealthReportBuilder builder) {
        builder.add(channel, true);
    }

    void isReady(HealthReport.HealthReportBuilder builder) {
        builder.add(channel, true);
    }

    void isAlive(HealthReport.HealthReportBuilder builder) {
        builder.add(channel, failures.isEmpty());
    }

    public void close() {
        // TODO: implement close logic if needed
    }
}
