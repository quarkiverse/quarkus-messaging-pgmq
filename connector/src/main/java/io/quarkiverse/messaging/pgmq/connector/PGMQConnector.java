package io.quarkiverse.messaging.pgmq.connector;

import static io.smallrye.reactive.messaging.annotations.ConnectorAttribute.Direction.INCOMING;
import static io.smallrye.reactive.messaging.annotations.ConnectorAttribute.Direction.INCOMING_AND_OUTGOING;
import static io.smallrye.reactive.messaging.annotations.ConnectorAttribute.Direction.OUTGOING;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.Flow;

import jakarta.annotation.PostConstruct;
import jakarta.annotation.Priority;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.context.BeforeDestroyed;
import jakarta.enterprise.event.Observes;
import jakarta.enterprise.event.Reception;
import jakarta.enterprise.inject.Instance;
import jakarta.inject.Inject;

import org.eclipse.microprofile.config.Config;
import org.eclipse.microprofile.reactive.messaging.Message;
import org.eclipse.microprofile.reactive.messaging.spi.Connector;

import io.quarkiverse.messaging.pgmq.client.PGMQClient;
import io.quarkiverse.messaging.pgmq.connector.i18n.PGMQLogging;
import io.smallrye.reactive.messaging.annotations.ConnectorAttribute;
import io.smallrye.reactive.messaging.connector.InboundConnector;
import io.smallrye.reactive.messaging.connector.OutboundConnector;
import io.smallrye.reactive.messaging.health.HealthReport;
import io.smallrye.reactive.messaging.health.HealthReporter;
import io.smallrye.reactive.messaging.providers.connectors.ExecutionHolder;
import io.smallrye.reactive.messaging.providers.helpers.CDIUtils;
import io.vertx.mutiny.core.Vertx;
import io.vertx.mutiny.sqlclient.Pool;

@ApplicationScoped
@Connector(PGMQConnector.CONNECTOR_NAME)
@ConnectorAttribute(name = "queue", type = "string", direction = INCOMING_AND_OUTGOING, description = "The default topic to send the messages, defaults to channel name if not set")
@ConnectorAttribute(name = "topic-patterns", type = "string", direction = INCOMING, description = "Comma-separated topic patterns to bind on startup (e.g. 'orders.*,events.#')")
@ConnectorAttribute(name = "client-id", type = "string", direction = INCOMING_AND_OUTGOING, description = "The PostgreSQL client identifier, defaults to queue name and then channel name if not set, only one client is found it'll be used")
@ConnectorAttribute(name = "tracing-enabled", type = "boolean", direction = INCOMING_AND_OUTGOING, description = "Whether tracing is enabled (default) or disabled", defaultValue = "true")
@ConnectorAttribute(name = "queue.declare", type = "boolean", direction = INCOMING_AND_OUTGOING, description = "Whether to auto-create the queue on startup", defaultValue = "false")
@ConnectorAttribute(name = "queue.declare.type", type = "string", direction = INCOMING_AND_OUTGOING, description = "Queue type: 'non_partitioned', 'partitioned', or 'unlogged'", defaultValue = "non_partitioned")
@ConnectorAttribute(name = "queue.declare.partition-interval", type = "string", direction = INCOMING_AND_OUTGOING, description = "Partition interval for partitioned queues (e.g. '10000')")
@ConnectorAttribute(name = "queue.declare.retention-interval", type = "string", direction = INCOMING_AND_OUTGOING, description = "Retention interval for partitioned queues (e.g. '100000')")

@ConnectorAttribute(name = "buffer-size", type = "int", direction = INCOMING, description = "The size buffer of incoming messages waiting to be processed", defaultValue = "128")
@ConnectorAttribute(name = "visibility-timeout", type = "int", direction = INCOMING, description = "The visibility timeout in seconds to pass to the read call", defaultValue = "10")
@ConnectorAttribute(name = "max-poll-messages", type = "int", direction = INCOMING, description = "The maximum number of messages to read", defaultValue = "10")
@ConnectorAttribute(name = "max-poll-seconds", type = "int", direction = INCOMING, description = "The maximum time to wait for messages in seconds", defaultValue = "10")
@ConnectorAttribute(name = "poll-interval", type = "int", direction = INCOMING, description = "The poll interval in milliseconds", defaultValue = "100")
@ConnectorAttribute(name = "ack-type", type = "string", direction = INCOMING, description = "The action to perform on message ack: 'delete' or 'archive'", defaultValue = "delete")
@ConnectorAttribute(name = "failure-type", type = "string", direction = INCOMING, description = "The action to perform on message nack: 'visibility', 'delete', 'archive', or 'ignore'", defaultValue = "visibility")
@ConnectorAttribute(name = "fifo", type = "boolean", direction = INCOMING, description = "Enable FIFO consumption and create FIFO index on startup", defaultValue = "false")
@ConnectorAttribute(name = "fifo-mode", type = "string", direction = INCOMING, description = "FIFO read mode: 'throughput' (batch from same group first) or 'round-robin' (interleave across groups fairly)", defaultValue = "throughput")
@ConnectorAttribute(name = "ack-batch", type = "boolean", direction = INCOMING, description = "Enable throttled batch ack mode", defaultValue = "false")
@ConnectorAttribute(name = "ack-batch-size", type = "int", direction = INCOMING, description = "Max buffered acks before flush", defaultValue = "500")
@ConnectorAttribute(name = "ack-batch-delay", type = "long", direction = INCOMING, description = "Max delay in milliseconds before flushing an incomplete batch", defaultValue = "5000")

@ConnectorAttribute(name = "batch", type = "boolean", direction = OUTGOING, description = "Whether to enable batch mode for sending messages", defaultValue = "false")
@ConnectorAttribute(name = "batch-size", type = "int", direction = OUTGOING, description = "The number of messages to group in each batch when batch mode is enabled", defaultValue = "1000")
@ConnectorAttribute(name = "batch-delay", type = "long", direction = OUTGOING, description = "The maximum delay in milliseconds to wait before sending an incomplete batch", defaultValue = "1000")
@ConnectorAttribute(name = "routing-key", type = "string", direction = OUTGOING, description = "When set, uses sendTopic() instead of send() for topic-based routing")

public class PGMQConnector implements InboundConnector, OutboundConnector, HealthReporter {

    public static final String CONNECTOR_NAME = "quarkus-pgmq";

    @Inject
    ExecutionHolder executionHolder;

    Vertx vertx;

    List<PGMQIncomingChannel> inboundChannels = new CopyOnWriteArrayList<>();
    List<PGMQOutgoingChannel> outboundChannels = new CopyOnWriteArrayList<>();

    @Inject
    Instance<Pool> pgPool;

    @PostConstruct
    void init() {
        this.vertx = executionHolder.vertx();
    }

    public void terminate(
            @Observes(notifyObserver = Reception.IF_EXISTS) @Priority(50) @BeforeDestroyed(ApplicationScoped.class) Object event) {
        inboundChannels.forEach(PGMQIncomingChannel::close);
        inboundChannels.clear();
        outboundChannels.forEach(PGMQOutgoingChannel::close);
        outboundChannels.clear();
    }

    @Override
    public Flow.Publisher<? extends Message<?>> getPublisher(Config config) {
        PGMQConnectorIncomingConfiguration ic = new PGMQConnectorIncomingConfiguration(config);
        PGMQClient client = new PGMQClient(getClient(ic));
        autoCreateQueueIfNeeded(client, ic);
        autoCreateFifoIndex(client, ic);
        autoBindTopics(client, ic);
        PGMQIncomingChannel channel = new PGMQIncomingChannel(vertx, ic, client);
        inboundChannels.add(channel);
        return channel.getStream();
    }

    private void autoBindTopics(PGMQClient client, PGMQConnectorIncomingConfiguration config) {
        String queueName = config.getQueue().orElse(config.getChannel());
        if (config.getTopicPatterns().isPresent()) {
            for (String pattern : config.getTopicPatterns()
                    .map(s -> Arrays.stream(s.split(",")).map(String::trim).filter(p -> !p.isEmpty()).toList())
                    .orElse(Collections.emptyList())) {
                PGMQLogging.log.bindingTopicPattern(pattern, queueName);
                client.bindTopic(pattern, queueName).await().indefinitely();
            }
        }
    }

    private void autoCreateFifoIndex(PGMQClient client, PGMQConnectorIncomingConfiguration config) {
        if (config.getFifo()) {
            String queueName = config.getQueue().orElse(config.getChannel());
            PGMQLogging.log.creatingFifoIndex(queueName);
            client.createFifoIndex(queueName).await().indefinitely();
        }
    }

    private Pool getClient(PGMQConnectorCommonConfiguration conf) {
        String clientId = conf.getClientId().or(conf::getQueue).orElse(conf.getChannel());
        return CDIUtils.getInstanceById(pgPool, clientId, () -> pgPool.get());
    }

    @Override
    public Flow.Subscriber<? extends Message<?>> getSubscriber(Config config) {
        PGMQConnectorOutgoingConfiguration oc = new PGMQConnectorOutgoingConfiguration(config);
        PGMQClient client = new PGMQClient(getClient(oc));
        if (oc.getRoutingKey().isEmpty()) {
            autoCreateQueueIfNeeded(client, oc);
        }
        PGMQOutgoingChannel channel = new PGMQOutgoingChannel(oc, client);
        outboundChannels.add(channel);
        return channel.getSubscriber();
    }

    private void autoCreateQueueIfNeeded(PGMQClient client, PGMQConnectorCommonConfiguration config) {
        if (!config.getQueueDeclare()) {
            return;
        }
        String queueName = config.getQueue().orElse(config.getChannel());
        String queueType = config.getQueueDeclareType();
        PGMQLogging.log.autoCreatingQueue(queueType, queueName);
        switch (queueType) {
            case "non_partitioned" -> client.createQueue(queueName).await().indefinitely();
            case "unlogged" -> client.createUnloggedQueue(queueName).await().indefinitely();
            case "partitioned" -> {
                var pi = config.getQueueDeclarePartitionInterval();
                var ri = config.getQueueDeclareRetentionInterval();
                if (pi.isPresent() && ri.isPresent()) {
                    client.createPartitionedQueue(queueName, pi.get(), ri.get()).await().indefinitely();
                } else {
                    client.createPartitionedQueue(queueName).await().indefinitely();
                }
            }
            default -> throw new IllegalArgumentException("Unknown queue type: " + queueType);
        }
    }

    @Override
    public HealthReport getReadiness() {
        HealthReport.HealthReportBuilder builder = HealthReport.builder();
        for (PGMQIncomingChannel channel : inboundChannels) {
            channel.isReady(builder);
        }
        for (PGMQOutgoingChannel channel : outboundChannels) {
            channel.isReady(builder);
        }
        return builder.build();
    }

    @Override
    public HealthReport getLiveness() {
        HealthReport.HealthReportBuilder builder = HealthReport.builder();
        for (PGMQIncomingChannel channel : inboundChannels) {
            channel.isAlive(builder);
        }
        for (PGMQOutgoingChannel channel : outboundChannels) {
            channel.isAlive(builder);
        }
        return builder.build();
    }

    @Override
    public HealthReport getStartup() {
        HealthReport.HealthReportBuilder builder = HealthReport.builder();
        for (PGMQIncomingChannel channel : inboundChannels) {
            channel.isStarted(builder);
        }
        for (PGMQOutgoingChannel channel : outboundChannels) {
            channel.isStarted(builder);
        }
        return builder.build();
    }
}
