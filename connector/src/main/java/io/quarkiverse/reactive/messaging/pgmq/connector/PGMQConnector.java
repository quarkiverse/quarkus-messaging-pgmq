package io.quarkiverse.reactive.messaging.pgmq.connector;

import static io.smallrye.reactive.messaging.annotations.ConnectorAttribute.Direction.INCOMING;
import static io.smallrye.reactive.messaging.annotations.ConnectorAttribute.Direction.INCOMING_AND_OUTGOING;
import static io.smallrye.reactive.messaging.annotations.ConnectorAttribute.Direction.OUTGOING;

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

import io.quarkiverse.reactive.messaging.pgmq.connector.client.PGMQClient;
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
@ConnectorAttribute(name = "client-id", type = "string", direction = INCOMING_AND_OUTGOING, description = "The PostgreSQL client identifier, defaults to queue name and then channel name if not set, only one client is found it'll be used")
@ConnectorAttribute(name = "tracing-enabled", type = "boolean", direction = INCOMING_AND_OUTGOING, description = "Whether tracing is enabled (default) or disabled", defaultValue = "true")

@ConnectorAttribute(name = "buffer-size", type = "int", direction = INCOMING, description = "The size buffer of incoming messages waiting to be processed", defaultValue = "128")
@ConnectorAttribute(name = "visibility-timeout", type = "int", direction = INCOMING, description = "The visibility timeout in seconds to pass to the read call", defaultValue = "10")
@ConnectorAttribute(name = "max-poll-messages", type = "int", direction = INCOMING, description = "The maximum number of messages to read", defaultValue = "10")
@ConnectorAttribute(name = "max-poll-seconds", type = "int", direction = INCOMING, description = "The maximum time to wait for messages in seconds", defaultValue = "10")
@ConnectorAttribute(name = "poll-interval", type = "int", direction = INCOMING, description = "The poll interval in milliseconds", defaultValue = "100")

@ConnectorAttribute(name = "batch", type = "boolean", direction = OUTGOING, description = "The maximum size of a queue holding pending messages", defaultValue = "1000")
@ConnectorAttribute(name = "batch-size", type = "int", direction = OUTGOING, description = "The maximum size of a queue holding pending messages", defaultValue = "1000")
@ConnectorAttribute(name = "batch-delay", type = "long", direction = OUTGOING, description = "The maximum size of a queue holding pending messages", defaultValue = "1000")
public class PGMQConnector implements InboundConnector, OutboundConnector, HealthReporter {

    public static final String CONNECTOR_NAME = "smallrye-pgmq";

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
        outboundChannels.forEach(PGMQOutgoingChannel::close);
    }

    @Override
    public Flow.Publisher<? extends Message<?>> getPublisher(Config config) {
        PGMQConnectorIncomingConfiguration ic = new PGMQConnectorIncomingConfiguration(config);
        PGMQClient client = new PGMQClient(getClient(ic));
        PGMQIncomingChannel channel = new PGMQIncomingChannel(vertx, ic, client);
        inboundChannels.add(channel);
        return channel.getStream();
    }

    // TODO: handle client creation properly
    private Pool getClient(PGMQConnectorCommonConfiguration conf) {
        String clientId = conf.getClientId().or(conf::getQueue).orElse(conf.getChannel());
        return CDIUtils.getInstanceById(pgPool, clientId, () -> pgPool.get());
    }

    @Override
    public Flow.Subscriber<? extends Message<?>> getSubscriber(Config config) {
        PGMQConnectorOutgoingConfiguration oc = new PGMQConnectorOutgoingConfiguration(config);
        PGMQClient client = new PGMQClient(getClient(oc));
        PGMQOutgoingChannel channel = new PGMQOutgoingChannel(oc, client);
        outboundChannels.add(channel);
        return channel.getSubscriber();
    }

    @Override
    public HealthReport getReadiness() {
        HealthReport.HealthReportBuilder builder = HealthReport.builder();
        for (PGMQIncomingChannel channel : inboundChannels) {
            builder.add(channel.getChannel(), true);
        }
        for (PGMQOutgoingChannel channel : outboundChannels) {
            builder.add(channel.getChannel(), true);
        }
        return builder.build();
    }

    @Override
    public HealthReport getLiveness() {
        HealthReport.HealthReportBuilder builder = HealthReport.builder();
        for (PGMQIncomingChannel channel : inboundChannels) {
            builder.add(channel.getChannel(), true);
        }
        for (PGMQOutgoingChannel channel : outboundChannels) {
            builder.add(channel.getChannel(), true);
        }
        return builder.build();
    }

    @Override
    public HealthReport getStartup() {
        HealthReport.HealthReportBuilder builder = HealthReport.builder();
        for (PGMQIncomingChannel channel : inboundChannels) {
            builder.add(channel.getChannel(), true);
        }
        for (PGMQOutgoingChannel channel : outboundChannels) {
            builder.add(channel.getChannel(), true);
        }
        return builder.build();
    }
}
