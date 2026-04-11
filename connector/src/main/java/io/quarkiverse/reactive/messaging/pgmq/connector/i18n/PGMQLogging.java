package io.quarkiverse.reactive.messaging.pgmq.connector.i18n;

import java.lang.invoke.MethodHandles;

import org.jboss.logging.BasicLogger;
import org.jboss.logging.Logger;
import org.jboss.logging.Logger.Level;
import org.jboss.logging.annotations.Cause;
import org.jboss.logging.annotations.LogMessage;
import org.jboss.logging.annotations.Message;
import org.jboss.logging.annotations.MessageLogger;

/**
 * Logging for PGMQ Connector
 * Assigned ID range is 20300-20399
 */
@MessageLogger(projectCode = "SRMSG", length = 5)
public interface PGMQLogging extends BasicLogger {
    PGMQLogging log = Logger.getMessageLogger(MethodHandles.publicLookup(), PGMQLogging.class,
            "io.smallrye.reactive.messaging.pgmq");

    @LogMessage(level = Level.TRACE)
    @Message(id = 20300, value = "Received PGMQ message %s")
    void receivedMessage(String message);

    @LogMessage(level = Level.TRACE)
    @Message(id = 20301, value = "PGMQ message is null")
    void receivedEmptyMessage();

    @LogMessage(level = Level.ERROR)
    @Message(id = 20302, value = "Error while receiving the message from channel '%s'")
    void errorReceivingMessage(String channel, @Cause Throwable e);

    @LogMessage(level = Level.DEBUG)
    @Message(id = 20305, value = "Message sent for channel %s with message id %s and sequence number %s")
    void messageSentToChannel(String channel, String messageId, String sequence);

    @LogMessage(level = Level.ERROR)
    @Message(id = 20306, value = "Error while sending message from channel '%s'")
    void unableToDispatch(String channel, @Cause Throwable e);

    @LogMessage(level = Level.ERROR)
    @Message(id = 20307, value = "A message sent to channel `%s` has been nacked, fail-stop")
    void messageNackedFailStop(String channel);
}
