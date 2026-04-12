package io.quarkiverse.messaging.pgmq.connector.i18n;

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
    PGMQLogging log = Logger.getMessageLogger(PGMQLogging.class,
            "io.quarkiverse.messaging.pgmq");

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

    @LogMessage(level = Level.INFO)
    @Message(id = 20310, value = "Auto-creating %s queue '%s'")
    void autoCreatingQueue(String queueType, String queueName);

    @LogMessage(level = Level.INFO)
    @Message(id = 20311, value = "Creating FIFO index on queue '%s'")
    void creatingFifoIndex(String queueName);

    @LogMessage(level = Level.INFO)
    @Message(id = 20312, value = "Binding topic pattern '%s' to queue '%s'")
    void bindingTopicPattern(String pattern, String queueName);

    @LogMessage(level = Level.WARN)
    @Message(id = 20315, value = "Message %d from queue '%s' was nacked")
    void messageNacked(long msgId, String queueName, @Cause Throwable reason);

    @LogMessage(level = Level.DEBUG)
    @Message(id = 20320, value = "Flushing batch ack for queue '%s': %d messages")
    void flushingBatchAck(String queueName, int messageCount);

    @LogMessage(level = Level.ERROR)
    @Message(id = 20321, value = "Batch ack flush failed for queue '%s'")
    void batchAckFlushFailed(String queueName, @Cause Throwable e);

    @LogMessage(level = Level.DEBUG)
    @Message(id = 20322, value = "Throttled ack handler closing for queue '%s', %d remaining acks")
    void throttledAckHandlerClosing(String queueName, int remaining);

}
