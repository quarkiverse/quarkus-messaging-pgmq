package io.quarkiverse.messaging.pgmq.connector.i18n;

import java.lang.invoke.MethodHandles;

import org.jboss.logging.Messages;
import org.jboss.logging.annotations.Cause;
import org.jboss.logging.annotations.Message;
import org.jboss.logging.annotations.MessageBundle;

/**
 * Exceptions for AWS Sqs Connector
 * Assigned ID range is 20400-20499
 */
@MessageBundle(projectCode = "SRMSG", length = 5)
public interface PGMQExceptions {

    PGMQExceptions ex = Messages.getBundle(MethodHandles.publicLookup(), PGMQExceptions.class);

    @Message(id = 20401, value = "Unable to load the class %s")
    IllegalStateException illegalStateUnableToLoadClass(String className, @Cause Throwable t);
}
