/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.connectors.ibmmq;

import java.util.HashMap;
import java.util.Set;

import javax.jms.BytesMessage;
import javax.jms.Connection;
import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.Session;

import org.apache.activemq.ActiveMQSession;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.source.MessageAcknowledgingSourceBase;
import org.apache.flink.streaming.api.operators.StreamingRuntimeContext;
import org.apache.flink.streaming.connectors.activemq.internal.IBMMQExceptionListener;
import org.apache.flink.streaming.connectors.activemq.internal.IBMMQUtil;
import org.apache.flink.streaming.connectors.activemq.internal.RunningChecker;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ibm.mq.jms.MQConnectionFactory;

/**
 * Source for reading messages from an ActiveMQ queue.
 * <p>
 * To create an instance of AMQSink class one should initialize and configure an
 * instance of a connection factory that will be used to create a connection.
 * This source is waiting for incoming messages from ActiveMQ and converts them from
 * an array of bytes into an instance of the output type. If an incoming
 * message is not a message with an array of bytes, this message is ignored
 * and warning message is logged.
 *
 * If checkpointing is enabled AMQSink will not acknowledge received AMQ messages as they arrive,
 * but will store them internally and will acknowledge a bulk of messages during checkpointing.
 *
 * @param <OUT> type of output messages
 */
public class IBMMQSource<OUT> extends MessageAcknowledgingSourceBase<OUT, String>
    implements ResultTypeQueryable<OUT> {

    private static final Logger LOG = LoggerFactory.getLogger(IBMMQSource.class);

    // Factory that is used to create AMQ connection
    private final MQConnectionFactory connectionFactory;
    // Name of a queue or topic
    private final String destinationName;
    // Deserialization scheme that is used to convert bytes to output message
    private final DeserializationSchema<OUT> deserializationSchema;
    // Type of AMQ destination (topic or a queue)
    private final DestinationType destinationType;
    // Throw exceptions or just log them
    private boolean logFailuresOnly = false;
    // Stores if source is running (used for testing)
    private RunningChecker runningChecker;
    // AMQ connection
    private transient Connection connection;
    // AMQ session
    private transient Session session;
    // Used to receive incoming messages
    private transient MessageConsumer consumer;
    // If source should immediately acknowledge incoming message
    private boolean autoAck;
    // Map of message ids to currently unacknowledged AMQ messages
    private HashMap<String, Message> unacknowledgedMessages = new HashMap<>();
    // Listener for AMQ exceptions
    private IBMMQExceptionListener exceptionListener;

    /**
     * Create AMQSource.
     *
     * @param config AMQSource configuration
     */
    public IBMMQSource(IBMMQSourceConfig<OUT> config) {
        super(String.class);
        this.connectionFactory = config.getConnectionFactory();
        this.destinationName = config.getDestinationName();
        this.deserializationSchema = config.getDeserializationSchema();
        this.runningChecker = config.getRunningChecker();
        this.destinationType = config.getDestinationType();
    }

    /**
     * Defines whether the producer should fail on errors, or only log them.
     * If this is set to true, then exceptions will be only logged, if set to false,
     * exceptions will be eventually thrown and cause the streaming program to
     * fail (and enter recovery).
     *
     * @param logFailuresOnly The flag to indicate logging-only on exceptions.
     */
    public void setLogFailuresOnly(boolean logFailuresOnly) {
        this.logFailuresOnly = logFailuresOnly;
    }

    // Visible for testing
    void setExceptionListener(IBMMQExceptionListener exceptionListener) {
        this.exceptionListener = exceptionListener;
    }

    @Override
    public void open(Configuration config) throws Exception {
        super.open(config);
        // Create a Connection
        connection = connectionFactory.createConnection();
        connection.start();

        exceptionListener = new IBMMQExceptionListener(LOG, logFailuresOnly);
        connection.setExceptionListener(exceptionListener);

        RuntimeContext runtimeContext = getRuntimeContext();
        int acknowledgeType;
        if (runtimeContext instanceof StreamingRuntimeContext
            && ((StreamingRuntimeContext) runtimeContext).isCheckpointingEnabled()) {
            autoAck = false;
            acknowledgeType = ActiveMQSession.INDIVIDUAL_ACKNOWLEDGE;
        } else {
            autoAck = true;
            acknowledgeType = ActiveMQSession.AUTO_ACKNOWLEDGE;
        }
        // Create a Session
        session = connection.createSession(false, acknowledgeType);

        // Create the destination (Topic or Queue)
        Destination destination = IBMMQUtil.getDestination(session, destinationType, destinationName);

        // Create a MessageConsumer from the Session to the Topic or
        // Queue
        consumer = session.createConsumer(destination);
        runningChecker.setIsRunning(true);
    }

    @Override
    public void close() throws Exception {
        super.close();
        RuntimeException exception = null;
        try {
            consumer.close();
        } catch (JMSException e) {
            if (logFailuresOnly) {
                LOG.error("Failed to close ActiveMQ session", e);
            } else {
                exception = new RuntimeException("Failed to close ActiveMQ consumer", e);
            }
        }
        try {
            session.close();
        } catch (JMSException e) {
            if (logFailuresOnly) {
                LOG.error("Failed to close ActiveMQ session", e);
            } else {
                exception = exception == null    ? new RuntimeException("Failed to close ActiveMQ session", e)
                                                : exception;
            }

        }
        try {
            connection.close();
        } catch (JMSException e) {
            if (logFailuresOnly) {
                LOG.error("Failed to close ActiveMQ session", e);
            } else {
                exception = exception == null    ? new RuntimeException("Failed to close ActiveMQ connection", e)
                                                : exception;
            }
        }

        if (exception != null) {
            throw exception;
        }
    }

    @Override
    protected void acknowledgeIDs(long checkpointId, Set<String> UIds) {
        try {
            for (String messageId : UIds) {
                Message unacknowledgedMessage = unacknowledgedMessages.get(messageId);
                if (unacknowledgedMessage != null) {
                    unacknowledgedMessage.acknowledge();
                    unacknowledgedMessages.remove(messageId);
                } else {
                    LOG.warn("Tried to acknowledge unknown ActiveMQ message id: {}", messageId);
                }
            }
        } catch (JMSException e) {
            if (logFailuresOnly) {
                LOG.error("Failed to acknowledge ActiveMQ message");
            } else {
                throw new RuntimeException("Failed to acknowledge ActiveMQ message");
            }
        }
    }

    @Override
    public void run(SourceContext<OUT> ctx) throws Exception {
        while (runningChecker.isRunning()) {
            exceptionListener.checkErroneous();

            Message message = consumer.receive(1000);
            if (! (message instanceof BytesMessage)) {
                LOG.warn("Active MQ source received non bytes message: {}", message);
                continue;
            }
            BytesMessage bytesMessage = (BytesMessage) message;
            byte[] bytes = new byte[(int) bytesMessage.getBodyLength()];
            bytesMessage.readBytes(bytes);
            OUT value = deserializationSchema.deserialize(bytes);
            synchronized (ctx.getCheckpointLock()) {
                if (!autoAck && addId(bytesMessage.getJMSMessageID())) {
                    ctx.collect(value);
                    unacknowledgedMessages.put(bytesMessage.getJMSMessageID(), bytesMessage);
                } else {
                    ctx.collect(value);
                }
            }
        }
    }

    @Override
    public void cancel() {
        runningChecker.setIsRunning(false);
    }

    @Override
    public TypeInformation<OUT> getProducedType() {
        return deserializationSchema.getProducedType();
    }

}
