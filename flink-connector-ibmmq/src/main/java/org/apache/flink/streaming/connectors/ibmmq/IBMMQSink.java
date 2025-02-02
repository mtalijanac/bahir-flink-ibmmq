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

import javax.jms.BytesMessage;
import javax.jms.Connection;
import javax.jms.DeliveryMode;
import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.MessageProducer;
import javax.jms.Session;

import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.connectors.activemq.internal.IBMMQUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ibm.mq.jms.MQConnectionFactory;

/**
 * Sink class for writing data into ActiveMQ queue.
 * <p>
 * To create an instance of AMQSink class one should initialize and configure an
 * instance of a connection factory that will be used to create a connection.
 * Every input message is converted into a byte array using a serialization
 * schema and being sent into a message queue.
 *
 * @param <IN> type of input messages
 */
public class IBMMQSink<IN> extends RichSinkFunction<IN> {
    private static final long serialVersionUID = 1L;

    private static final Logger LOG = LoggerFactory.getLogger(IBMMQSink.class);


    // Factory that is used to create AMQ connection
    private final MQConnectionFactory connectionFactory;
    // Name of a queue or topic
    private final String destinationName;
    // Serialization scheme that is used to convert input message to bytes
    private final SerializationSchema<IN> serializationSchema;
    // Defines if persistent delivery in AMQ is used
    private final boolean persistentDelivery;
    // Type of AMQ destination (topic or a queue)
    private final DestinationType destinationType;
    // Throw exceptions or just log them
    private boolean logFailuresOnly = false;
    // Used to send messages
    private transient MessageProducer producer;
    // AMQ session
    private transient Session session;
    // AMQ connection
    private transient Connection connection;

    /**
     * Create AMQSink.
     *
     * @param config AMQSink configuration
     */
    public IBMMQSink(IBMMQSinkConfig<IN> config) {
        this.connectionFactory = config.getConnectionFactory();
        this.destinationName = config.getDestinationName();
        this.serializationSchema = config.getSerializationSchema();
        this.persistentDelivery = config.isPersistentDelivery();
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


    @Override
    public void open(Configuration config) throws Exception {
        super.open(config);
        // Create a Connection
        connection = connectionFactory.createConnection();
        connection.start();

        // Create a Session
        session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);

        // Create the destination (Topic or Queue)
        Destination destination = IBMMQUtil.getDestination(session, destinationType, destinationName);

        // Create a MessageProducer from the Session to the Topic or
        // Queue
        producer = session.createProducer(destination);
        producer.setDeliveryMode(getDeliveryMode());
    }

    private int getDeliveryMode() {
        if (persistentDelivery) {
            return DeliveryMode.PERSISTENT;
        }

        return DeliveryMode.NON_PERSISTENT;
    }

    /**
     * Called when new data arrives to the sink, and forwards it to RMQ.
     *
     * @param value
     *            The incoming data
     */
    @Override
    public void invoke(IN value, Context context) throws Exception {
        try {
            byte[] bytes = serializationSchema.serialize(value);
            BytesMessage message = session.createBytesMessage();
            message.writeBytes(bytes);
            producer.send(message);
        } catch (JMSException e) {
            if (logFailuresOnly) {
                LOG.error("Failed to send message to ActiveMQ", e);
            } else {
                throw new RuntimeException("Failed to send message to ActiveMQ", e);
            }
        }
    }

    @Override
    public void close() {
        RuntimeException t = null;
        try {
            session.close();
        } catch (JMSException e) {
            if (logFailuresOnly) {
                LOG.error("Failed to close ActiveMQ session", e);
            } else {
                t = new RuntimeException("Failed to close ActiveMQ session", e);
            }
        }

        try {
            connection.close();
        } catch (JMSException e) {
            if (logFailuresOnly) {
                LOG.error("Failed to close ActiveMQ connection", e);
            } else {
                t = t == null    ? new RuntimeException("Failed to close ActiveMQ session", e)
                                : t;
            }
        }

        if (t != null) {
            throw t;
        }
    }

}
