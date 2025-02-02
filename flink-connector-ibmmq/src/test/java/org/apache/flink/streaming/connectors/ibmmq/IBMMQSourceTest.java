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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyBoolean;
import static org.mockito.Matchers.anyInt;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.Collections;
import java.util.OptionalLong;

import javax.jms.BytesMessage;
import javax.jms.Connection;
import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.MessageConsumer;
import javax.jms.Session;

import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.state.KeyedStateStore;
import org.apache.flink.api.common.state.OperatorStateStore;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.operators.StreamingRuntimeContext;
import org.apache.flink.streaming.connectors.activemq.internal.IBMMQExceptionListener;
import org.apache.flink.streaming.connectors.activemq.internal.RunningChecker;
import org.apache.flink.streaming.connectors.ibmmq.DestinationType;
import org.apache.flink.streaming.connectors.ibmmq.IBMMQSource;
import org.apache.flink.streaming.connectors.ibmmq.IBMMQSourceConfig;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import com.ibm.mq.jms.MQConnectionFactory;

import scala.Array;

public class IBMMQSourceTest {

    private static final long CHECKPOINT_ID = 1;
    private final String DESTINATION_NAME = "queue";
    private final String MSG_ID = "msgId";

    private MQConnectionFactory connectionFactory;
    private Session session;
    private Connection connection;
    private Destination destination;
    private MessageConsumer consumer;
    private BytesMessage message;

    private IBMMQSource<String> amqSource;
    private SimpleStringSchema deserializationSchema;
    SourceFunction.SourceContext<String> context;

    @SuppressWarnings("unchecked")
    @BeforeEach
    public void before() throws Exception {
        connectionFactory = mock(MQConnectionFactory.class);
        session = mock(Session.class);
        connection = mock(Connection.class);
        destination = mock(Destination.class);
        consumer = mock(MessageConsumer.class);
        context = mock(SourceFunction.SourceContext.class);

        message = mock(BytesMessage.class);

        when(connectionFactory.createConnection()).thenReturn(connection);
        when(connection.createSession(anyBoolean(), anyInt())).thenReturn(session);
        when(consumer.receive(anyInt())).thenReturn(message);
        when(session.createConsumer(any(Destination.class))).thenReturn(consumer);
        when(context.getCheckpointLock()).thenReturn(new Object());
        when(message.getJMSMessageID()).thenReturn(MSG_ID);

        deserializationSchema = new SimpleStringSchema();
        IBMMQSourceConfig<String> config = new IBMMQSourceConfig.AMQSourceConfigBuilder<String>()
            .setConnectionFactory(connectionFactory)
            .setDestinationName(DESTINATION_NAME)
            .setDeserializationSchema(deserializationSchema)
            .setRunningChecker(new SingleLoopRunChecker())
            .build();
        amqSource = new IBMMQSource<>(config);
        amqSource.setRuntimeContext(createRuntimeContext());
        amqSource.open(new Configuration());
        amqSource.initializeState(new FunctionInitializationContext() {
            @Override
            public boolean isRestored() {
                return false;
            }

            @Override
            public OptionalLong getRestoredCheckpointId() {
                return mock(OptionalLong.class);
            }

            @Override
            public OperatorStateStore getOperatorStateStore() {
                return mock(OperatorStateStore.class);
            }

            @Override
            public KeyedStateStore getKeyedStateStore() {
                return mock(KeyedStateStore.class);
            }
        });
    }

    private RuntimeContext createRuntimeContext() {
        StreamingRuntimeContext runtimeContext = mock(StreamingRuntimeContext.class);
        when(runtimeContext.isCheckpointingEnabled()).thenReturn(true);
        return runtimeContext;
    }

    @Test
    public void readFromTopic() throws Exception {
        IBMMQSourceConfig<String> config = new IBMMQSourceConfig.AMQSourceConfigBuilder<String>()
            .setConnectionFactory(connectionFactory)
            .setDestinationName(DESTINATION_NAME)
            .setDeserializationSchema(deserializationSchema)
            .setDestinationType(DestinationType.TOPIC)
            .setRunningChecker(new SingleLoopRunChecker())
            .build();
        amqSource = new IBMMQSource<>(config);
        amqSource.setRuntimeContext(createRuntimeContext());
        amqSource.open(new Configuration());
        verify(session).createTopic(DESTINATION_NAME);
    }

    @Test
    public void parseReceivedMessage() throws Exception {
        final byte[] bytes = deserializationSchema.serialize("msg");
        when(message.getBodyLength()).thenReturn((long) bytes.length);
        when(message.readBytes(any(byte[].class))).thenAnswer(new Answer<Object>() {
            @Override
            public Object answer(InvocationOnMock invocationOnMock) throws Throwable {
                byte[] inputBytes = (byte[]) invocationOnMock.getArguments()[0];
                Array.copy(bytes, 0, inputBytes, 0, bytes.length);
                return null;
            }
        });

        amqSource.run(context);

        verify(context).collect("msg");
    }

    @Test
    public void acknowledgeReceivedMessage() throws Exception {
        amqSource.run(context);
        amqSource.acknowledgeIDs(CHECKPOINT_ID, Collections.singleton(MSG_ID));

        verify(message).acknowledge();
    }

    @Test
    public void handleUnknownIds() throws Exception {
        amqSource.run(context);
        amqSource.acknowledgeIDs(CHECKPOINT_ID, Collections.singleton("unknown-id"));

        verify(message, never()).acknowledge();
    }

    @Test
    public void doNotAcknowledgeMessageTwice() throws Exception {
        amqSource.run(context);
        amqSource.acknowledgeIDs(CHECKPOINT_ID, Collections.singleton(MSG_ID));
        amqSource.acknowledgeIDs(CHECKPOINT_ID, Collections.singleton(MSG_ID));

        verify(message, times(1)).acknowledge();
    }


    @Test
    public void propagateAsyncException() throws Exception {
        IBMMQExceptionListener exceptionListener = mock(IBMMQExceptionListener.class);
        amqSource.setExceptionListener(exceptionListener);
        doThrow(JMSException.class).when(exceptionListener).checkErroneous();

        Assertions.assertThrows(JMSException.class, () -> amqSource.run(context), "a exception is expected");

    }

    @Test
    public void throwAcknowledgeExceptionByDefault() throws Exception {
        doThrow(JMSException.class).when(message).acknowledge();

        amqSource.run(context);

        Assertions.assertThrows(RuntimeException.class, () -> amqSource.acknowledgeIDs(CHECKPOINT_ID, Collections.singleton(MSG_ID)), "a exception is expected");
    }


    @Test
    public void doNotThrowAcknowledgeExceptionByDefault() throws Exception {
        amqSource.setLogFailuresOnly(true);

        doThrow(JMSException.class).when(message).acknowledge();

        amqSource.run(context);
        amqSource.acknowledgeIDs(CHECKPOINT_ID, Collections.singleton(MSG_ID));
    }

    @Test
    public void closeResources() throws Exception {
        amqSource.close();

        verify(consumer).close();
        verify(session).close();
        verify(connection).close();
    }

    @Test
    public void consumerCloseExceptionShouldBePased() throws Exception {
        doThrow(new JMSException("consumer")).when(consumer).close();
        doThrow(new JMSException("session")).when(session).close();
        doThrow(new JMSException("connection")).when(connection).close();

        try {
            amqSource.close();
            fail("Should throw an exception");
        } catch (RuntimeException ex) {
            assertEquals("consumer", ex.getCause().getMessage());
        }
    }

    @Test
    public void sessionCloseExceptionShouldBePased() throws Exception {
        doThrow(new JMSException("session")).when(session).close();
        doThrow(new JMSException("connection")).when(connection).close();

        try {
            amqSource.close();
            fail("Should throw an exception");
        } catch (RuntimeException ex) {
            assertEquals("session", ex.getCause().getMessage());
        }
    }

    @Test
    public void connectionCloseExceptionShouldBePased() throws Exception {
        doThrow(new JMSException("connection")).when(connection).close();

        try {
            amqSource.close();
            fail("Should throw an exception");
        } catch (RuntimeException ex) {
            assertEquals("connection", ex.getCause().getMessage());
        }
    }

    @Test
    public void exceptionsShouldNotBePassedIfLogFailuresOnly() throws Exception {
        doThrow(new JMSException("consumer")).when(consumer).close();
        doThrow(new JMSException("session")).when(session).close();
        doThrow(new JMSException("connection")).when(connection).close();

        amqSource.setLogFailuresOnly(true);
        amqSource.close();
    }

    class SingleLoopRunChecker extends RunningChecker {

        int count = 0;

        @Override
        public boolean isRunning() {
            return (count++ == 0);
        }

        @Override
        public void setIsRunning(boolean isRunning) {

        }
    }
}
