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

import static org.apache.flink.test.util.TestUtils.tryExecute;
import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.HashSet;
import java.util.Random;

import javax.jms.JMSException;

import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.TaskManagerOptions;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.operators.StreamingRuntimeContext;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.test.util.SuccessException;
import org.junit.BeforeClass;
import org.junit.Test;

import com.ibm.mq.jms.MQConnectionFactory;
import com.ibm.msg.client.wmq.WMQConstants;

import scala.concurrent.duration.Deadline;
import scala.concurrent.duration.FiniteDuration;

public class IBMMQConnectorITCase {

    public static final int MESSAGES_NUM = 10000;
    public static final String QUEUE_NAME = "queue";
    public static final String TOPIC_NAME = "topic";

    private static Configuration flinkConfig;

    @BeforeClass
    public static void beforeClass() {
        // start also a re-usable Flink mini cluster
        flinkConfig = new Configuration();
        flinkConfig.setInteger(ConfigConstants.LOCAL_NUMBER_TASK_MANAGER, 1);
        flinkConfig.setInteger(TaskManagerOptions.NUM_TASK_SLOTS.key(), 8);
        flinkConfig.setInteger(TaskManagerOptions.MANAGED_MEMORY_SIZE.key(), 16);

    }

    @Test
    public void amqTopologyWithQueue() throws Exception {
        StreamExecutionEnvironment env = createExecutionEnvironment();

        IBMMQSinkConfig<String> sinkConfig = new IBMMQSinkConfig.AMQSinkConfigBuilder<String>()
            .setConnectionFactory(createConnectionFactory())
            .setDestinationName(QUEUE_NAME)
            .setSerializationSchema(new SimpleStringSchema())
            .build();
        createProducerTopology(env, sinkConfig);

        MQConnectionFactory sourceConnectionFactory = createConnectionFactory();
        IBMMQSourceConfig<String> sourceConfig = new IBMMQSourceConfig.AMQSourceConfigBuilder<String>()
            .setConnectionFactory(sourceConnectionFactory)
            .setDestinationName(QUEUE_NAME)
            .setDeserializationSchema(new SimpleStringSchema())
            .build();
        createConsumerTopology(env, sourceConfig);

        tryExecute(env, "AMQTest");
    }

    @Test
    public void amqTopologyWithTopic() throws Exception {
        StreamExecutionEnvironment env = createExecutionEnvironment();
        IBMMQSinkConfig<String> sinkConfig = new IBMMQSinkConfig.AMQSinkConfigBuilder<String>()
            .setConnectionFactory(createConnectionFactory())
            .setDestinationName(TOPIC_NAME)
            .setSerializationSchema(new SimpleStringSchema())
            .setDestinationType(DestinationType.TOPIC)
            .build();
        createProducerTopology(env, sinkConfig);

        MQConnectionFactory sourceConnectionFactory = createConnectionFactory();
        IBMMQSourceConfig<String> sourceConfig = new IBMMQSourceConfig.AMQSourceConfigBuilder<String>()
            .setConnectionFactory(sourceConnectionFactory)
            .setDestinationName(TOPIC_NAME)
            .setDeserializationSchema(new SimpleStringSchema())
            .setDestinationType(DestinationType.TOPIC)
            .build();
        createConsumerTopology(env, sourceConfig);

        tryExecute(env, "AMQTest");
    }

    private StreamExecutionEnvironment createExecutionEnvironment() {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(flinkConfig);
        env.setRestartStrategy(RestartStrategies.noRestart());
        return env;
    }

    private MQConnectionFactory createConnectionFactory() {
    	try {
	    	MQConnectionFactory mqcf = new MQConnectionFactory();
	    	mqcf.setHostName("localhost");
	    	mqcf.setPort(1414);
	    	mqcf.setChannel("CHANNEL1");
	    	mqcf.setTransportType(WMQConstants.WMQ_CM_CLIENT);
	        return mqcf;
    	}
    	catch (JMSException exc) {
    		throw new RuntimeException(exc);
    	}
    }

    private void createProducerTopology(StreamExecutionEnvironment env, IBMMQSinkConfig<String> config) {
        DataStreamSource<String> stream = env.addSource(new SourceFunction<String>() {
            @Override
            public void run(SourceContext<String> ctx) throws Exception {
                for (int i = 0; i < MESSAGES_NUM; i++) {
                    ctx.collect("amq-" + i);
                }
            }

            @Override
            public void cancel() {}
        });


        IBMMQSink<String> sink = new IBMMQSink<>(config);
        stream.addSink(sink);
    }

    private void createConsumerTopology(StreamExecutionEnvironment env, IBMMQSourceConfig<String> config) {
        IBMMQSource<String> source = new IBMMQSource<>(config);

        env.addSource(source)
            .addSink(new SinkFunction<String>() {
                final HashSet<Integer> set = new HashSet<>();
                @Override
                public void invoke(String value, Context context) throws Exception {
                    int val = Integer.parseInt(value.split("-")[1]);
                    set.add(val);

                    if (set.size() == MESSAGES_NUM) {
                        throw new SuccessException();
                    }
                }
            });
    }

    @Test
    public void amqTopologyWithCheckpointing() throws Exception {
        MQConnectionFactory connectionFactory = createConnectionFactory();
        IBMMQSinkConfig<String> sinkConfig = new IBMMQSinkConfig.AMQSinkConfigBuilder<String>()
            .setConnectionFactory(connectionFactory)
            .setDestinationName("queue2")
            .setSerializationSchema(new SimpleStringSchema())
            .build();
        IBMMQSink<String> sink = new IBMMQSink<>(sinkConfig);
        sink.open(new Configuration());

        for (int i = 0; i < MESSAGES_NUM; i++) {
            sink.invoke("amq-" + i, null);
        }

        IBMMQSourceConfig<String> sourceConfig = new IBMMQSourceConfig.AMQSourceConfigBuilder<String>()
            .setConnectionFactory(connectionFactory)
            .setDestinationName("queue2")
            .setDeserializationSchema(new SimpleStringSchema())
            .build();

        final IBMMQSource<String> source = new IBMMQSource<>(sourceConfig);
        RuntimeContext runtimeContext = createMockRuntimeContext();
        source.setRuntimeContext(runtimeContext);
        source.open(new Configuration());

        final TestSourceContext sourceContext = new TestSourceContext();
        Thread thread = new Thread(new Runnable() {
            @Override
            public void run() {
                try {
                    source.run(sourceContext);
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        });
        thread.start();

        Deadline deadline = FiniteDuration.apply(5, "s").fromNow();
        while (deadline.hasTimeLeft() && sourceContext.getIdsNum() < MESSAGES_NUM) {
            Thread.sleep(100);
            Random random = new Random();
            final long checkpointId = random.nextLong();
            synchronized (sourceContext.getCheckpointLock()) {
                source.snapshotState(new FunctionSnapshotContext() {
                    @Override
                    public long getCheckpointId() {
                        return checkpointId;
                    }

                    @Override
                    public long getCheckpointTimestamp() {
                        return System.currentTimeMillis();
                    }
                });
                source.notifyCheckpointComplete(checkpointId);
            }
        }
        assertEquals(MESSAGES_NUM, sourceContext.getIdsNum());
    }

    private RuntimeContext createMockRuntimeContext() {
        StreamingRuntimeContext runtimeContext = mock(StreamingRuntimeContext.class);
        when(runtimeContext.isCheckpointingEnabled()).thenReturn(true);
        return runtimeContext;
    }

    class TestSourceContext implements SourceFunction.SourceContext<String> {

        private HashSet<Integer> ids = new HashSet<>();
        private Object contextLock = new Object();
        @Override
        public void collect(String value) {
            int val = Integer.parseInt(value.split("-")[1]);
            ids.add(val);
        }

        @Override
        public void collectWithTimestamp(String element, long timestamp) { }

        @Override
        public void emitWatermark(Watermark mark) { }

        @Override
        public void markAsTemporarilyIdle() {
            throw new UnsupportedOperationException();
        }

        @Override
        public Object getCheckpointLock() {
            return contextLock;
        }

        @Override
        public void close() { }

        public int getIdsNum() {
            synchronized (contextLock) {
                return ids.size();
            }
        }
    };
}
