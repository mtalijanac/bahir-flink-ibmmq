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

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.streaming.connectors.activemq.internal.RunningChecker;
import org.apache.flink.util.Preconditions;

import com.ibm.mq.jms.MQConnectionFactory;

/**
 * Immutable AMQ source config.
 *
 * @param <OUT> type of source output messages
 */
public class IBMMQSourceConfig<OUT> {

    private final MQConnectionFactory connectionFactory;
    private final String destinationName;
    private final DeserializationSchema<OUT> deserializationSchema;
    private final RunningChecker runningChecker;
    private final DestinationType destinationType;

    IBMMQSourceConfig(MQConnectionFactory connectionFactory, String destinationName,
                    DeserializationSchema<OUT> deserializationSchema, RunningChecker runningChecker,
                    DestinationType destinationType) {
        this.connectionFactory = Preconditions.checkNotNull(connectionFactory, "connectionFactory not set");
        this.destinationName = Preconditions.checkNotNull(destinationName, "destinationName not set");
        this.deserializationSchema = Preconditions.checkNotNull(deserializationSchema, "deserializationSchema not set");
        this.runningChecker = Preconditions.checkNotNull(runningChecker, "runningChecker not set");
        this.destinationType = Preconditions.checkNotNull(destinationType, "destinationType not set");
    }

    public MQConnectionFactory getConnectionFactory() {
        return connectionFactory;
    }

    public String getDestinationName() {
        return destinationName;
    }

    public DeserializationSchema<OUT> getDeserializationSchema() {
        return deserializationSchema;
    }

    public RunningChecker getRunningChecker() {
        return runningChecker;
    }

    public DestinationType getDestinationType() {
        return destinationType;
    }

    /**
     * Builder for {@link IBMMQSourceConfig}
     *
     * @param <OUT> type of source output messages
     */
    public static class AMQSourceConfigBuilder<OUT> {
        private MQConnectionFactory connectionFactory;
        private String destinationName;
        private DeserializationSchema<OUT> deserializationSchema;
        private RunningChecker runningChecker = new RunningChecker();
        private DestinationType destinationType = DestinationType.QUEUE;

        public AMQSourceConfigBuilder<OUT> setConnectionFactory(MQConnectionFactory connectionFactory) {
            this.connectionFactory = Preconditions.checkNotNull(connectionFactory);
            return this;
        }

        public AMQSourceConfigBuilder<OUT> setDestinationName(String destinationName) {
            this.destinationName = Preconditions.checkNotNull(destinationName);
            return this;
        }

        public AMQSourceConfigBuilder<OUT> setDeserializationSchema(DeserializationSchema<OUT> deserializationSchema) {
            this.deserializationSchema = Preconditions.checkNotNull(deserializationSchema);
            return this;
        }

        public AMQSourceConfigBuilder<OUT> setRunningChecker(RunningChecker runningChecker) {
            this.runningChecker = Preconditions.checkNotNull(runningChecker);
            return this;
        }

        public AMQSourceConfigBuilder<OUT> setDestinationType(DestinationType destinationType) {
            this.destinationType = Preconditions.checkNotNull(destinationType);
            return this;
        }

        public IBMMQSourceConfig<OUT> build() {
            return new IBMMQSourceConfig<OUT>(connectionFactory, destinationName, deserializationSchema, runningChecker, destinationType);
        }

    }
}
