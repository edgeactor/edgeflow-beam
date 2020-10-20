/*
 * Copyright (c) 2015-2020, Cloudera, Inc. All Rights Reserved.
 *
 * Cloudera, Inc. licenses this file to you under the Apache License,
 * Version 2.0 (the "License"). You may not use this file except in
 * compliance with the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * This software is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied. See the License for
 * the specific language governing permissions and limitations under the
 * License.
 */

package com.cloudera.labs.envelope.kafka;

import com.cloudera.labs.envelope.component.ComponentFactory;
import com.cloudera.labs.envelope.kafka.serde.AvroSerializer;
import com.cloudera.labs.envelope.kafka.serde.DelimitedSerializer;
import com.cloudera.labs.envelope.component.ProvidesAlias;
import com.cloudera.labs.envelope.kafka.serde.JsonSerializer;
import com.cloudera.labs.envelope.output.BulkOutput;
import com.cloudera.labs.envelope.plan.MutationType;
import com.cloudera.labs.envelope.schema.Schema;
import com.cloudera.labs.envelope.validate.ProvidesValidations;
import com.cloudera.labs.envelope.validate.SupportedFieldTypesValidation;
import com.cloudera.labs.envelope.validate.Validations;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigValue;
import com.typesafe.config.ConfigValueType;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.DecimalType;
import org.apache.spark.sql.types.StructType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;

import java.util.*;

import static com.cloudera.labs.envelope.kafka.serde.JsonSerializer.JSON_SCHEMA_CONFIG;

public class KafkaOutput implements BulkOutput, ProvidesAlias, ProvidesValidations {

    public static final String BROKERS_CONFIG_NAME = "brokers";
    public static final String TOPIC_CONFIG_NAME = "topic";
    public static final String SERIALIZER_CONFIG_PREFIX = "serializer.";
    public static final String SERIALIZER_TYPE_CONFIG_NAME = SERIALIZER_CONFIG_PREFIX + "type";
    public static final String DELIMITED_SERIALIZER = "delimited";
    public static final String AVRO_SERIALIZER = "avro";
    public static final String JSON_SERIALIZER = "json";
    public static final String TIMESTAMP_FORMAT_CONFIG_NAME = "timestamp.formats";

    private static Logger LOG = LoggerFactory.getLogger(KafkaOutput.class);

    private Config config;

    @Override
    public void configure(Config config) {
        this.config = config;
    }

    @Override
    public void applyBulkMutations(List<Tuple2<MutationType, Dataset<Row>>> planned) {
        for (Tuple2<MutationType, Dataset<Row>> mutation : planned) {
            MutationType mutationType = mutation._1();
            Dataset<Row> mutationDF = mutation._2();

            if (mutationType.equals(MutationType.INSERT)) {
                mutationDF.javaRDD().foreachPartition(new SendRowToKafkaFunction(config));
            }
        }
    }

    @Override
    public Set<MutationType> getSupportedBulkMutationTypes() {
        return Sets.newHashSet(MutationType.INSERT);
    }

    @Override
    public String getAlias() {
        return "kafka";
    }

    @Override
    public Validations getValidations() {
        return Validations.builder()
                .mandatoryPath(BROKERS_CONFIG_NAME, ConfigValueType.STRING)
                .mandatoryPath(TOPIC_CONFIG_NAME, ConfigValueType.STRING)
                .mandatoryPath(SERIALIZER_TYPE_CONFIG_NAME, ConfigValueType.STRING)
                .allowedValues(SERIALIZER_TYPE_CONFIG_NAME, DELIMITED_SERIALIZER, AVRO_SERIALIZER, JSON_SERIALIZER)
                .handlesOwnValidationPath(SERIALIZER_CONFIG_PREFIX)
                .handlesOwnValidationPath(KafkaCommon.PARAMETER_CONFIG_PREFIX)
                .optionalPath(TIMESTAMP_FORMAT_CONFIG_NAME, ConfigValueType.LIST)
                .build();
    }

    @SuppressWarnings("serial")
    private static class SendRowToKafkaFunction implements VoidFunction<Iterator<Row>> {
        private KafkaProducer<Row, Row> producer;
        private String topic;
        private String brokers;
        private String serializerType;
        private Config config;

        public SendRowToKafkaFunction(Config config) {
            this.brokers = config.getString(BROKERS_CONFIG_NAME);
            this.topic = config.getString(TOPIC_CONFIG_NAME);
            this.serializerType = config.getString(SERIALIZER_TYPE_CONFIG_NAME);
            this.config = config;
        }

        private void initialize() {
            Serializer<Row> keySerializer, valueSerializer;
            switch (serializerType) {
                case DELIMITED_SERIALIZER:
                    keySerializer = new DelimitedSerializer();
                    valueSerializer = new DelimitedSerializer();
                    break;
                case AVRO_SERIALIZER:
                    keySerializer = new AvroSerializer();
                    valueSerializer = new AvroSerializer();
                    break;
                case JSON_SERIALIZER:
                    keySerializer = new JsonSerializer();
                    valueSerializer = new JsonSerializer();
                    break;
                default:
                    throw new RuntimeException("Kafka output does not support serializer type: " + serializerType);
            }

            Map<String, ?> serializerConfiguration = getSerializerConfiguration();
            keySerializer.configure(serializerConfiguration, true);
            valueSerializer.configure(serializerConfiguration, false);

            Map<String, Object> producerProps = Maps.newHashMap();
            producerProps.put("bootstrap.servers", brokers);

            KafkaCommon.addCustomParams(producerProps, config);

            producer = new KafkaProducer<>(producerProps, keySerializer, valueSerializer);

            LOG.info("Producer initialized");
        }

        @Override
        public void call(Iterator<Row> mutations) throws Exception {
            initialize();

            while (mutations.hasNext()) {
                Row mutation = mutations.next();

                producer.send(new ProducerRecord<Row, Row>(topic, mutation));
            }
            LOG.info("Finished sending messages");

            // This will block until all mutations have been acked by Kafka
            producer.flush();
            LOG.info("Producer flushed");

            producer.close();
            LOG.info("Producer closed");
        }

        private Map<String, ?> getSerializerConfiguration() {
            Map<String, Object> configs = Maps.newHashMap();

            for (Map.Entry<String, ConfigValue> entry : config.entrySet()) {
                String propertyName = entry.getKey();
                if (propertyName.startsWith(SERIALIZER_CONFIG_PREFIX)) {
                    String paramName = propertyName.substring(SERIALIZER_CONFIG_PREFIX.length());
                    String paramValue = config.getString(propertyName);

                    configs.put(paramName, paramValue);
                }
            }

            return configs;
        }
    }

}
