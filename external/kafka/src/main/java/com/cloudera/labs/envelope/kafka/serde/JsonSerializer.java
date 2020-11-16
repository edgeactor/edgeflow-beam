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

package com.cloudera.labs.envelope.kafka.serde;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Maps;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.StructType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.Objects;

public class JsonSerializer implements Serializer<Row> {
    private final ObjectMapper objectMapper = new ObjectMapper();

    private final Logger log = LoggerFactory.getLogger(getClass());

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
    }

    @Override
    public byte[] serialize(String topic, Row row) {

        log.debug("start serialize");
        if (row == null) {
            return null;
        }
        StructType schema = row.schema();
        log.debug("row = " + row.toString());
        if (schema == null) {
            return null;
        }
        Map<String, Object> map = Maps.newHashMap();
        for (int i = 0; i < schema.fields().length; i++) {
            map.put(schema.fields()[i].name(), row.get(i));
        }
        log.debug("messageMap = " + map.toString());

        String message = null;

        try {
            message = objectMapper.writeValueAsString(map);
        } catch (JsonProcessingException e) {
            e.printStackTrace();
        }
        log.debug("message = " + message);

        return Objects.requireNonNull(message).getBytes();
    }

    @Override
    public void close() {

    }
}
