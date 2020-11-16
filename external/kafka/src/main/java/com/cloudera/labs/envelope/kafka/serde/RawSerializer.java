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

import org.apache.kafka.common.serialization.Serializer;
import org.apache.spark.sql.Row;

import java.util.Map;

public class RawSerializer implements Serializer<Row> {
    private String rawField = "raw";
    public static final String RAW_FIELD = "raw.field";

    @Override
    public void configure(Map<String, ?> map, boolean b) {

        if (map.containsKey(RAW_FIELD)) {
            rawField = map.get(RAW_FIELD).toString();
        }
    }

    @Override
    public byte[] serialize(String topic, Row row) {
        if (row == null) {
            return null;
        }
        String rawString = row.<String>getAs(rawField);
        if (rawString != null) {
            return rawString.getBytes();
        }
        return new byte[0];
    }

    @Override
    public void close() {

    }
}
