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

import com.cloudera.labs.envelope.component.ProvidesAlias;
import com.cloudera.labs.envelope.input.StructuredStreamingInput;
import com.cloudera.labs.envelope.validate.ProvidesValidations;
import com.cloudera.labs.envelope.validate.Validations;
import com.typesafe.config.Config;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.StructType;

public class KafkaStreamInput implements StructuredStreamingInput, ProvidesAlias, ProvidesValidations {
    @Override
    public Dataset<Row> read() throws Exception {
        return null;
    }

    @Override
    public void configure(Config config) {

    }

    @Override
    public String getAlias() {
        return null;
    }

    @Override
    public StructType getProvidingSchema() {
        return null;
    }

    @Override
    public Validations getValidations() {
        return null;
    }
}
