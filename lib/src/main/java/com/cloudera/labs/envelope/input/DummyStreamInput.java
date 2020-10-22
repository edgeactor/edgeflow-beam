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

package com.cloudera.labs.envelope.input;

import com.cloudera.labs.envelope.component.ProvidesAlias;
import com.cloudera.labs.envelope.spark.Contexts;
import com.cloudera.labs.envelope.validate.ProvidesValidations;
import com.cloudera.labs.envelope.validate.Validations;
import com.typesafe.config.Config;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.StructType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DummyStreamInput implements StructuredStreamingInput,
        ProvidesValidations, ProvidesAlias {

    private static final String CSV_DATA = "file:///Codes/edgeflow-core/lib/src/test/resources/filesystem/streaming";
    private final Logger log = LoggerFactory.getLogger(getClass());

    @Override
    public Dataset<Row> read() throws Exception {

//        String csvPath = getClass().getResource(CSV_DATA).getPath();
        String csvPath = CSV_DATA;
        log.info("CSV Path = " + csvPath);

        Contexts.getSparkSession().conf()
                .set("spark.sql.streaming.checkpointLocation", "file:///tmp/checkpoints");
        // One,Two,Three,Four
        Dataset<Row> df = Contexts
                .getSparkSession()
                .readStream()
                .schema(getProvidingSchema())
                .format("csv")
                .option("sep", ",")
                .load(csvPath);
        return df;
    }

    @Override
    public void configure(Config config) {

    }

    @Override
    public String getAlias() {
        return "dummy-stream";
    }

    @Override
    public StructType getProvidingSchema() {

        StructType schema = new StructType().add("N1", "int")
                .add("N2", "int")
                .add("S1", "string")
                .add("S2", "string");
        return schema;
    }

    @Override
    public Validations getValidations() {
        return Validations.builder().build();
    }
}
