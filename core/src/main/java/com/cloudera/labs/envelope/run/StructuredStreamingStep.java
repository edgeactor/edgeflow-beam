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

package com.cloudera.labs.envelope.run;

import com.cloudera.labs.envelope.component.InstantiatesComponents;
import com.cloudera.labs.envelope.input.CanRecordProgress;
import com.cloudera.labs.envelope.input.StructuredStreamingInput;
import com.cloudera.labs.envelope.validate.ProvidesValidations;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import java.util.Iterator;

/**
 * Spark Structured Streaming
 */
public class StructuredStreamingStep extends DataStep implements CanRecordProgress, ProvidesValidations, InstantiatesComponents {

    public StructuredStreamingStep(String name) {
        super(name);
    }

    public Dataset<Row> getStream() throws Exception {
        Dataset<Row> stream = ((StructuredStreamingInput) getInput(true)).read();
//        stream.writeStream().
        return stream;
    }

    @Override
    public void recordProgress(JavaRDD<?> batch) throws Exception {

    }

    public Dataset<Row> translate(Iterator<Row> raw) {
//        Stream<Row> stream = Stream
//        Contexts.getSparkSession()
//                .createDataFrame(raw)
        return null;
    }

    @Override
    public Step copy() {
        StructuredStreamingStep copy = new StructuredStreamingStep(name);
        copy.configure(config);

        copy.setDependencyNames(getDependencyNames());
        copy.setState(getState());

        if (getState() == StepState.FINISHED) {
            copy.setData(getData());
        }

        return copy;
    }
}
