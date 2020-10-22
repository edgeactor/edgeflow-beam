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

import com.cloudera.labs.envelope.run.Runner;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import mockit.integration.junit4.JMockit;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.io.File;

@RunWith(JMockit.class)
public class FilesystemStreamInputTest {

    private static final String PIPELINE_CONF = "/structured-streaming.conf";

    @Test
    public void testInput() throws Exception {
        FilesystemStreamInput input = new FilesystemStreamInput();
        Config config = ConfigFactory.empty();
        input.configure(config);
        Dataset<Row> df = input.read();

        StreamingQuery query = df.writeStream().start("file:///tmp/test");

        query.awaitTermination();
    }

    @Test
    public void testPipeline() throws Exception {

        String pipelinePath = getClass().getResource(PIPELINE_CONF).getPath();
        Runner runner = new Runner();
        Config config = ConfigFactory.parseFile(new File(pipelinePath));
        runner.run(config);
    }
}
