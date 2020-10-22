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

import com.cloudera.labs.envelope.component.ComponentFactory;
import com.cloudera.labs.envelope.run.Runner;
import com.cloudera.labs.envelope.run.Step;
import com.cloudera.labs.envelope.utils.StepUtils;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import mockit.integration.junit4.JMockit;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.Set;

@RunWith(JMockit.class)
public class DummyStreamInputTest {

    private static final String PIPELINE_CONF = "/structured-streaming.conf";
    private final Logger log = LoggerFactory.getLogger(getClass());

    @Test
    public void testInput() throws Exception {
        DummyStreamInput input = new DummyStreamInput();
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
//        runner.run(config);
        runner.validateConfigurations(config);
    }

    @Test
    public void testExtractPipelines() throws Exception {

        String pipelinePath = getClass().getResource(PIPELINE_CONF).getPath();
        Config config = ConfigFactory.parseFile(new File(pipelinePath));
        Set<Step> steps = StepUtils.extractSteps(config, true, true);
        log.info(steps.toString());
    }

    @Test
    public void testComponent() {
        String pipelinePath = getClass().getResource(PIPELINE_CONF).getPath();
        Config config = ConfigFactory.parseFile(new File(pipelinePath));
        Config inputConfig = config.getConfig("step1");
//        ComponentFactory.create(DummyStreamInput.class, inputConfig)
    }

}
