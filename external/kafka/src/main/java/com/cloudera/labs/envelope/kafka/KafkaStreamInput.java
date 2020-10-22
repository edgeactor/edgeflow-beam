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
