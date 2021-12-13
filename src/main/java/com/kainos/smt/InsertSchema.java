package com.kainos.smt;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.transforms.Transformation;

import java.util.Map;

import static org.apache.kafka.connect.transforms.util.Requirements.requireMap;

public class InsertSchema<R extends ConnectRecord<R>> implements Transformation<R> {

    public static final String OVERVIEW_DOC = "Insert schema to the payload";
    private static final String PURPOSE = "adding schema to record";
    public static final ConfigDef CONFIG_DEF = new ConfigDef();
    @Override
    public void configure(Map<String, ?> props) {
    }

    @Override
    public ConfigDef config() {
        return CONFIG_DEF;
    }

    @Override
    public void close() {
    }

    @Override
    public R apply(R record) {
        final Map<String,?> value = requireMap(record.value(),PURPOSE);
        Schema updatedSchema = makeUpdatedSchema();

        final Struct updatedValue = new Struct(updatedSchema);


        for (Field field : updatedSchema.fields()) {

            updatedValue.put(field.name(), value.get(field.name()));
        }


        return newRecord(record, updatedSchema, updatedValue);
    }

    private Schema makeUpdatedSchema() {
        final SchemaBuilder builder = SchemaBuilder.struct()
                .name("json_schema")
                .field("first",Schema.INT64_SCHEMA)
                .field("second",Schema.STRING_SCHEMA);

        return builder.build();
    }

    protected R newRecord(R record, Schema updatedSchema, Struct updatedValue) {
        return record.newRecord(record.topic(), record.kafkaPartition(), record.keySchema(), record.key(), updatedSchema, updatedValue, record.timestamp());
    }
}
