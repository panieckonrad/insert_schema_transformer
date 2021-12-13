package com.kainos.smt;

import io.confluent.connect.avro.AvroData;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.transforms.Transformation;
import org.apache.kafka.connect.transforms.util.SimpleConfig;

import java.io.File;
import java.io.IOException;
import java.util.Map;

import static io.confluent.connect.avro.AvroDataConfig.SCHEMAS_CACHE_SIZE_DEFAULT;
import static org.apache.kafka.connect.transforms.util.Requirements.requireMap;

public class InsertSchema<R extends ConnectRecord<R>> implements Transformation<R> {
    public static final String OVERVIEW_DOC = "Insert schema to the payload";
    private static final String PURPOSE = "adding schema to record";

    private final AvroData avroData = new AvroData(SCHEMAS_CACHE_SIZE_DEFAULT);

    private interface ConfigName {
        String AVRO_SCHEMA_PATH = "avro.schema.path";
    }

    public static final ConfigDef CONFIG_DEF = new ConfigDef()
            .define(ConfigName.AVRO_SCHEMA_PATH, ConfigDef.Type.STRING, "", ConfigDef.Importance.HIGH,
                    "path to avsc file holding schema information about the records");
    private String avroSchemaPath;

    @Override
    public void configure(Map<String, ?> props) {
        final SimpleConfig config = new SimpleConfig(CONFIG_DEF, props);
        avroSchemaPath = config.getString(ConfigName.AVRO_SCHEMA_PATH);
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
        final Map<String, ?> value = requireMap(record.value(), PURPOSE);
        Schema updatedSchema = convertAvroSchemaToConnectSchema(avroSchemaPath);

        final Struct updatedValue = new Struct(updatedSchema);


        for (Field field : updatedSchema.fields()) {

            updatedValue.put(field.name(), value.get(field.name()));
        }


        return newRecord(record, updatedSchema, updatedValue);
    }

    private Schema convertAvroSchemaToConnectSchema(String pathToAvscFile) {
        Schema schema = null;
        try {
            schema = tryToConvertAvroSchemaToConnectSchema(pathToAvscFile);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        return schema;
    }

    private Schema tryToConvertAvroSchemaToConnectSchema(String pathToAvscFile) throws IOException {
        org.apache.avro.Schema schema = new org.apache.avro.Schema.Parser().parse(
                new File(pathToAvscFile)
        );
        return avroData.toConnectSchema(schema);
    }

    protected R newRecord(R record, Schema updatedSchema, Struct updatedValue) {
        return record.newRecord(record.topic(), record.kafkaPartition(), record.keySchema(), record.key(), updatedSchema, updatedValue, record.timestamp());
    }
}
