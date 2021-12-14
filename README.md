Kafka Connect SMT to transform schemaless JSON records to records with schema (useful for creating avro/parquet files from JSON)

This SMT supports inserting a schema into the record Value </br>
Properties:

|Name|Description|Type|Default|Importance|
|---|---|---|---|---|
|`avro.schema.path`| path to .avsc file containing schema for records | String | `""` | High |

Example on how to add to your connector:
```
      "transforms":"insertschema",
      "transforms.insertschema.type":"com.kainos.smt.InsertSchema",
      "transforms.insertschema.avro.schema.path":"/path/to/file.avsc"
```

You need to store the schema file somewhere on your Kafka Connect instances