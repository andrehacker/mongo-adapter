package com.exasol.mongo.mapping;

import com.exasol.utils.JsonHelper;

import javax.json.JsonArrayBuilder;
import javax.json.JsonBuilderFactory;
import javax.json.JsonObjectBuilder;

public class MongoDBMappingSerializer {

    public static String serialize(MongoDBMapping mongoDBMapping) {
        JsonBuilderFactory factory = JsonHelper.getBuilderFactory();
        JsonObjectBuilder builder = factory.createObjectBuilder();
        JsonArrayBuilder tablesBuilder = factory.createArrayBuilder();
        for (MongoCollectionMapping collectionMapping : mongoDBMapping.getCollectionMappings()) {
            tablesBuilder.add(serializeCollectionMapping(collectionMapping, factory));
        }
        builder.add("tables", tablesBuilder);
        return builder.build().toString();
    }

    private static JsonObjectBuilder serializeCollectionMapping(MongoCollectionMapping collectionMapping, JsonBuilderFactory factory) {
        JsonObjectBuilder builder = factory.createObjectBuilder();
        builder.add("collectionName", collectionMapping.getCollectionName());
        builder.add("tableName", collectionMapping.getTableName());
        JsonArrayBuilder columnsBuilder = factory.createArrayBuilder();
        for (MongoColumnMapping columnMapping : collectionMapping.getColumnMappings()) {
            columnsBuilder.add(serializeColumnMapping(columnMapping, factory));
        }
        builder.add("columns", columnsBuilder);
        return builder;
    }

    private static JsonObjectBuilder serializeColumnMapping(MongoColumnMapping columnMapping, JsonBuilderFactory factory) {
        JsonObjectBuilder builder = factory.createObjectBuilder();
        builder.add("jsonPath", columnMapping.getJsonPath());
        builder.add("columnName", columnMapping.getColumnName());
        builder.add("mongoType", columnMapping.getMongoType().toString());
        return builder;
    }
}
