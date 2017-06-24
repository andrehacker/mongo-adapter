package com.exasol.mongo.mapping;

import com.exasol.adapter.AdapterException;
import com.exasol.utils.JsonHelper;
import com.exasol.utils.UdfUtils;

import javax.json.JsonObject;
import java.util.ArrayList;
import java.util.List;

public class MongoDBMappingParser {

    public static MongoDBMapping parse(String json) throws AdapterException {
        List<MongoCollectionMapping> mappings = new ArrayList<>();
        JsonObject root;
        try {
            root = JsonHelper.getJsonObject(json);
            for (JsonObject table : root.getJsonArray("tables").getValuesAs(JsonObject.class)) {
                String collectionName = table.getString("collectionName");
                String tableName = table.getString("tableName", collectionName);
                List<MongoColumnMapping> columnMappings = parseColumnMappings(table.getJsonArray("columns").getValuesAs(JsonObject.class));
                mappings.add(new MongoCollectionMapping(collectionName, tableName, columnMappings));
            }
        } catch (Exception ex) {
            throw new AdapterException("Could not parse mapping: " + ex.getMessage() + "\nStacktrace:\n" + UdfUtils.traceToString(ex));
        }
        return new MongoDBMapping(mappings);
    }

    public static List<MongoColumnMapping> parseColumnMappings(List<JsonObject> columns) throws AdapterException {
        List<MongoColumnMapping> columnMappings = new ArrayList<>();
        for (JsonObject column : columns) {
            String jsonPath = column.getString("jsonPath");
            String columnName = column.getString("columnName", jsonPath);
            MongoColumnMapping.MongoType mongoType = MongoColumnMapping.MongoType.fromString(column.getString("mongoType"));
            columnMappings.add(new MongoColumnMapping(jsonPath, columnName, mongoType));
        }
        return columnMappings;
    }

}
