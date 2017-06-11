package com.exasol.mongo;

import com.exasol.adapter.AdapterException;
import com.exasol.utils.JsonHelper;

import javax.json.Json;
import javax.json.JsonArray;
import javax.json.JsonObject;
import javax.json.JsonReader;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.List;

public class MongoMappingParser {

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
            throw new AdapterException("Could not parse mapping: " + ex.getMessage());
        }
        return new MongoDBMapping(mappings);
    }

    public static List<MongoColumnMapping> parseColumnMappings(List<JsonObject> columns) throws AdapterException {
        List<MongoColumnMapping> columnMappings = new ArrayList<>();
        for (JsonObject column : columns) {
            String jsonPath = column.getString("jsonpath");
            String columnName = column.getString("columnName", jsonPath);
            MongoColumnMapping.MongoType mongoType = MongoColumnMapping.MongoType.fromString(column.getString("type"));
            columnMappings.add(new MongoColumnMapping(jsonPath, columnName, mongoType));
        }
        return columnMappings;
    }

}
