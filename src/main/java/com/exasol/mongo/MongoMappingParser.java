package com.exasol.mongo;

import com.exasol.utils.JsonHelper;

import javax.json.Json;
import javax.json.JsonArray;
import javax.json.JsonObject;
import javax.json.JsonReader;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.List;

public class MongoMappingParser {

    // TODO parse jsonpath correctly, including $.: https://github.com/EXASOL/hadoop-etl-udfs/blob/master/src/main/java/com/exasol/jsonpath/JsonPathParser.java

    public static MongoDBMapping parse(String json) throws Exception {
        List<MongoCollectionMapping> mappings = new ArrayList<>();
        JsonObject root = JsonHelper.getJsonObject(json);
        for (JsonObject table : root.getJsonArray("tables").getValuesAs(JsonObject.class)) {
            String collectionName = table.getString("collectionName");
            String tableName = table.getString("tableName", collectionName);
            List<MongoColumnMapping> columnMappings = parseColumnMappings(table.getJsonArray("columns").getValuesAs(JsonObject.class));
            mappings.add(new MongoCollectionMapping(collectionName, tableName, columnMappings, table.getJsonArray("columns").toString()));
        }
        return new MongoDBMapping(mappings);
    }

    public static List<MongoColumnMapping> parseColumnMappings(String columnMappingsJson) throws Exception {
        return parseColumnMappings(getJsonArray(columnMappingsJson).getValuesAs(JsonObject.class));
    }

    public static List<MongoColumnMapping> parseColumnMappings(List<JsonObject> columns) {
        List<MongoColumnMapping> columnMappings = new ArrayList<>();
        for (JsonObject column : columns) {
            String jsonPath = column.getString("jsonpath");
            String columnName = column.getString("columnName", jsonPath);
            MongoColumnMapping.MongoType mongoType = MongoColumnMapping.mongoTypeFromString(column.getString("type"));
            columnMappings.add(new MongoColumnMapping(jsonPath, columnName, mongoType));
        }
        return columnMappings;
    }

    // TODO Make public available
    private static JsonArray getJsonArray(String data) throws Exception {
        JsonReader jr = Json.createReader(new StringReader(data));
        JsonArray obj = jr.readArray();
        jr.close();
        return obj;
    }

}
