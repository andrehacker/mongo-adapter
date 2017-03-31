package com.exasol.mongo;


import java.util.List;

public class MongoCollectionMapping {

    private String collectionName;
    private String tableName;
    private List<MongoColumnMapping> columnMappings;
    private String jsonMappingSpec;

    public MongoCollectionMapping(String collectionName, String tableName, List<MongoColumnMapping> columnMappings, String jsonMappingSpec) {
        this.collectionName = collectionName;
        this.tableName = tableName;
        this.columnMappings = columnMappings;
        this.jsonMappingSpec = jsonMappingSpec;
    }

    public String getCollectionName() {
        return collectionName;
    }

    public String getTableName() {
        return tableName;
    }

    public List<MongoColumnMapping> getColumnMappings() {
        return columnMappings;
    }

    public String getJsonMappingSpec() {
        return jsonMappingSpec;
    }
}
