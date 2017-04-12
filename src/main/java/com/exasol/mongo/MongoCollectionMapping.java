package com.exasol.mongo;


import java.util.List;

public class MongoCollectionMapping {

    private String collectionName;
    private String tableName;
    private List<MongoColumnMapping> columnMappings;

    public MongoCollectionMapping(String collectionName, String tableName, List<MongoColumnMapping> columnMappings) {
        this.collectionName = collectionName;
        this.tableName = tableName;
        this.columnMappings = columnMappings;
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
}
