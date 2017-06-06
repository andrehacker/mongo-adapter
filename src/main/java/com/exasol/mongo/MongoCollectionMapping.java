package com.exasol.mongo;


import com.exasol.jsonpath.JsonPathElement;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

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

    public boolean hasListStar() {
        for (MongoColumnMapping col : getColumnMappings()) {
            if (col.getJsonPathParsed().stream().anyMatch(jsonPathElement -> jsonPathElement.getType() == JsonPathElement.Type.LIST_STAR)) {
                return true;
            }
        }
        return false;
    }

    public List<Integer> getColumnMappingsWithoutListStar() {
        List<Integer> indices = new ArrayList<>();
        int i = 0;
        for (MongoColumnMapping columnMapping : columnMappings) {
            if (! columnMapping.getJsonPathParsed().stream().anyMatch(jsonPathElement -> jsonPathElement.getType() == JsonPathElement.Type.LIST_STAR)) {
                indices.add(i);
            }
            i++;
        }
        return indices;
    }

    public List<Integer> getListStarColumnMappings() {
        List<Integer> indices = new ArrayList<>();
        int i = 0;
        for (MongoColumnMapping columnMapping : columnMappings) {
            if (columnMapping.getJsonPathParsed().stream().anyMatch(jsonPathElement -> jsonPathElement.getType() == JsonPathElement.Type.LIST_STAR)) {
                indices.add(i);
            }
            i++;
        }
        return indices;
    }
}
