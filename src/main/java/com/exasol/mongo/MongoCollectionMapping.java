package com.exasol.mongo;


import com.exasol.adapter.sql.SqlColumn;
import com.exasol.adapter.sql.SqlNode;
import com.exasol.adapter.sql.SqlSelectList;
import com.exasol.jsonpath.JsonPathElement;

import java.util.ArrayList;
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

    public boolean hasListWildcard() {
        for (MongoColumnMapping col : getColumnMappings()) {
            if (col.getJsonPathParsed().stream().anyMatch(jsonPathElement -> jsonPathElement.getType() == JsonPathElement.Type.LIST_WILDCARD)) {
                return true;
            }
        }
        return false;
    }

    public List<Integer> getColumnIndicesWithoutListWildcard(SqlSelectList selectList) {
        List<Integer> indices = new ArrayList<>();
        if (selectList.isSelectStar()) {
            int i = 0;
            for (MongoColumnMapping columnMapping : columnMappings) {
                if (! columnMapping.getJsonPathParsed().stream().anyMatch(jsonPathElement -> jsonPathElement.getType() == JsonPathElement.Type.LIST_WILDCARD)) {
                    indices.add(i);
                }
                i++;
            }
        } else {
            for (SqlNode expression : selectList.getExpressions()) {
                SqlColumn column = (SqlColumn) expression;
                MongoColumnMapping columnMapping = getColumnMappings().stream().filter(mongoColumnMapping -> mongoColumnMapping.getColumnName().equals(column.getName())).findFirst().get();
                indices.add(getColumnMappings().indexOf(columnMapping));
            }
        }
        return indices;
    }

    public List<Integer> getColumnIndicesWithListWildcard() {
        List<Integer> indices = new ArrayList<>();
        int i = 0;
        for (MongoColumnMapping columnMapping : columnMappings) {
            if (columnMapping.getJsonPathParsed().stream().anyMatch(jsonPathElement -> jsonPathElement.getType() == JsonPathElement.Type.LIST_WILDCARD)) {
                indices.add(i);
            }
            i++;
        }
        return indices;
    }

    public List<Integer> getListWildcardJsonPathIndices() {
        List<Integer> indices = new ArrayList<>();
        for (MongoColumnMapping columnMapping : columnMappings) {
            if (columnMapping.getJsonPathParsed().stream().anyMatch(jsonPathElement -> jsonPathElement.getType() == JsonPathElement.Type.LIST_WILDCARD)) {
                int i=0;
                for (JsonPathElement jsonPathElement : columnMapping.getJsonPathParsed() ) {
                    if (jsonPathElement.getType() == JsonPathElement.Type.LIST_WILDCARD) {
                        indices.add(i);
                    }
                    i++;
                }
            }
        }
        return indices;
    }
}
