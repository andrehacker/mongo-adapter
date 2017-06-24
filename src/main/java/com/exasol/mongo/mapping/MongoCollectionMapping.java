package com.exasol.mongo.mapping;


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

    public static MongoColumnMapping getColumnMappingByName(List<MongoColumnMapping> columnsMapping, String columnName) {
        for (MongoColumnMapping mapping : columnsMapping) {
            if (mapping.getColumnName().equals(columnName)) {
                return mapping;
            }
        }
        throw new RuntimeException("Internal error: Could not find mapping for " + columnName);
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

    private List<MongoColumnMapping> getRequestedColumnMappings(SqlSelectList selectList) {
        if (selectList.isSelectStar()) {
            return columnMappings;
        }
        List<MongoColumnMapping> requestedColumnMappings = new ArrayList<>();
        for (SqlNode expression : selectList.getExpressions()) {
            SqlColumn column = (SqlColumn) expression;
            requestedColumnMappings.add(getColumnMappingByName(getColumnMappings(), column.getName()));
        }
        return requestedColumnMappings;
    }

    public boolean hasWildcard(SqlSelectList selectList) {
        List<MongoColumnMapping> requestedColumnMappings = getRequestedColumnMappings(selectList);
        for (MongoColumnMapping col : requestedColumnMappings) {
            if (col.getJsonPathParsed().stream().anyMatch(jsonPathElement -> jsonPathElement.getType() == JsonPathElement.Type.LIST_WILDCARD)) {
                return true;
            }
        }
        return false;
    }

    public static class IndicesCache {
        private List<Integer> simpleColumnIndices;
        private List<Integer> simpleColumnTargetIndices;
        private List<Integer> wildcardColumnIndices;
        private List<Integer> wildcardColumnTargetIndices;
        private List<Integer> wildcardJsonPathIndices;

        public IndicesCache(List<Integer> simpleColumnIndices, List<Integer> simpleColumnTargetIndices, List<Integer> wildcardColumnIndices, List<Integer> wildcardColumnTargetIndices, List<Integer> wildcardJsonPathIndices) {
            this.simpleColumnIndices = simpleColumnIndices;
            this.simpleColumnTargetIndices = simpleColumnTargetIndices;
            this.wildcardColumnIndices = wildcardColumnIndices;
            this.wildcardColumnTargetIndices = wildcardColumnTargetIndices;
            this.wildcardJsonPathIndices = wildcardJsonPathIndices;
        }

        public List<Integer> getSimpleColumnIndices() {
            return simpleColumnIndices;
        }

        public List<Integer> getSimpleColumnTargetIndices() {
            return simpleColumnTargetIndices;
        }

        public List<Integer> getWildcardColumnIndices() {
            return wildcardColumnIndices;
        }

        public List<Integer> getWildcardColumnTargetIndices() {
            return wildcardColumnTargetIndices;
        }

        public List<Integer> getWildcardJsonPathIndices() {
            return wildcardJsonPathIndices;
        }
    }

    public IndicesCache computeIndicesCache(SqlSelectList selectList) {
        List<Integer> simpleColumnIndices = new ArrayList<>();
        List<Integer> simpleColumnTargetIndices = new ArrayList<>();
        List<Integer> wildcardColumnIndices = new ArrayList<>();
        List<Integer> wildcardColumnTargetIndices = new ArrayList<>();
        List<Integer> wildcardJsonPathIndices = new ArrayList<>();

        List<MongoColumnMapping> requestedColumnMappings = getRequestedColumnMappings(selectList);

        for (int targetColumn = 0; targetColumn < requestedColumnMappings.size(); targetColumn++) {
            MongoColumnMapping columnMapping = requestedColumnMappings.get(targetColumn);
            if (!columnMapping.jsonPathHasListWildcard()) {
                simpleColumnIndices.add(getColumnMappings().indexOf(columnMapping));
                simpleColumnTargetIndices.add(targetColumn);
            } else {
                wildcardColumnIndices.add(getColumnMappings().indexOf(columnMapping));
                wildcardColumnTargetIndices.add(targetColumn);
                int jsonPathIndex=0;
                for (JsonPathElement jsonPathElement : columnMapping.getJsonPathParsed() ) {
                    if (jsonPathElement.getType() == JsonPathElement.Type.LIST_WILDCARD) {
                        wildcardJsonPathIndices.add(jsonPathIndex);
                        break;
                    }
                    jsonPathIndex++;
                }
            }
        }
        return new IndicesCache(simpleColumnIndices, simpleColumnTargetIndices, wildcardColumnIndices, wildcardColumnTargetIndices, wildcardJsonPathIndices);
    }

    public List<Integer> getColumnIndicesWithoutWildcard(SqlSelectList selectList) {
        List<Integer> indices = new ArrayList<>();
        List<MongoColumnMapping> requestedColumnMappings = getRequestedColumnMappings(selectList);
        for (MongoColumnMapping columnMapping : requestedColumnMappings) {
            if (columnMapping.getJsonPathParsed().stream().noneMatch(jsonPathElement -> jsonPathElement.getType() == JsonPathElement.Type.LIST_WILDCARD)) {
                indices.add(getColumnMappings().indexOf(columnMapping));
            }
        }
        return indices;
    }

    public List<Integer> getColumnIndicesWithWildcard(SqlSelectList selectList) {
        List<Integer> indices = new ArrayList<>();
        List<MongoColumnMapping> requestedColumnMappings = getRequestedColumnMappings(selectList);
        for (MongoColumnMapping columnMapping : requestedColumnMappings) {
            if (columnMapping.getJsonPathParsed().stream().anyMatch(jsonPathElement -> jsonPathElement.getType() == JsonPathElement.Type.LIST_WILDCARD)) {
                indices.add(getColumnMappings().indexOf(columnMapping));
            }
        }
        return indices;
    }

    public List<Integer> getWildcardJsonPathIndices(SqlSelectList selectList) {
        List<Integer> indices = new ArrayList<>();
        List<MongoColumnMapping> requestedColumnMappings = getRequestedColumnMappings(selectList);
        for (MongoColumnMapping columnMapping : requestedColumnMappings) {
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
