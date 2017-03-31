package com.exasol.mongo;

import java.util.List;

/**
 * Mapping of a MongoDB DB to virtual tables
 */
public class MongoDBMapping {

    private List<MongoCollectionMapping> collectionMappings;

    public MongoDBMapping(List<MongoCollectionMapping> collectionMappings) {
        this.collectionMappings = collectionMappings;
    }

    public List<MongoCollectionMapping> getCollectionMappings() {
        return collectionMappings;
    }

    public MongoCollectionMapping getCollectionMappingByTableName(String tableName) {
        for (MongoCollectionMapping mapping : collectionMappings) {
            if (mapping.getTableName().equals(tableName)) {
                return mapping;
            }
        }
        return null; // TODO
    }
}
