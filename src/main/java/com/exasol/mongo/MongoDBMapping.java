package com.exasol.mongo;

import com.exasol.adapter.metadata.TableMetadata;

import java.util.ArrayList;
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
        throw new RuntimeException("Internal error: Did not find collection mapping by table name: " + tableName);
    }

    public static MongoDBMapping constructDefaultMapping(List<TableMetadata> tablesMetadata) {
        List<MongoCollectionMapping> collectionMappings = new ArrayList<>();
        for (TableMetadata tableMetadata : tablesMetadata) {
            List<MongoColumnMapping> columnMappings = new ArrayList<>();
            columnMappings.add(new MongoColumnMapping("$", "JSON", MongoColumnMapping.MongoType.DOCUMENT));
            MongoCollectionMapping collectionMapping = new MongoCollectionMapping(tableMetadata.getName(), tableMetadata.getName(), columnMappings);
            collectionMappings.add(collectionMapping);
        }
        return new MongoDBMapping(collectionMappings);
    }
}
