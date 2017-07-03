package com.exasol.mongo.deriveschema;

import com.exasol.adapter.AdapterException;
import com.exasol.mongo.adapter.MongoAdapter;
import com.exasol.mongo.adapter.MongoAdapterProperties;
import com.exasol.mongo.mapping.MongoCollectionMapping;
import com.exasol.mongo.mapping.MongoColumnMapping;
import com.exasol.mongo.mapping.MongoDBMapping;
import com.mongodb.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import org.bson.Document;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import static com.mongodb.client.model.Aggregates.sample;

public class DeriveSchema {

    public static MongoDBMapping deriveSchema(MongoAdapterProperties properties, boolean ignoreCollectionCase) throws AdapterException {
        MongoClient mongoClient = new MongoClient(properties.getMongoHost(), properties.getMongoPort());
        MongoDatabase database = mongoClient.getDatabase(properties.getMongoDB());
        MongoCursor<String> collectionCursor = database.listCollectionNames().iterator();
        int sampleSize = properties.getAutoMappingSampleSize();
        List<String> collectionNames = new ArrayList<>();
        try {
            while (collectionCursor.hasNext()) {
                collectionNames.add(collectionCursor.next());
            }
        } finally {
            collectionCursor.close();
        }
        List<MongoCollectionMapping> collectionMappings = new ArrayList<>();
        List<String> columnPathsToIgnore = properties.getAutoMappingIgnoredColumnPaths();
        for (final String collectionName : collectionNames) {
            collectionMappings.add(deriveCollectionMapping(database, sampleSize, collectionName, collectionNames, columnPathsToIgnore, ignoreCollectionCase));
        }
        return new MongoDBMapping(collectionMappings);
    }

    private static MongoCollectionMapping deriveCollectionMapping(MongoDatabase database, int sampleSize, String collectionName, List<String> collectionNames, List<String> columnPathsToIgnore, boolean ignoreCollectionCase) throws AdapterException {
        MongoCollection<Document> collection = database.getCollection(collectionName);
        DeriveSchemaRootNode root = new DeriveSchemaRootNode();
        MongoCursor<Document> sampleCursor = collection.aggregate(Arrays.asList(sample(sampleSize))).iterator();
        try {
            while (sampleCursor.hasNext()) {
                root.updateDerivedSchema(sampleCursor.next());
            }
        } finally {
            sampleCursor.close();
        }
        List<MongoColumnMapping> columnMappings = root.mergeCompatibleAndGetBestCollectionMapping();
        List<MongoColumnMapping> filteredColumnMappings = columnMappings.stream().filter(columnMapping -> {
            final String curJsonPath = columnMapping.getJsonPath();
            return columnPathsToIgnore.stream().noneMatch(pathToIgnore -> curJsonPath.matches(pathToIgnore));
        }).collect(Collectors.toList());
        String tableName = collectionName;
        if (ignoreCollectionCase) {
            if (MongoAdapter.isCollectionNameUnambiguous(collectionNames, collectionName)) {
                tableName = tableName.toUpperCase();
            }
        }
        return new MongoCollectionMapping(collectionName, tableName, filteredColumnMappings);
    }

}
