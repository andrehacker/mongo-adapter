package com.exasol.mongo.deriveschema;

import com.exasol.adapter.AdapterException;
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

import static com.mongodb.client.model.Aggregates.sample;

public class DeriveSchema {

    public static MongoDBMapping deriveSchema(MongoAdapterProperties properties) throws AdapterException {
        MongoClient mongoClient = new MongoClient(properties.getMongoHost(), properties.getMongoPort());
        MongoDatabase database = mongoClient.getDatabase(properties.getMongoDB());
        List<MongoCollectionMapping> collectionMappings = new ArrayList<>();
        MongoCursor<String> collectionCursor = database.listCollectionNames().iterator();
        int sampleSize = properties.getAutoMappingSampleSize();
        try {
            while (collectionCursor.hasNext()) {
                String collectionName = collectionCursor.next();
                collectionMappings.add(deriveCollectionMapping(database, sampleSize, collectionName));
            }
        } finally {
            collectionCursor.close();
        }
        return new MongoDBMapping(collectionMappings);
    }

    private static MongoCollectionMapping deriveCollectionMapping(MongoDatabase database, int sampleSize, String collectionName) throws AdapterException {
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
        return new MongoCollectionMapping(collectionName, collectionName, columnMappings);
    }

}
