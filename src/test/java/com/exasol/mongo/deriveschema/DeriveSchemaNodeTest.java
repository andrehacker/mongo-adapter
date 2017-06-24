package com.exasol.mongo.deriveschema;

import com.exasol.adapter.AdapterException;
import com.exasol.mongo.mapping.MongoColumnMapping;
import com.exasol.mongo.scriptclasses.ReadCollectionMappedTest;
import com.mongodb.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import org.bson.Document;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

import static com.exasol.mongo.scriptclasses.ReadCollectionMappedTest.*;
import static com.mongodb.client.model.Aggregates.sample;

public class DeriveSchemaNodeTest {

    @Test
    public void testDeriveSchemaOneDocument() throws AdapterException {
        testDeriveSchema(ReadCollectionMappedTest.MONGO_DB, "test");
    }

    @Test
    public void testDeriveSchemaMultiDocument() throws AdapterException {
        testDeriveSchema(ReadCollectionMappedTest.MONGO_DB, "testMulti");
    }

    @Test
    public void testDeriveSchemaTestJobs() throws AdapterException {
        testDeriveSchema("testdb", "testjobs");
        testDeriveSchema("testdb", "comments");
    }

    public void testDeriveSchema(String db, String collectionName) throws AdapterException {
        createTestData();
        MongoClient mongoClient = new MongoClient(MONGO_HOST, Integer.parseInt(MONGO_PORT));
        MongoDatabase database = mongoClient.getDatabase(db);
        MongoCollection<Document> collection = database.getCollection(collectionName);

        DeriveSchemaRootNode root = new DeriveSchemaRootNode();

        MongoCursor<Document> cursor = collection.aggregate(Arrays.asList(sample(100))).iterator();
        try {
            while (cursor.hasNext()) {
                Document doc = cursor.next();
                root.updateDerivedSchema(doc);
                root.updateDerivedSchemaRecursive(doc);
            }
        } finally {
            cursor.close();
        }
        List<MongoColumnMapping> columnMappings = root.mergeCompatibleAndGetBestCollectionMapping();
        for (MongoColumnMapping columnMapping : columnMappings) {
            System.out.println("- Mapping: jsonPath: " + columnMapping.getJsonPath() + " columnName: " + columnMapping.getColumnName().toString() + " Type: " + columnMapping.getType());
        }
    }

}