package com.exasol.mongo.scriptclasses;

import com.exasol.ExaIterator;
import com.exasol.ExaMetadata;
import com.mongodb.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import org.bson.Document;

public class ReadCollectionJson {

    public static void run(ExaMetadata meta, ExaIterator iter) throws Exception {
        String host = iter.getString("host");
        int port = iter.getInteger("port");
        String db = iter.getString("db");
        String collectionName = iter.getString("collection");
        int maxRows = iter.getInteger("maxrows");

        MongoClient mongoClient = new MongoClient( host , port );
        MongoDatabase database = mongoClient.getDatabase(db);
        MongoCollection<Document> collection = database.getCollection(collectionName);

        MongoCursor<Document> cursor = collection.find().limit(maxRows).iterator();
        try {
            while (cursor.hasNext()) {
                iter.emit(cursor.next().toJson());
            }
        } finally {
            cursor.close();
        }
    }

}
