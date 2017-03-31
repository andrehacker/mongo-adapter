package com.exasol.mongo.scriptclasses;


import com.exasol.ExaIterator;
import com.exasol.ExaMetadata;
import com.exasol.mongo.MongoColumnMapping;
import com.mongodb.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import org.bson.Document;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class ReadCollectionMappedOld {

    public static void run(ExaMetadata meta, ExaIterator iter) throws Exception {
        String host = iter.getString("host");
        int port = iter.getInteger("port");
        String db = iter.getString("db");
        String collectionName = iter.getString("collection");
        List<MongoColumnMapping> columnsSpec = parseColumnSpec(iter.getString("columnspec"));

        MongoClient mongoClient = new MongoClient( host , port );
        MongoDatabase database = mongoClient.getDatabase(db);
        MongoCollection<Document> collection = database.getCollection(collectionName);

        Object row[] = new Object[columnsSpec.size()];
        MongoCursor<Document> cursor = collection.find().iterator();
        try {
            while (cursor.hasNext()) {
                Document doc = cursor.next();
                int i = 0;
                for (MongoColumnMapping col : columnsSpec) {
                    row[i++] = getFieldByType(doc, col.getMongoJsonPath(), col.getType());
                }
                iter.emit(row);
            }
        } finally {
            cursor.close();
        }
    }

    private static Object getFieldByType(Document doc, String name, MongoColumnMapping.MongoType type) {
        switch (type) {
            case STRING:
                return doc.getString(name);
            case LONG:
                return doc.getLong(name);
            case BOOLEAN:
                return doc.getBoolean(name);
            case INTEGER:
                return doc.getInteger(name);
            case DATE:
                return doc.getDate(name);
            case DOUBLE:
                return doc.getDouble(name);
            case OBJECTID:
                return doc.getObjectId(name).toString();
            default:
                throw new RuntimeException("Invalid MongoDB type " + type + ". Should never happen.");
        }
    }

    private static List<MongoColumnMapping> parseColumnSpec(String columnSpec) {
        List<MongoColumnMapping> columns = new ArrayList<>();
        for (String column : Arrays.asList(columnSpec.split(","))) {
            columns.add(new MongoColumnMapping(
                    column.split(":")[0],
                    column.split(":")[0],
                    MongoColumnMapping.mongoTypeFromString(column.split(":")[1])));
        }
        return columns;
    }


}
