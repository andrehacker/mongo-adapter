package com.exasol.mongo.scriptclasses;


import com.exasol.ExaIterator;
import com.exasol.ExaMetadata;
import com.exasol.adapter.AdapterException;
import com.exasol.mongo.MongoColumnMapping;
import com.exasol.mongo.MongoMappingParser;
import com.mongodb.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import org.bson.Document;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class ReadCollectionMapped {

    public static void run(ExaMetadata meta, ExaIterator iter) throws Exception {
        String host = iter.getString("host");
        int port = iter.getInteger("port");
        String db = iter.getString("db");
        String collectionName = iter.getString("collection");
        List<MongoColumnMapping> columnsMapping = MongoMappingParser.parseColumnMappings(iter.getString("columnmapping")); // parseColumnSpec(iter.getString("columnspec"));
        int maxRows = iter.getInteger("maxrows");


        MongoClient mongoClient = new MongoClient(host , port);
        MongoDatabase database = mongoClient.getDatabase(db);
        MongoCollection<Document> collection = database.getCollection(collectionName);

        Object row[] = new Object[columnsMapping.size()];
        MongoCursor<Document> cursor = collection.find().limit(maxRows).iterator();
        try {
            while (cursor.hasNext()) {
                Document doc = cursor.next();
                int i = 0;
                for (MongoColumnMapping col : columnsMapping) {
                    row[i++] = getFieldByType(doc, col.getMongoJsonPath(), col.getType());
                }
                iter.emit(row);
            }
        } finally {
            cursor.close();
        }
    }

    private static Object getFieldByType(Document doc, String jsonPath, MongoColumnMapping.MongoType type) throws AdapterException {
        try {
            switch (type) {
                case STRING:
                    return doc.getString(jsonPath);
                case LONG:
                    return doc.getLong(jsonPath);
                case BOOLEAN:
                    return doc.getBoolean(jsonPath);
                case INTEGER:
                    return doc.getInteger(jsonPath);
                case DATE:
                    return doc.getDate(jsonPath);
                case DOUBLE:
                    return doc.getDouble(jsonPath);
                case OBJECTID:
                    return doc.getObjectId(jsonPath).toString();
                default:
                    throw new RuntimeException("Invalid MongoDB type " + type + ". Should never happen.");
            }
        } catch (Exception e) {
            throw new AdapterException("Invalid mapping: Path " + jsonPath + " cannot be interpreted as " + type + ": " + e.getMessage());
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
