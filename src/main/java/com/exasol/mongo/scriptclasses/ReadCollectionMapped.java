package com.exasol.mongo.scriptclasses;


import com.exasol.ExaDataTypeException;
import com.exasol.ExaIterationException;
import com.exasol.ExaIterator;
import com.exasol.ExaMetadata;
import com.exasol.adapter.AdapterException;
import com.exasol.jsonpath.JsonPathElement;
import com.exasol.jsonpath.JsonPathFieldElement;
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

import static com.mongodb.client.model.Projections.excludeId;
import static com.mongodb.client.model.Projections.fields;
import static com.mongodb.client.model.Projections.include;

public class ReadCollectionMapped {

    public static void run(ExaMetadata meta, ExaIterator iter) throws Exception {
        String host = iter.getString("host");
        int port = iter.getInteger("port");
        String db = iter.getString("db");
        String collectionName = iter.getString("collection");
        List<MongoColumnMapping> columnsMapping = MongoMappingParser.parseColumnMappings(iter.getString("columnmapping")); // parseColumnSpec(iter.getString("columnspec"));
        int maxRows = iter.getInteger("maxrows");

        readMapped(iter, host, port, db, collectionName, columnsMapping, maxRows);
    }

    static void readMapped(ExaIterator iter, String host, int port, String db, String collectionName, List<MongoColumnMapping> columnsMapping, int maxRows) throws AdapterException, ExaIterationException, ExaDataTypeException {
        MongoClient mongoClient = new MongoClient(host , port);
        MongoDatabase database = mongoClient.getDatabase(db);
        MongoCollection<Document> collection = database.getCollection(collectionName);

        Document projection = new Document();
        boolean includeId = false;
        for (MongoColumnMapping col : columnsMapping) {
            projection.append(col.getMongoJsonPath(), 1);
            if (col.getMongoJsonPath().equals("_id")) {
                includeId = true;
            };
        }
        if (!includeId) {
            projection.append("_id", 0);  // has to be excluded explicitly, otherwise is automatically included
        }
        MongoCursor<Document> cursor = collection.find()
                .projection(projection)
                .limit(maxRows)
                .iterator();
        Object row[] = new Object[columnsMapping.size()];
        try {
            while (cursor.hasNext()) {
                Document doc = cursor.next();
                int i = 0;
                for (MongoColumnMapping col : columnsMapping) {
                    row[i++] = getFieldByType(doc, col.getMongoJsonPathParsed(), col.getType());
                }
                iter.emit(row);
            }
        } finally {
            cursor.close();
        }
    }

    private static Object getFieldByType(Document doc, List<JsonPathElement> jsonPath, MongoColumnMapping.MongoType type) throws AdapterException {
        try {
            if (jsonPath.size() == 1) {
                assert(jsonPath.get(0).getType() == JsonPathElement.Type.FIELD);
                return doc.get(((JsonPathFieldElement)jsonPath.get(0)).getFieldName(), type.getClazz());
            } else {
                for (int i = 0; i < jsonPath.size() - 1; i++) {
                    JsonPathElement element = jsonPath.get(i);
                    assert(element.getType() == JsonPathElement.Type.FIELD);  // TODO Only field access supported - check in a nicer way!
                    doc = doc.get(((JsonPathFieldElement)element).getFieldName(), Document.class);
                }
                return doc.get(((JsonPathFieldElement)jsonPath.get(jsonPath.size()-1)).getFieldName(), type.getClazz());
            }
        } catch (Exception e) {
            throw new AdapterException("Error when retrieving path " + jsonPath + " as type " + type + ": " + e.getMessage());
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
