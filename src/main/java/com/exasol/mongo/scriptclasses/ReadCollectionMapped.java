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
import com.mongodb.util.JSON;
import org.bson.Document;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

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

        Document projection = constructProjectionFromColumnMapping(columnsMapping);
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
                    row[i++] = getFieldByType(doc, col.getJsonPathParsed(), col.getType());
                }
                iter.emit(row);
            }
        } finally {
            cursor.close();
        }
    }

    private static Document constructProjectionFromColumnMapping(List<MongoColumnMapping> columnsMapping) {
        Document projection = new Document();
        boolean includeId = false;
        for (MongoColumnMapping col : columnsMapping) {
            List<JsonPathElement> path = col.getJsonPathParsed();
            String mongoProjection = path.stream().map(JsonPathElement::toJsonPathString).collect(Collectors.joining(".")); //"";   // JsonPath could look like "$.fieldname", but projection should look like "fieldname"
            projection.append(mongoProjection, 1);
            if (mongoProjection.equals("_id")) {
                includeId = true;
            };
        }
        if (!includeId) {
            projection.append("_id", 0);  // has to be excluded explicitly, otherwise is automatically included
        }
        return projection;
    }

    /**
     * TODO We should ignore if types are different or nested structure is not as expected. Or let the user decide if he wants to ignore.
     */
    private static Object getFieldByType(Document doc, List<JsonPathElement> jsonPath, MongoColumnMapping.MongoType type) throws AdapterException {
        try {
            JsonPathFieldElement element = (JsonPathFieldElement)jsonPath.get(0);
            if (jsonPath.size() > 1) {
                // go down into nested document
                for (int i = 0; i < jsonPath.size() - 1; i++) {
                    JsonPathElement curElement = jsonPath.get(i);
                    assert(curElement.getType() == JsonPathElement.Type.FIELD);  // TODO Only field access supported - check in a nicer way!
                    doc = doc.get(((JsonPathFieldElement)curElement).getFieldName(), Document.class);
                }
                element = ((JsonPathFieldElement)jsonPath.get(jsonPath.size()-1));
            }
            if (type.isPrimitive()) {
                return doc.get(element.getFieldName(), type.getClazz());
            } else {
                // return as json representation
                if (type == MongoColumnMapping.MongoType.DOCUMENT) {
                    return doc.get(element.getFieldName(), Document.class).toJson();
                } else if (type == MongoColumnMapping.MongoType.ARRAY) {
                    return JSON.serialize(doc.get(element.getFieldName(), List.class));
                } else {
                    throw new RuntimeException("Unknown non-primitive mongo type, should never happen: " + type);
                }
            }
        } catch (Exception e) {
            throw new AdapterException("Error when retrieving path " + jsonPath + " as type " + type + ": " + e.getMessage());
        }
    }

    private static List<MongoColumnMapping> parseColumnSpec(String columnSpec) throws AdapterException {
        List<MongoColumnMapping> columns = new ArrayList<>();
        for (String column : Arrays.asList(columnSpec.split(","))) {
            columns.add(new MongoColumnMapping(
                    column.split(":")[0],
                    column.split(":")[0],
                    MongoColumnMapping.MongoType.fromString(column.split(":")[1])));
        }
        return columns;
    }


}
