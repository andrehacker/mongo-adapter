package com.exasol.mongo.scriptclasses;


import com.exasol.ExaIterator;
import com.exasol.ExaMetadata;
import com.exasol.adapter.AdapterException;
import com.exasol.adapter.json.RequestJsonParser;
import com.exasol.adapter.request.PushdownRequest;
import com.exasol.adapter.sql.SqlStatementSelect;
import com.exasol.jsonpath.JsonPathElement;
import com.exasol.jsonpath.JsonPathFieldElement;
import com.exasol.mongo.*;
import com.exasol.mongo.adapter.MongoAdapterProperties;
import com.mongodb.MongoClient;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import com.mongodb.util.JSON;
import org.bson.Document;
import org.bson.types.ObjectId;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static com.exasol.mongo.adapter.MongoAdapterProperties.SchemaEnforcementLevel;
import static com.exasol.mongo.adapter.MongoAdapterProperties.SchemaEnforcementLevel.CHECK_TYPE;
import static com.exasol.mongo.adapter.MongoAdapterProperties.UNLIMITED_RESULT_ROWS;

/**
 * https://docs.mongodb.com/manual/core/document/#document-dot-notation
 * - dot notation
 * - field name conventions
 *
 * https://docs.mongodb.com/manual/tutorial/project-fields-from-query-results/
 * - Return Specific Fields in Embedded Documents: You can return specific fields in an embedded document. Use the dot notation to refer to the embedded field and set to 1 in the projection document.
 *
 */
public class ReadCollectionMapped {

    public static void run(ExaMetadata meta, ExaIterator iter) throws Exception {
        PushdownRequest request = (PushdownRequest) (new RequestJsonParser()).parseRequest(iter.getString("request"));

        readMapped(iter, request);
    }

    static void readMapped(ExaIterator iter, PushdownRequest request) throws Exception {
        MongoAdapterProperties properties = new MongoAdapterProperties(request.getSchemaMetadataInfo().getProperties());
        String host = properties.getMongoHost();
        int port = properties.getMongoPort();
        String db = properties.getMongoDB();
        SqlStatementSelect select = (SqlStatementSelect) request.getSelect();
        MongoDBMapping mapping = MongoMappingParser.parse(properties.getMapping());
        String tableName = select.getFromClause().getName();
        MongoCollectionMapping collectionMapping = mapping.getCollectionMappingByTableName(tableName);
        String collectionName = collectionMapping.getCollectionName();
        SchemaEnforcementLevel schemaEnforcementLevel = SchemaEnforcementLevel.fromString(properties.getSchemaEnforcementLevel().name());
        int maxRows = select.hasLimit() ? select.getLimit().getLimit() : properties.getMaxResultRows();

        MongoClient mongoClient = new MongoClient(host , port);
        MongoDatabase database = mongoClient.getDatabase(db);
        MongoCollection<Document> collection = database.getCollection(collectionName);
        MongoFilterGeneratorVisitor filterGenerator = new MongoFilterGeneratorVisitor(collectionMapping.getColumnMappings());

        Document projection = constructProjectionFromColumnMapping(collectionMapping.getColumnMappings());

        FindIterable<Document> tempCursor = collection.find();
        if (projection != null) {
            tempCursor = tempCursor.projection(projection);
        }
        if (select.hasFilter()) {
            tempCursor = tempCursor.filter(select.getWhereClause().accept(filterGenerator));
        }
        if (maxRows != UNLIMITED_RESULT_ROWS) {
            tempCursor = tempCursor.limit(maxRows);
        }
        MongoCursor<Document> cursor = tempCursor.iterator();
        Object row[] = new Object[collectionMapping.getColumnMappings().size()];
        try {
            while (cursor.hasNext()) {
                Document doc = cursor.next();
                int i = 0;
                for (MongoColumnMapping col : collectionMapping.getColumnMappings()) {
                    row[i++] = getFieldByType(doc, col.getJsonPathParsed(), col.getType(), schemaEnforcementLevel);
                }
                iter.emit(row);
            }
        } finally {
            cursor.close();
        }
    }

    /**
     * Attention: If we specify several projections for a subdocument, only the last one seems to be considered.
     * To keep it simple we only project on the highest level of fields.
     */
    private static Document constructProjectionFromColumnMapping(List<MongoColumnMapping> columnsMapping) {
        Set<String> highestLevelFields = new HashSet<>();
        Document projection = new Document();
        boolean includeId = false;
        for (MongoColumnMapping col : columnsMapping) {
            List<JsonPathElement> path = col.getJsonPathParsed();
            if (path.size() == 0) {
                // root element requested, we need all data and thus no projection
                return null;
            }
            if (!highestLevelFields.contains(path.get(0))) {
                highestLevelFields.add(path.get(0).toJsonPathString());
                //String mongoProjection = path.stream().map(JsonPathElement::toJsonPathString).collect(joining(".")); //"";   // JsonPath could look like "$.fieldname", but projection should look like "fieldname"
                String mongoProjection = path.get(0).toJsonPathString();
                projection.append(mongoProjection, 1);
                if (mongoProjection.equals("_id")) {
                    includeId = true;
                };
            }
        }
        if (!includeId) {
            projection.append("_id", 0);  // has to be excluded explicitly, otherwise is automatically included
        }
        return projection;
    }

    private static Object getFieldByType(Document doc, List<JsonPathElement> jsonPath, MongoColumnMapping.MongoType type, SchemaEnforcementLevel schemaEnforcementLevel) throws AdapterException {
        try {
            JsonPathFieldElement element = (JsonPathFieldElement) jsonPath.get(0);
            if (jsonPath.size() > 1) {
                // go down into nested document
                try {
                    for (int i = 0; i < jsonPath.size() - 1; i++) {
                        JsonPathElement curElement = jsonPath.get(i);
                        assert (curElement.getType() == JsonPathElement.Type.FIELD);  // TODO Only field access supported - check in a nicer way!
                        doc = doc.get(((JsonPathFieldElement) curElement).getFieldName(), Document.class);
                    }
                } catch (NullPointerException ex) {
                    // Handle only case where nested structure does not exist as specified (not even with different types)
                    return null;
                }
                if (doc == null) {
                    return null;  // we would run into an nullpointerexception
                }
                element = ((JsonPathFieldElement) jsonPath.get(jsonPath.size() - 1));
            }
            if (type == MongoColumnMapping.MongoType.OBJECTID) {
                return doc.get(element.getFieldName(), ObjectId.class).toString();
            } else if (type.isPrimitive()) {
                return doc.get(element.getFieldName(), type.getClazz());
            } else {
                // return as json representation
                if (type == MongoColumnMapping.MongoType.DOCUMENT) {
                    Document val = doc.get(element.getFieldName(), Document.class);
                    return (val == null) ? null : val.toJson();
                } else if (type == MongoColumnMapping.MongoType.ARRAY) {
                    List<?> val = doc.get(element.getFieldName(), List.class);
                    return (val == null) ? null : JSON.serialize(val);
                } else {
                    throw new RuntimeException("Unknown non-primitive mongo type, should never happen: " + type);
                }
            }
        } catch (ClassCastException e) {
            if (schemaEnforcementLevel == CHECK_TYPE) {
                throw new AdapterException("Error when retrieving path " + jsonPath + " as type " + type + ": " + e.getMessage());
            } else {
                return null;
            }
        } catch (Exception e) {
            throw new RuntimeException("Unexpected error when retrieving path " + jsonPath + " as type " + type + ": " + e.getMessage(), e);
        }
    }

}
