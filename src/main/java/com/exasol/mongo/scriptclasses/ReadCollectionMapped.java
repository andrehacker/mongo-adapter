package com.exasol.mongo.scriptclasses;


import com.exasol.ExaIterator;
import com.exasol.ExaMetadata;
import com.exasol.adapter.AdapterException;
import com.exasol.adapter.json.RequestJsonParser;
import com.exasol.adapter.request.PushdownRequest;
import com.exasol.adapter.sql.SqlStatementSelect;
import com.exasol.jsonpath.JsonPathElement;
import com.exasol.jsonpath.JsonPathFieldElement;
import com.exasol.jsonpath.JsonPathListIndexElement;
import com.exasol.mongo.MongoCollectionMapping;
import com.exasol.mongo.MongoColumnMapping;
import com.exasol.mongo.MongoDBMapping;
import com.exasol.mongo.MongoFilterGeneratorVisitor;
import com.exasol.mongo.adapter.MongoAdapter;
import com.exasol.mongo.adapter.MongoAdapterProperties;
import com.mongodb.MongoClient;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import com.mongodb.util.JSON;
import org.bson.Document;
import org.bson.types.ObjectId;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Stream;

import static com.exasol.mongo.MongoColumnMapping.MongoType.DOCUMENT;
import static com.exasol.mongo.adapter.MongoAdapterProperties.MongoMappingMode.MAPPED;
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
        MongoDBMapping mapping = (properties.getMappingMode() == MAPPED) ? properties.getMapping() : MongoDBMapping.constructDefaultMapping(request.getInvolvedTablesMetadata());
        String tableName = select.getFromClause().getName();
        MongoCollectionMapping collectionMapping = mapping.getCollectionMappingByTableName(tableName);
        String collectionName = collectionMapping.getCollectionName();
        SchemaEnforcementLevel schemaEnforcementLevel = SchemaEnforcementLevel.fromString(properties.getSchemaEnforcementLevel().name());
        int maxRows = select.hasLimit() ? select.getLimit().getLimit() : properties.getMaxResultRows();

        MongoClient mongoClient = new MongoClient(host, port);
        MongoDatabase database = mongoClient.getDatabase(db);
        MongoCollection<Document> collection = database.getCollection(collectionName);
        MongoFilterGeneratorVisitor filterGenerator = new MongoFilterGeneratorVisitor(collectionMapping.getColumnMappings());

        if (MongoAdapter.isCountStar(select.getSelectList())) {
            long count = (select.hasFilter()) ? collection.count(select.getWhereClause().accept(filterGenerator)) : collection.count();
            iter.emit(count);
        } else {
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
            if (collectionMapping.hasListStar()) {
                // Better approach: call getFieldByType one time for simple values, and then n times for complex values (with incrementing the list index to obtain!) This simulates for a.b[*] calls like a.b[0], a.b[1], a.b[2], until there is no more (return NULL then). Could be extended in future by multiple list indices.
                Object row[] = new Object[collectionMapping.getColumnMappings().size()];
                List<Integer> simpleColumnIndices = collectionMapping.getColumnMappingsWithoutListStar();
                List<Integer> listColumnIndices = collectionMapping.getListStarColumnMappings();
                try {
                    while (cursor.hasNext()) {
                        Document doc = cursor.next();
                        for (Integer index : simpleColumnIndices) {
                            MongoColumnMapping col = collectionMapping.getColumnMappings().get(index);
                            row[index] = getFieldByType(doc, col.getJsonPathParsed(), col.getType(), schemaEnforcementLevel);
                        }
                        boolean foundListElement = true;
                        int listIndex = 0;
                        while (foundListElement) {
                            foundListElement = false;
                            for (Integer colIndex : listColumnIndices) {
                                MongoColumnMapping col = collectionMapping.getColumnMappings().get(colIndex);
                                row[colIndex] = getFieldByType(doc, replaceListStar(col.getJsonPathParsed(), listIndex), col.getType(), schemaEnforcementLevel);
                                foundListElement = foundListElement || (row[colIndex] != null);
                            }
                            if (foundListElement) {
                                iter.emit(row);
                                listIndex++;
                            }
                        }
                    }
                } finally {
                    cursor.close();
                }
            } else {
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
        }
    }

    private static List<JsonPathElement> replaceListStar(List<JsonPathElement> jsonPath, int index) {
        List<JsonPathElement> clone = new ArrayList<>(jsonPath);  // TODO slow!
        for (int i=0; i<jsonPath.size(); i++) {
            if (jsonPath.get(i).getType() == JsonPathElement.Type.LIST_STAR) {
                clone.set(i, new JsonPathListIndexElement(index));
            }
        }
        return clone;
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
        Object curElement = doc;
        if (jsonPath.isEmpty()) {
            if (type != DOCUMENT) {
                throw new AdapterException("The root field '$' is of type DOCUMENT, but was specified as type " + type.name() + " in the mapping.");
            }
            return ((Document)curElement).toJson();
        }
        try {
            JsonPathElement curPathElement = (JsonPathFieldElement) jsonPath.get(0);
            if (jsonPath.size() > 1) {
                try {
                    for (int i = 0; i < jsonPath.size() - 1; i++) {  // TODO Can we simply go to leaf here?
                        curPathElement = jsonPath.get(i);
                        if (curPathElement.getType() == JsonPathElement.Type.FIELD) {
                            curElement = ((Document)curElement).get(((JsonPathFieldElement) curPathElement).getFieldName());
                        } else if (curPathElement.getType() == JsonPathElement.Type.LIST_INDEX) {
                            if (((List)curElement).size() <= ((JsonPathListIndexElement) curPathElement).getListIndex()) {
                                return null;
                            }
                            curElement = ((List)curElement).get(((JsonPathListIndexElement) curPathElement).getListIndex());
                        } else {
                            throw new RuntimeException("Unsupported path type (" + curPathElement.getType() + "), should never happen");
                        }
                    }
                } catch (NullPointerException ex) {
                    // Handle only case where nested structure does not exist as specified (not even with different types)
                    return null;
                }
                if (curElement == null) {
                    return null;  // we would run into an nullpointerexception
                }
                curPathElement = ((JsonPathFieldElement) jsonPath.get(jsonPath.size() - 1));
            }
            if (curPathElement.getType() == JsonPathElement.Type.FIELD) {
                curElement = ((Document)curElement).get(((JsonPathFieldElement) curPathElement).getFieldName());
            } else if (curPathElement.getType() == JsonPathElement.Type.LIST_INDEX) {
                curElement = ((List)curElement).get(((JsonPathListIndexElement) curPathElement).getListIndex());
            }
            if (type == MongoColumnMapping.MongoType.OBJECTID) {
                return ((ObjectId)curElement).toString();
            } else if (type.isPrimitive()) {
                return curElement;
            } else {
                // return as json representation
                if (type == DOCUMENT) {
                    return (curElement == null) ? null : ((Document)curElement).toJson();
                } else if (type == MongoColumnMapping.MongoType.ARRAY) {
                    return (curElement == null) ? null : JSON.serialize(curElement);
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
            throw e;
            //throw new RuntimeException("Unexpected error when retrieving path " + jsonPath + " as type " + type + ": " + e.getMessage(), e);
        }
    }

}
