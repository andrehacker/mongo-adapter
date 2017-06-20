package com.exasol.mongo.scriptclasses;


import com.exasol.ExaIterator;
import com.exasol.ExaMetadata;
import com.exasol.adapter.AdapterException;
import com.exasol.adapter.json.RequestJsonParser;
import com.exasol.adapter.request.PushdownRequest;
import com.exasol.adapter.sql.*;
import com.exasol.jsonpath.JsonPathElement;
import com.exasol.jsonpath.JsonPathFieldElement;
import com.exasol.jsonpath.JsonPathListIndexElement;
import com.exasol.mongo.MongoCollectionMapping;
import com.exasol.mongo.MongoColumnMapping;
import com.exasol.mongo.MongoDBMapping;
import com.exasol.mongo.MongoVisitor;
import com.exasol.mongo.adapter.MongoAdapter;
import com.exasol.mongo.adapter.MongoAdapterProperties;
import com.mongodb.MongoClient;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import com.mongodb.util.JSON;
import org.bson.Document;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static com.exasol.mongo.MongoColumnMapping.MongoType.*;
import static com.exasol.mongo.adapter.MongoAdapterProperties.MongoMappingMode.MAPPED;
import static com.exasol.mongo.adapter.MongoAdapterProperties.SchemaEnforcementLevel;
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
        MongoVisitor mongoVisitor = new MongoVisitor(collectionMapping.getColumnMappings());

        if (MongoAdapter.isCountStar(select.getSelectList())) {
            long count = (select.hasFilter()) ? collection.count(select.getWhereClause().accept(mongoVisitor)) : collection.count();
            iter.emit(count);
        } else {
            Document projection = constructProjectionFromColumnMapping(collectionMapping.getColumnMappings(), select.getSelectList());

            FindIterable<Document> tempCursor = collection.find();
            if (projection != null) {
                tempCursor = tempCursor.projection(projection);
            }
            if (select.hasFilter()) {
                tempCursor = tempCursor.filter(select.getWhereClause().accept(mongoVisitor));
            }
            if (select.hasOrderBy()) {
                tempCursor = tempCursor.sort(select.getOrderBy().accept(mongoVisitor));
            }
            if (maxRows != UNLIMITED_RESULT_ROWS) {
                tempCursor = tempCursor.limit(maxRows);
            }
            MongoCursor<Document> cursor = tempCursor.iterator();
            int numColumns = (select.getSelectList().isSelectStar()) ? collectionMapping.getColumnMappings().size() : select.getSelectList().getExpressions().size();
            List<Integer> simpleColumnIndices = collectionMapping.getColumnIndicesWithoutListWildcard(select.getSelectList());
            if (collectionMapping.hasListWildcard()) {
                Object row[] = new Object[numColumns];
                List<Integer> listWildcardColumnIndices = collectionMapping.getColumnIndicesWithListWildcard();
                List<Integer> listWildcardJsonPathIndices = collectionMapping.getListWildcardJsonPathIndices();
                try {
                    while (cursor.hasNext()) {
                        Document doc = cursor.next();
                        for (Integer index : simpleColumnIndices) {
                            MongoColumnMapping col = collectionMapping.getColumnMappings().get(index);
                            row[index] = getFieldByType(doc, col.getJsonPathParsed(), col.getType(), schemaEnforcementLevel);
                        }
                        boolean foundListElement;
                        int listIndexToInject = 0;
                        do {
                            foundListElement = false;
                            for (int i=0; i<listWildcardColumnIndices.size(); i++) {
                                Integer colIndex = listWildcardColumnIndices.get(i);
                                Integer jsonPathWildcardIndex = listWildcardJsonPathIndices.get(i);
                                MongoColumnMapping col = collectionMapping.getColumnMappings().get(colIndex);
                                col.getJsonPathParsed().set(jsonPathWildcardIndex, new JsonPathListIndexElement(listIndexToInject)); // Attention: original parsed path (with wildcard) no longer available afterwards
                                row[colIndex] = getFieldByType(doc, col.getJsonPathParsed(), col.getType(), schemaEnforcementLevel);
                                foundListElement = foundListElement || (row[colIndex] != null);
                            }
                            if (foundListElement) {
                                iter.emit(row);
                                listIndexToInject++;
                            }
                        } while (foundListElement);
                    }
                } finally {
                    cursor.close();
                }
            } else {
                Object row[] = new Object[numColumns];
                try {
                    while (cursor.hasNext()) {
                        Document doc = cursor.next();
                        int i = 0;
                        for (Integer index : simpleColumnIndices) {
                            MongoColumnMapping col = collectionMapping.getColumnMappings().get(index);
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

    /**
     * Attention: If we specify several projections for a subdocument, only the last one seems to be considered
     * To keep it simple we only project on the highest level of fields.
     */
    private static Document constructProjectionFromColumnMapping(List<MongoColumnMapping> columnsMapping, SqlSelectList selectList) {
        Set<String> topLevelFieldsSeenBefore = new HashSet<>();
        Document projection = new Document();
        boolean includeId = false;
        for (MongoColumnMapping col : columnsMapping) {
            if (!selectList.isSelectStar()
                    && selectList.getExpressions().stream().noneMatch(sqlNode -> sqlNode.getType().equals(SqlNodeType.COLUMN) && ((SqlColumn)sqlNode).getName().equals(col.getColumnName()))) {
                continue; // column not in projection, i.e. skip
            }
            List<JsonPathElement> path = col.getJsonPathParsed();
            if (path.size() == 0) {
                // root element requested, we need all data and thus no projection
                return null;
            }
            if (!topLevelFieldsSeenBefore.contains(path.get(0))) {
                topLevelFieldsSeenBefore.add(path.get(0).toJsonPathString());
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

    private static Object getFieldByType(Document doc, List<JsonPathElement> jsonPath, MongoColumnMapping.MongoType expectedMongoType, SchemaEnforcementLevel schemaEnforcementLevel) throws AdapterException {
        if (jsonPath.isEmpty()) {   // user specified "$" jsonpath
            return doc.toJson();
        }
        Object curElement = doc;
        try {
            JsonPathElement curPathElement;
            for (int i = 0; i < jsonPath.size(); i++) {
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
            if (!expectedMongoType.getClassFromMongo().isInstance(curElement)) {
                return tryAutoConvertValue(expectedMongoType, curElement);
            }
            if (expectedMongoType == MongoColumnMapping.MongoType.OBJECTID) {
                return curElement.toString();
            } else if (expectedMongoType.isPrimitive()) {
                return curElement;
            } else {    // return non-primitive type as json representation
                if (expectedMongoType == DOCUMENT) {
                    return (curElement == null) ? null : ((Document)curElement).toJson();
                } else if (expectedMongoType == MongoColumnMapping.MongoType.ARRAY) {
                    return (curElement == null) ? null : JSON.serialize(curElement);
                } else {
                    throw new RuntimeException("Unknown non-primitive mongo type, should never happen: " + expectedMongoType);
                }
            }
        } catch (ClassCastException|NullPointerException e) {
            return null;
        } catch (Exception e) {
            throw new RuntimeException("Unexpected error when retrieving path " + jsonPath + " as type " + expectedMongoType + ": " + e.getMessage(), e);
        }
    }

    private static Object tryAutoConvertValue(MongoColumnMapping.MongoType expectedMongoType, Object curElement) {
        if (expectedMongoType == MongoColumnMapping.MongoType.OBJECTID) {
            return null;
        } else if (expectedMongoType.isPrimitive()) {
            if (expectedMongoType == STRING) {
                if (curElement instanceof Document) {
                    return ((Document)curElement).toJson();
                } else if (curElement instanceof List) {
                    return JSON.serialize(curElement);
                }
                return curElement.toString();
            } else if (expectedMongoType == DOUBLE && (curElement instanceof Long || curElement instanceof Integer)) {
                return curElement;
            } else if (expectedMongoType == LONG && curElement instanceof Integer) {
                return curElement;
            } else {
                return null;
            }
        } else { // document, array, objectid
            return null;
        }
    }

}
