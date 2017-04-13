package com.exasol.mongo.adapter;

import com.exasol.ExaMetadata;
import com.exasol.adapter.AdapterException;
import com.exasol.adapter.capabilities.*;
import com.exasol.adapter.json.RequestJsonParser;
import com.exasol.adapter.json.ResponseJsonSerializer;
import com.exasol.adapter.metadata.*;
import com.exasol.adapter.request.*;
import com.exasol.adapter.sql.SqlFunctionAggregate;
import com.exasol.adapter.sql.SqlNodeType;
import com.exasol.adapter.sql.SqlSelectList;
import com.exasol.adapter.sql.SqlStatementSelect;
import com.exasol.mongo.MongoCollectionMapping;
import com.exasol.mongo.MongoColumnMapping;
import com.exasol.mongo.MongoDBMapping;
import com.exasol.mongo.MongoMappingParser;
import com.exasol.utils.JsonHelper;
import com.google.common.base.Joiner;
import com.mongodb.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import org.bson.Document;

import java.util.ArrayList;
import java.util.List;

import static com.exasol.adapter.sql.AggregateFunction.COUNT;
import static com.exasol.jsonpath.JsonPathElement.Type.LIST_INDEX;
import static com.exasol.mongo.adapter.MongoAdapterProperties.MongoMappingMode.MAPPED;

public class MongoAdapter {

    public static String adapterCall(ExaMetadata meta, String input) throws Exception {
        String result = "";
        try {
            AdapterRequest request = new RequestJsonParser().parseRequest(input);
            System.out.println("----------\nAdapter Request:\n----------\n" + input);

            switch (request.getType()) {
                case CREATE_VIRTUAL_SCHEMA:
                    result = handleCreateVirtualSchema((CreateVirtualSchemaRequest)request, meta);
                    break;
                case DROP_VIRTUAL_SCHEMA:
                    result = ResponseJsonSerializer.makeDropVirtualSchemaResponse();
                    break;
                case REFRESH:
                    throw new AdapterException("Refreshing not yet supported");
                    //result = handleRefresh((RefreshRequest)request, meta);
                    //break;
                case SET_PROPERTIES:
                    result = handleSetProperty((SetPropertiesRequest)request, meta);
                    break;
                case GET_CAPABILITIES:
                    result = handleGetCapabilities((GetCapabilitiesRequest)request);
                    break;
                case PUSHDOWN:
                    result = handlePushdownRequest((PushdownRequest)request, meta, input);
                    break;
                default:
                    throw new RuntimeException("Request Type not supported: " + request.getType());
            }
            assert(result.isEmpty());
            System.out.println("----------\nResponse:\n----------\n" + JsonHelper.prettyJson(JsonHelper.getJsonObject(result)));
            return result;
        } catch (AdapterException ex) {
            throw ex;
        }
        catch (Exception ex) {
            throw ex;
            //throw new RuntimeException("Unexpected error in adapter: " + ex.getMessage() + "\nFor following request: " + input + "\nResponse: " + result, ex);
        }
    }

    private static String handleCreateVirtualSchema(CreateVirtualSchemaRequest request, ExaMetadata meta) throws Exception {
        SchemaMetadata remoteMeta = readMetadata(request.getSchemaMetadataInfo(), meta);
        return ResponseJsonSerializer.makeCreateVirtualSchemaResponse(remoteMeta);
    }

    private static String handleSetProperty(SetPropertiesRequest request, ExaMetadata meta) {
        // TODO Check consistency, check if we need to update metadata
        return ResponseJsonSerializer.makeSetPropertiesResponse(null);
    }

    private static String handleGetCapabilities(GetCapabilitiesRequest request) {
        Capabilities capabilities = new Capabilities();
        // TODO Bug: AND is pushed down even though we didn't have the capability (ltt)
        // TODO Bug: Could delete a schema containing an adapter script which was used in a virtual schema (ltt)
        capabilities.supportMainCapability(MainCapability.LIMIT);
        capabilities.supportMainCapability(MainCapability.FILTER_EXPRESSIONS);
        capabilities.supportMainCapability(MainCapability.AGGREGATE_SINGLE_GROUP);
        capabilities.supportMainCapability(MainCapability.SELECTLIST_EXPRESSIONS); // TODO COMMON: Problem: This is required for COUNT(*) pushdown, but I don't want to allow it for other expressions basically!
        capabilities.supportAggregateFunction(AggregateFunctionCapability.COUNT_STAR);
        capabilities.supportPredicate(PredicateCapability.AND);
        capabilities.supportPredicate(PredicateCapability.OR);
        capabilities.supportPredicate(PredicateCapability.NOT);
        capabilities.supportPredicate(PredicateCapability.EQUAL);
        capabilities.supportPredicate(PredicateCapability.NOTEQUAL);
        capabilities.supportPredicate(PredicateCapability.BETWEEN);
        capabilities.supportPredicate(PredicateCapability.LESS);
        capabilities.supportPredicate(PredicateCapability.LESSEQUAL);
        capabilities.supportPredicate(PredicateCapability.IN_CONSTLIST);
        capabilities.supportPredicate(PredicateCapability.IS_NULL);
        capabilities.supportPredicate(PredicateCapability.IS_NOT_NULL);
        capabilities.supportLiteral(LiteralCapability.BOOL);
        capabilities.supportLiteral(LiteralCapability.STRING);
        capabilities.supportLiteral(LiteralCapability.DOUBLE);
        // TODO DECIMAL is not fully supported, because mongo integers range only to 32bit. We should handle larger values somehow.
        capabilities.supportLiteral(LiteralCapability.EXACTNUMERIC);
        return ResponseJsonSerializer.makeGetCapabilitiesResponse(capabilities);
    }

    private static String handlePushdownRequest(PushdownRequest request, ExaMetadata meta, String jsonRequest) throws Exception {
        SqlStatementSelect select = (SqlStatementSelect)request.getSelect();
        MongoAdapterProperties properties = new MongoAdapterProperties(request.getSchemaMetadataInfo().getProperties());
        String tableName = select.getFromClause().getName();
        StringBuilder builder = new StringBuilder();
        MongoDBMapping mapping = (properties.getMappingMode() == MAPPED) ? properties.getMapping() : MongoDBMapping.constructDefaultMapping(request.getInvolvedTablesMetadata());
        MongoCollectionMapping collectionMapping = mapping.getCollectionMappingByTableName(tableName);
        List<String> arguments = new ArrayList<>();
        arguments.add("'" + jsonRequest.replace("'", "''") + "'");

        List<String> emitColumns = new ArrayList<>();
        if (isCountStar(select.getSelectList())) {
            emitColumns.add("COUNT DECIMAL(36,0)");
        } else {
            for (MongoColumnMapping columnMapping : collectionMapping.getColumnMappings()) {
                emitColumns.add(columnMapping.getColumnName() + " " + mongoTypeToExasolType(columnMapping.getType()).toString());
            }
        }

        builder.append("select MONGO_ADAPTER.READ_COLLECTION_MAPPED(");
        builder.append(Joiner.on(", ").join(arguments));
        builder.append(") emits (");
        builder.append(Joiner.on(", ").join(emitColumns));
        builder.append(")");
        return ResponseJsonSerializer.makePushdownResponse(builder.toString());
    }

    public static boolean isCountStar(SqlSelectList selectList) throws AdapterException {
        if (selectList.getExpressions() == null) {
            return false;
        } else {
            if ((selectList.getExpressions().size() == 1) && (selectList.getExpressions().get(0).getType() == SqlNodeType.FUNCTION_AGGREGATE)) {
                SqlFunctionAggregate aggFunction = (SqlFunctionAggregate) selectList.getExpressions().get(0);
                if (aggFunction.getFunction() == COUNT && aggFunction.getArguments().size() == 0) {
                    return true;
                } else {
                    throw new AdapterException("Unsupported pushdown of aggregate function in select list: " + selectList.toSimpleSql());
                }
            } else {
                throw new AdapterException("Unsupported pushdown of select list: " + selectList.toSimpleSql());
            }
        }
    }

    public static SchemaMetadata readMetadata(SchemaMetadataInfo schemaMetadataInfo, ExaMetadata meta) throws Exception {
        MongoAdapterProperties properties = new MongoAdapterProperties(schemaMetadataInfo.getProperties());
        String host = properties.getMongoHost();
        int port = properties.getMongoPort();
        String db = properties.getMongoDB();

        MongoClient mongoClient = new MongoClient( host , port );
        MongoDatabase database = mongoClient.getDatabase(db);

        List<TableMetadata> tables = new ArrayList<>();
        if (properties.getMappingMode() == MongoAdapterProperties.MongoMappingMode.JSON) {
            System.out.println("Collections:");
            MongoCursor<String> cursor = database.listCollectionNames().iterator();
            try {
                while (cursor.hasNext()) {
                    String collectionName = cursor.next();
                    tables.add(mapCollectionToSimpleTable(database, collectionName, properties));
                }
            } finally {
                cursor.close();
            }
        } else {  // MAPPED
            MongoDBMapping mapping = properties.getMapping();
            for (MongoCollectionMapping collectionMapping : mapping.getCollectionMappings()) {
                List<ColumnMetadata> columns = new ArrayList<>();
                for (MongoColumnMapping columnMapping : collectionMapping.getColumnMappings()) {
                    if (columnMapping.getJsonPathParsed().stream().anyMatch(element -> element.getType() == LIST_INDEX)) {
                        throw new AdapterException("Your mapping contains an array index, which is not yet supported.");
                    }
                    columns.add(new ColumnMetadata(columnMapping.getColumnName(), "", mongoTypeToExasolType(columnMapping.getType()), true, false, "", ""));
                }
                tables.add(new TableMetadata(collectionMapping.getTableName(), "", columns, ""));
            }
        }
        return new SchemaMetadata("", tables);
    }

    private static TableMetadata mapCollectionToSimpleTable(MongoDatabase database, String collectionName, MongoAdapterProperties properties) throws MetadataException {
        MongoCollection<Document> collection = database.getCollection(collectionName);
        // TODO infer columns
        List<ColumnMetadata> columns = new ArrayList<>();
        columns.add(new ColumnMetadata("JSON", "", DataType.createVarChar(2000000, DataType.ExaCharset.UTF8),true,false,"", ""));
        return new TableMetadata(collectionName, "", columns, "");
    }

    private static DataType mongoTypeToExasolType(MongoColumnMapping.MongoType mongoType) {
        switch (mongoType) {
            case STRING:
                return DataType.createVarChar(2000000, DataType.ExaCharset.UTF8);
            case LONG:
                return DataType.createDecimal(36,0);
            case DOUBLE:
                return DataType.createDouble();
            case OBJECTID:
                return DataType.createVarChar(24, DataType.ExaCharset.UTF8);
            case BOOLEAN:
                return DataType.createBool();
            case DATE:
                return DataType.createVarChar(100, DataType.ExaCharset.UTF8); // TODO
            case INTEGER:  // 32 bit integer
                return DataType.createDecimal(10,0);
            case DOCUMENT:
                return DataType.createVarChar(2000000, DataType.ExaCharset.UTF8);
            case ARRAY:
                return DataType.createVarChar(2000000, DataType.ExaCharset.UTF8);
            default:
                throw new RuntimeException("No mongo type to exasol type mapping, should never happen: " + mongoType);  // TODO
        }
    }
}
