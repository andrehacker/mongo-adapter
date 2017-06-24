package com.exasol.mongo.adapter;

import com.exasol.ExaMetadata;
import com.exasol.adapter.AdapterException;
import com.exasol.adapter.capabilities.*;
import com.exasol.adapter.json.RequestJsonParser;
import com.exasol.adapter.json.ResponseJsonSerializer;
import com.exasol.adapter.metadata.*;
import com.exasol.adapter.request.*;
import com.exasol.adapter.sql.*;
import com.exasol.mongo.deriveschema.DeriveSchema;
import com.exasol.mongo.mapping.*;
import com.exasol.utils.JsonHelper;
import com.google.common.base.Joiner;
import com.mongodb.MongoClient;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;

import java.util.ArrayList;
import java.util.List;

import static com.exasol.adapter.sql.AggregateFunction.COUNT;
import static com.exasol.mongo.adapter.MongoAdapterProperties.MongoMappingMode.AUTO_MAPPED;
import static com.exasol.mongo.adapter.MongoAdapterProperties.MongoMappingMode.JSON;
import static com.exasol.mongo.adapter.MongoAdapterProperties.MongoMappingMode.MAPPED;

/**
 * Next steps
 * - projection pushdown
 * - schema auto discovery
 */
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

    private static String handleSetProperty(SetPropertiesRequest request, ExaMetadata meta) throws Exception {
        SchemaMetadata newMetadata = null;
        if (MongoAdapterProperties.isRefreshNeeded(request.getProperties())) {
            newMetadata = readMetadata(request.getSchemaMetadataInfo(), meta);
        }
        return ResponseJsonSerializer.makeSetPropertiesResponse(newMetadata);
    }

    private static String handleGetCapabilities(GetCapabilitiesRequest request) {
        Capabilities capabilities = new Capabilities();
        // TODO Bug: AND is pushed down even though we didn't have the capability (ltt)
        // TODO Bug: Could delete a schema containing an adapter script which was used in a virtual schema (ltt)
        capabilities.supportMainCapability(MainCapability.LIMIT);
        capabilities.supportMainCapability(MainCapability.ORDER_BY_COLUMN);
        capabilities.supportMainCapability(MainCapability.FILTER_EXPRESSIONS);
        capabilities.supportMainCapability(MainCapability.AGGREGATE_SINGLE_GROUP);
        capabilities.supportMainCapability(MainCapability.SELECTLIST_PROJECTION);
        capabilities.supportMainCapability(MainCapability.SELECTLIST_EXPRESSIONS);      // TODO COMMON: Problem: This is required for COUNT(*) pushdown, but I don't want to allow it for other expressions basically! ORDER BY FALSE triggers this too
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
        capabilities.supportPredicate(PredicateCapability.REGEXP_LIKE);
        // capabilities.supportPredicate(PredicateCapability.LIKE);  // not yet working
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
        MongoDBMapping mapping = getMappingDuringPushdown(properties, request);
        MongoCollectionMapping collectionMapping = mapping.getCollectionMappingByTableName(tableName);
        List<String> arguments = new ArrayList<>();
        arguments.add("'" + jsonRequest.replace("'", "''") + "'");

        List<String> emitColumns = new ArrayList<>();
        if (isCountStar(select.getSelectList())) {
            emitColumns.add("COUNT DECIMAL(36,0)");
        } else {
            if (select.getSelectList().isSelectStar()) {
                for (MongoColumnMapping columnMapping : collectionMapping.getColumnMappings()) {
                    emitColumns.add("\"" + columnMapping.getColumnName() + "\" " + mongoTypeToExasolType(columnMapping.getType()).toString());
                }
            } else {
                for (SqlNode expression : select.getSelectList().getExpressions()) {
                    SqlColumn column = (SqlColumn)expression;
                    MongoColumnMapping columnMapping = collectionMapping.getColumnMappings().stream().filter(mongoColumnMapping -> mongoColumnMapping.getColumnName().equals(column.getName())).findFirst().get();
                    emitColumns.add("\"" + columnMapping.getColumnName() + "\" " + mongoTypeToExasolType(columnMapping.getType()).toString());
                }
            }
        }

        builder.append("select " + meta.getScriptSchema() + ".READ_COLLECTION_MAPPED(");
        builder.append(Joiner.on(", ").join(arguments));
        builder.append(") emits (");
        builder.append(Joiner.on(", ").join(emitColumns));
        builder.append(")");
        return ResponseJsonSerializer.makePushdownResponse(builder.toString());
    }

    public static MongoDBMapping getMappingDuringPushdown(MongoAdapterProperties properties, PushdownRequest request) throws AdapterException {
        if (properties.getMappingMode() == MAPPED) {
            return properties.getMapping();
        } else if (properties.getMappingMode() == AUTO_MAPPED) {
            return MongoDBMappingParser.parse(request.getSchemaMetadataInfo().getAdapterNotes());
        } else {
            return MongoDBMapping.constructDefaultMapping(request.getInvolvedTablesMetadata());
        }
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
                if (selectList.getExpressions().stream().anyMatch(sqlNode -> !sqlNode.getType().equals(SqlNodeType.COLUMN))) {
                    throw new AdapterException("Unsupported pushdown of select list: " + selectList.toSimpleSql());
                }
                return false;
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
        String schemaAdapterNotes = "";
        if (properties.getMappingMode() == JSON) {
            MongoCursor<String> cursor = database.listCollectionNames().iterator();
            try {
                while (cursor.hasNext()) {
                    String collectionName = cursor.next();
                    tables.add(mapCollectionToSimpleTable(collectionName));
                }
            } finally {
                cursor.close();
            }
        } else if (properties.getMappingMode() == AUTO_MAPPED) {
            MongoDBMapping mapping = DeriveSchema.deriveSchema(properties);
            tables = getTableMetadataForMapping(mapping);
            schemaAdapterNotes = MongoDBMappingSerializer.serialize(mapping);
        } else {  // MAPPED
            tables = getTableMetadataForMapping(properties.getMapping());
        }
        return new SchemaMetadata(schemaAdapterNotes, tables);
    }

    public static List<TableMetadata> getTableMetadataForMapping(MongoDBMapping mapping) throws AdapterException {
        List<TableMetadata> tables = new ArrayList<>();
        for (MongoCollectionMapping collectionMapping : mapping.getCollectionMappings()) {
            List<ColumnMetadata> columns = new ArrayList<>();
            for (MongoColumnMapping columnMapping : collectionMapping.getColumnMappings()) {
                columns.add(new ColumnMetadata(columnMapping.getColumnName(), "", mongoTypeToExasolType(columnMapping.getType()), true, false, "", ""));
            }
            tables.add(new TableMetadata(collectionMapping.getTableName(), "", columns, ""));
        }
        return tables;
    }

    private static TableMetadata mapCollectionToSimpleTable(String collectionName) throws MetadataException {
        List<ColumnMetadata> columns = new ArrayList<>();
        columns.add(new ColumnMetadata("OBJECTID", "", DataType.createVarChar(24, DataType.ExaCharset.UTF8),false,false,"", ""));
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
