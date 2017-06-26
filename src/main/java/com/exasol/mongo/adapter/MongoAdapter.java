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
import com.exasol.mongo.sql.SqlHelper;
import com.exasol.utils.JsonHelper;
import com.google.common.base.Joiner;
import com.mongodb.MongoClient;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;

import java.util.ArrayList;
import java.util.List;

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
        capabilities.supportMainCapability(MainCapability.ORDER_BY_COLUMN);
        capabilities.supportMainCapability(MainCapability.LIMIT);
        capabilities.supportMainCapability(MainCapability.SELECTLIST_PROJECTION);
        capabilities.supportMainCapability(MainCapability.SELECTLIST_EXPRESSIONS);      // TODO COMMON: Problem: This is required for COUNT(*) pushdown, but I don't want to allow it for other expressions basically! ORDER BY FALSE triggers this too
        capabilities.supportMainCapability(MainCapability.FILTER_EXPRESSIONS);
        capabilities.supportMainCapability(MainCapability.AGGREGATE_SINGLE_GROUP);
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
        // capabilities.supportPredicate(PredicateCapability.LIKE);  // no "LIKE" in MongoDB, only regex operator. However, not trivial to simulate via regexp
        capabilities.supportLiteral(LiteralCapability.BOOL);
        capabilities.supportLiteral(LiteralCapability.STRING);
        capabilities.supportLiteral(LiteralCapability.DOUBLE);
        // TODO DECIMAL is not fully supported, because mongo integers range only to 32bit. We should handle larger values somehow.
        capabilities.supportLiteral(LiteralCapability.EXACTNUMERIC);
        return ResponseJsonSerializer.makeGetCapabilitiesResponse(capabilities);
    }

    private static String handlePushdownRequest(PushdownRequest request, ExaMetadata meta, String jsonRequest) throws Exception {
        SqlStatementSelect select = (SqlStatementSelect)request.getSelect();
        String surroundingPrefix = "";
        String surroundingSuffix = "";
        if (select.getSelectList().isRequestAnyColumn()) {
            select = SqlHelper.replaceRequestAnyColumnByFirstColumn(select);
        }
        if (SqlHelper.isPushUpNeeded(select)) {
            surroundingPrefix = "SELECT " + select.getSelectList().toSimpleSql() + " FROM (";
            surroundingSuffix = " )";
            select = SqlHelper.pushUpSelectListExpressions(select);
            // Note that we still send the old json request (including expressions), but no problem: the UDF will also pushup!
            // Can be improved by serializing select to JSON (not supported currently)
        }
        MongoAdapterProperties properties = new MongoAdapterProperties(request.getSchemaMetadataInfo().getProperties());
        String tableName = select.getFromClause().getName();
        MongoDBMapping mapping = getMappingDuringPushdown(properties, request);
        MongoCollectionMapping collectionMapping = mapping.getCollectionMappingByTableName(tableName);
        List<String> arguments = new ArrayList<>();
        arguments.add("'" + jsonRequest.replace("'", "''") + "'");

        List<String> emitColumns = new ArrayList<>();
        if (SqlHelper.hasSelectListExpressions(select.getSelectList())) {
            if (SqlHelper.isCountStar(select.getSelectList())) {
                emitColumns.add("COUNT DECIMAL(36,0)");
            } else {
                throw new RuntimeException("Internal error: Should never arrive here, should be pushed up before!");
            }
        } else {
            if (select.getSelectList().isSelectStar()) {
                for (MongoColumnMapping columnMapping : collectionMapping.getColumnMappings()) {
                    emitColumns.add("\"" + columnMapping.getColumnName() + "\" " + mongoTypeToExasolType(columnMapping.getMongoType()).toString());
                }
            } else {
                for (SqlNode expression : select.getSelectList().getExpressions()) {
                    SqlColumn column = (SqlColumn)expression;
                    MongoColumnMapping columnMapping = collectionMapping.getColumnMappings().stream().filter(mongoColumnMapping -> mongoColumnMapping.getColumnName().equals(column.getName())).findFirst().get();
                    emitColumns.add("\"" + columnMapping.getColumnName() + "\" " + mongoTypeToExasolType(columnMapping.getMongoType()).toString());
                }
            }
        }

        StringBuilder builder = new StringBuilder();
        builder.append(surroundingPrefix);
        builder.append("select " + meta.getScriptSchema() + ".READ_COLLECTION_MAPPED(");
        builder.append(Joiner.on(", ").join(arguments));
        builder.append(") emits (");
        builder.append(Joiner.on(", ").join(emitColumns));
        builder.append(")");
        builder.append(surroundingSuffix);
        return ResponseJsonSerializer.makePushdownResponse(builder.toString());
    }

    public static MongoDBMapping getMappingDuringPushdown(MongoAdapterProperties properties, PushdownRequest request) throws AdapterException {
        if (properties.getMappingMode() == MAPPED) {
            return properties.getMapping();
        } else {
            assert(properties.getMappingMode() == AUTO_MAPPED || properties.getMappingMode() == JSON);
            return MongoDBMappingParser.parse(request.getSchemaMetadataInfo().getAdapterNotes());
        }
    }

    public static SchemaMetadata readMetadata(SchemaMetadataInfo schemaMetadataInfo, ExaMetadata meta) throws Exception {
        MongoAdapterProperties properties = new MongoAdapterProperties(schemaMetadataInfo.getProperties());
        String host = properties.getMongoHost();
        int port = properties.getMongoPort();
        String db = properties.getMongoDB();

        MongoClient mongoClient = new MongoClient( host , port );
        MongoDatabase database = mongoClient.getDatabase(db);

        List<TableMetadata> tables;
        String schemaAdapterNotes = "";
        if (properties.getMappingMode() == JSON) {
            List<MongoCollectionMapping> collectionMappings = new ArrayList<>();
            MongoCursor<String> cursor = database.listCollectionNames().iterator();
            try {
                while (cursor.hasNext()) {
                    String collectionName = cursor.next();
                    String tableName = collectionName;
                    List<MongoColumnMapping> columnMappings = new ArrayList<>();
                    columnMappings.add(new MongoColumnMapping("_id", "OBJECTID", MongoColumnMapping.MongoType.OBJECTID));
                    columnMappings.add(new MongoColumnMapping("$", "JSON", MongoColumnMapping.MongoType.DOCUMENT));
                    collectionMappings.add(new MongoCollectionMapping(collectionName, tableName, columnMappings));
                }
            } finally {
                cursor.close();
            }
            MongoDBMapping mapping = new MongoDBMapping(collectionMappings);
            tables = getTableMetadataForMapping(mapping);
            schemaAdapterNotes = MongoDBMappingSerializer.serialize(mapping);
        } else if (properties.getMappingMode() == AUTO_MAPPED) {
            MongoDBMapping mapping = DeriveSchema.deriveSchema(properties);
            tables = getTableMetadataForMapping(mapping);
            schemaAdapterNotes = MongoDBMappingSerializer.serialize(mapping);
        } else {    // MAPPED
            tables = getTableMetadataForMapping(properties.getMapping());
        }
        return new SchemaMetadata(schemaAdapterNotes, tables);
    }

    public static List<TableMetadata> getTableMetadataForMapping(MongoDBMapping mapping) throws AdapterException {
        List<TableMetadata> tables = new ArrayList<>();
        for (MongoCollectionMapping collectionMapping : mapping.getCollectionMappings()) {
            List<ColumnMetadata> columns = new ArrayList<>();
            for (MongoColumnMapping columnMapping : collectionMapping.getColumnMappings()) {
                columns.add(new ColumnMetadata(columnMapping.getColumnName(), columnMapping.getMongoType().name(), mongoTypeToExasolType(columnMapping.getMongoType()), true, false, "", ""));
            }
            tables.add(new TableMetadata(collectionMapping.getTableName(), "", columns, ""));
        }
        return tables;
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
