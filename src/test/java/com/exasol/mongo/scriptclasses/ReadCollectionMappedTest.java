package com.exasol.mongo.scriptclasses;

import com.exasol.DummyExaIterator;
import com.exasol.adapter.AdapterException;
import com.exasol.adapter.metadata.*;
import com.exasol.adapter.request.PushdownRequest;
import com.exasol.adapter.sql.*;
import com.exasol.mongo.adapter.MongoAdapterProperties;
import com.google.common.collect.Lists;
import com.mongodb.MongoClient;
import com.mongodb.client.MongoDatabase;
import org.bson.Document;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.*;

import static junit.framework.TestCase.assertEquals;


/**
 * Start Mongo first: sudo docker run -p 27017:27017 --name some-mongo -d mongo
 */
public class ReadCollectionMappedTest {

    private static final String MONGO_HOST = "127.0.0.1";
    private static final String MONGO_PORT = "27017";
    private static final String MONGO_DB = "tmp_automatic_test_db";

    @BeforeClass
    public static void beforeClass() {
        createTestData();
    }

    @Test
    public void testReadMappedSimple() throws Exception {
        String mappingSpec = "{\n" +
                "    \"tables\": [\n" +
                "        {\n" +
                "            \"collectionName\": \"test\",\n" +
                "            \"tableName\": \"TEST\",\n" +
                "            \"columns\": [\n" +
                "                {\n" +
                "                    \"jsonpath\": \"field1\",\n" +
                "                    \"columnName\": \"FIELD1\",\n" +
                "                    \"type\": \"string\"\n" +
                "                },\n" +
                "                {\n" +
                "                    \"jsonpath\": \"field2.subfield1\",\n" +
                "                    \"columnName\": \"SUBFIELD1\",\n" +
                "                    \"type\": \"string\"\n" +
                "                },\n" +
                "                {\n" +
                "                    \"jsonpath\": \"field2.subfield2\",\n" +
                "                    \"columnName\": \"SUBFIELD1_FIRST\",\n" +
                "                    \"type\": \"array\"\n" +
                "                }\n" +
                "            ]\n" +
                "        }\n" +
                "    ]\n" +
                "}";
        DummyExaIterator iter = new DummyExaIterator();
        ReadCollectionMapped.readMapped(iter, dummyPushdownRequest(mappingSpec));
        assertEquals(1, iter.getEmittedRows().size());
        assertEquals(Lists.newArrayList("val1", "val2", "[ { \"attr\" : \"val3\"} , { \"attr\" : \"val4\"}]"), iter.getEmittedRows().get(0));
    }

    @Test
    public void testReadMappedSimpleWithRoot() throws Exception {
        String mappingSpec = "{\n" +
                "    \"tables\": [\n" +
                "        {\n" +
                "            \"collectionName\": \"test\",\n" +
                "            \"tableName\": \"TEST\",\n" +
                "            \"columns\": [\n" +
                "                {\n" +
                "                    \"jsonpath\": \"field1\",\n" +
                "                    \"columnName\": \"FIELD1\",\n" +
                "                    \"type\": \"string\"\n" +
                "                },\n" +
                "                {\n" +
                "                    \"jsonpath\": \"field2.subfield1\",\n" +
                "                    \"columnName\": \"SUBFIELD1\",\n" +
                "                    \"type\": \"string\"\n" +
                "                },\n" +
                "                {\n" +
                "                    \"jsonpath\": \"$\",\n" +
                "                    \"columnName\": \"ALL\",\n" +
                "                    \"type\": \"document\"\n" +
                "                }\n" +
                "            ]\n" +
                "        }\n" +
                "    ]\n" +
                "}";
        DummyExaIterator iter = new DummyExaIterator();
        ReadCollectionMapped.readMapped(iter, dummyPushdownRequest(mappingSpec));
        assertEquals(1, iter.getEmittedRows().size());
        assertEquals("val1", iter.getEmittedRows().get(0).get(0));
        assertEquals("val2", iter.getEmittedRows().get(0).get(1));
        assertEquals("{ \"_id\" : {  }, \"field1\" : \"val1\", \"field2\" : { \"subfield1\" : \"val2\", \"subfield2\" : [{ \"attr\" : \"val3\" }, { \"attr\" : \"val4\" }] }, \"field3\" : [\"val5\", \"val6\", \"val7\"] }", iter.getEmittedRows().get(0).get(2).toString().replaceFirst("\"\\$oid\" : \"[\\d|a-z]+\"",""));
    }

    @Test
    public void testReadMappedListWildcard() throws Exception {
        String mappingSpec = "{\n" +
                "    \"tables\": [\n" +
                "        {\n" +
                "            \"collectionName\": \"test\",\n" +
                "            \"tableName\": \"TEST\",\n" +
                "            \"columns\": [\n" +
                "                {\n" +
                "                    \"jsonpath\": \"field2.subfield1\",\n" +
                "                    \"columnName\": \"SUBFIELD1\",\n" +
                "                    \"type\": \"string\"\n" +
                "                },\n" +
                "                {\n" +
                "                    \"jsonpath\": \"field2.subfield2[*].attr\",\n" +
                "                    \"columnName\": \"SUBFIELD1_ALL_ATTR\",\n" +
                "                    \"type\": \"string\"\n" +
                "                }\n" +
                "            ]\n" +
                "        }\n" +
                "    ]\n" +
                "}";
        DummyExaIterator iter = new DummyExaIterator();
        ReadCollectionMapped.readMapped(iter, dummyPushdownRequest(mappingSpec));
        assertEquals(2, iter.getEmittedRows().size());
        assertEquals(Lists.newArrayList("val2", "val3"), iter.getEmittedRows().get(0));
        assertEquals(Lists.newArrayList("val2", "val4"), iter.getEmittedRows().get(1));
    }

    @Test
    public void testReadMappedTwoListWildcards() throws Exception {
        String mappingSpec = "{\n" +
                "    \"tables\": [\n" +
                "        {\n" +
                "            \"collectionName\": \"test\",\n" +
                "            \"tableName\": \"TEST\",\n" +
                "            \"columns\": [\n" +
                "                {\n" +
                "                    \"jsonpath\": \"field2.subfield1\",\n" +
                "                    \"columnName\": \"SUBFIELD1\",\n" +
                "                    \"type\": \"string\"\n" +
                "                },\n" +
                "                {\n" +
                "                    \"jsonpath\": \"field2.subfield2[*].attr\",\n" +
                "                    \"columnName\": \"SUBFIELD1_ALL_ATTR\",\n" +
                "                    \"type\": \"string\"\n" +
                "                },\n" +
                "                {\n" +
                "                    \"jsonpath\": \"field3[*]\",\n" +
                "                    \"columnName\": \"FIELD3\",\n" +
                "                    \"type\": \"string\"\n" +
                "                }\n" +
                "            ]\n" +
                "        }\n" +
                "    ]\n" +
                "}";
        DummyExaIterator iter = new DummyExaIterator();
        ReadCollectionMapped.readMapped(iter, dummyPushdownRequest(mappingSpec));
        assertEquals(3, iter.getEmittedRows().size());
        assertEquals(Lists.newArrayList("val2", "val3", "val5"), iter.getEmittedRows().get(0));
        assertEquals(Lists.newArrayList("val2", "val4", "val6"), iter.getEmittedRows().get(1));
        assertEquals(Lists.newArrayList("val2", null, "val7"), iter.getEmittedRows().get(2));
    }

    @Test(expected= AdapterException.class)
    public void testUnsupportedMultipleListWildcards() throws Exception {
        String mappingSpec = "{\n" +
                "    \"tables\": [\n" +
                "        {\n" +
                "            \"collectionName\": \"test\",\n" +
                "            \"tableName\": \"TEST\",\n" +
                "            \"columns\": [\n" +
                "                {\n" +
                "                    \"jsonpath\": \"any1[*].any2[*].attr\",\n" +
                "                    \"columnName\": \"ANY\",\n" +
                "                    \"type\": \"string\"\n" +
                "                }\n" +
                "            ]\n" +
                "        }\n" +
                "    ]\n" +
                "}";
        DummyExaIterator iter = new DummyExaIterator();
        ReadCollectionMapped.readMapped(iter, dummyPushdownRequest(mappingSpec));
    }

    @Test
    public void testReadMappedInvalidTypes() throws Exception {
        String mappingSpec = "{\n" +
                "    \"tables\": [\n" +
                "        {\n" +
                "            \"collectionName\": \"test\",\n" +
                "            \"tableName\": \"TEST\",\n" +
                "            \"columns\": [\n" +
                "                {\n" +
                "                    \"jsonpath\": \"field1\",\n" +
                "                    \"columnName\": \"FIELD1\",\n" +
                "                    \"type\": \"integer\"\n" +         // wrong type (1st level)
                "                },\n" +
                "                {\n" +
                "                    \"jsonpath\": \"field2.subfield2\",\n" + // wrong type (leaf of nested)
                "                    \"columnName\": \"SUBFIELD2\",\n" +
                "                    \"type\": \"string\"\n" +
                "                },\n" +
                "                {\n" +
                "                    \"jsonpath\": \"field2.subfield1.nonexisting\",\n" +  // wrong type (in middle of path)
                "                    \"columnName\": \"NONEXISTING1\",\n" +
                "                    \"type\": \"string\"\n" +
                "                }\n" +
                "            ]\n" +
                "        }\n" +
                "    ]\n" +
                "}";
        DummyExaIterator iter = new DummyExaIterator();
        ReadCollectionMapped.readMapped(iter, dummyPushdownRequest(mappingSpec));
        assertEquals(1, iter.getEmittedRows().size());
        assertEquals(Lists.newArrayList(null, "[ { \"attr\" : \"val3\"} , { \"attr\" : \"val4\"}]", null), iter.getEmittedRows().get(0));
    }

    @Test
    public void testReadMappedToString() throws Exception {
        String mappingSpec = "{\n" +
                "    \"tables\": [\n" +
                "        {\n" +
                "            \"collectionName\": \"test\",\n" +
                "            \"tableName\": \"TEST\",\n" +
                "            \"columns\": [\n" +
                "                {\n" +
                "                    \"jsonpath\": \"field2.subfield2\",\n" + // wrong type (leaf of nested)
                "                    \"columnName\": \"SUBFIELD2\",\n" +
                "                    \"type\": \"array\"\n" +
                "                },\n" +
                "                {\n" +
                "                    \"jsonpath\": \"field2.subfield2[0]\",\n" +  // wrong type (in middle of path)
                "                    \"columnName\": \"NONEXISTING1\",\n" +
                "                    \"type\": \"string\"\n" +
                "                }\n" +
                "            ]\n" +
                "        }\n" +
                "    ]\n" +
                "}";
        DummyExaIterator iter = new DummyExaIterator();
        ReadCollectionMapped.readMapped(iter, dummyPushdownRequest(mappingSpec));
        assertEquals(1, iter.getEmittedRows().size());
        assertEquals(Lists.newArrayList("[ { \"attr\" : \"val3\"} , { \"attr\" : \"val4\"}]", "{ \"attr\" : \"val3\" }"), iter.getEmittedRows().get(0));
    }

    private static void createTestData() {
        MongoClient mongoClient = new MongoClient(MONGO_HOST, Integer.parseInt(MONGO_PORT));
        MongoDatabase database = mongoClient.getDatabase(MONGO_DB);
        if (database.listCollectionNames().into(new ArrayList<>()).contains("test")) {
            database.getCollection("test").drop();
        }
        database.createCollection("test");
        String json = "{\n" +
                "    \"field1\": \"val1\",\n" +
                "    \"field2\": {\n" +
                "        \"subfield1\": \"val2\",\n" +
                "        \"subfield2\": [\n" +
                "            {\n" +
                "                \"attr\": \"val3\"\n" +
                "            },\n" +
                "            {\n" +
                "                \"attr\": \"val4\"\n" +
                "            }\n" +
                "        ]\n" +
                "    },\n" +
                "    \"field3\": [\n" +
                "        \"val5\",\n" +
                "        \"val6\",\n" +
                "        \"val7\"\n" +
                "    ]\n" +
                "}";

        database.getCollection("test").insertOne(Document.parse(json));
    }

    private PushdownRequest dummyPushdownRequest(String mappingSpec) throws MetadataException {
        Map<String, String> properties = new HashMap<>();
        properties.put(MongoAdapterProperties.PROP_MONGO_HOST, MONGO_HOST);
        properties.put(MongoAdapterProperties.PROP_MONGO_PORT, MONGO_PORT);
        properties.put(MongoAdapterProperties.PROP_MONGO_DB, MONGO_DB);
        properties.put(MongoAdapterProperties.PROP_MODE, "mapped");
        properties.put(MongoAdapterProperties.PROP_MAPPING, mappingSpec);
        SchemaMetadataInfo schemaMetadataInfo = new SchemaMetadataInfo("VS_MONGO", "", properties);

        return new PushdownRequest(schemaMetadataInfo, getTestSqlNode(), Lists.newArrayList(getTestTableMetadata()));
    }

    public static SqlStatement getTestSqlNode() throws MetadataException {
        TableMetadata testTableMetadata = getTestTableMetadata();
        SqlTable fromClause = new SqlTable("TEST", testTableMetadata);
        SqlSelectList selectList = SqlSelectList.createSelectStarSelectList();
        SqlNode whereClause = null;
        SqlExpressionList groupBy = null;
        SqlNode having = null;
        SqlOrderBy orderBy = null;
        SqlLimit limit = null;
        return new SqlStatementSelect(fromClause, selectList, whereClause, groupBy, having, orderBy, limit);
    }

    public static TableMetadata getTestTableMetadata() throws MetadataException {
        List<ColumnMetadata> columns = new ArrayList<>();
        columns.add(new ColumnMetadata("DUMMYFIELD", "", DataType.createVarChar(10000, DataType.ExaCharset.UTF8), true, false, "", ""));
        TableMetadata tableMetadata = new TableMetadata("TEST", "", columns, "");
        return tableMetadata;
    }

}