package com.exasol.mongo.scriptclasses;

import com.exasol.DummyExaIterator;
import com.exasol.adapter.AdapterException;
import com.exasol.adapter.metadata.SchemaMetadataInfo;
import com.exasol.adapter.metadata.TableMetadata;
import com.exasol.adapter.request.PushdownRequest;
import com.exasol.adapter.sql.*;
import com.exasol.mongo.adapter.MongoAdapter;
import com.exasol.mongo.adapter.MongoAdapterProperties;
import com.exasol.mongo.mapping.MongoDBMappingParser;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.mongodb.MongoClient;
import com.mongodb.client.MongoDatabase;
import org.bson.Document;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static junit.framework.TestCase.assertEquals;


/**
 * Start Mongo first: sudo docker run -p 27017:27017 --name some-mongo -d mongo
 */
public class ReadCollectionMappedTest {

    public static final String MONGO_HOST = "127.0.0.1";
    public static final String MONGO_PORT = "27017";
    public static final String MONGO_DB = "tmp_automatic_test_db";

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
                "                    \"jsonPath\": \"field1\",\n" +
                "                    \"columnName\": \"FIELD1\",\n" +
                "                    \"mongoType\": \"string\"\n" +
                "                },\n" +
                "                {\n" +
                "                    \"jsonPath\": \"field2.subfield1\",\n" +
                "                    \"columnName\": \"SUBFIELD1\",\n" +
                "                    \"mongoType\": \"string\"\n" +
                "                },\n" +
                "                {\n" +
                "                    \"jsonPath\": \"field2.subfield2\",\n" +
                "                    \"columnName\": \"SUBFIELD1_FIRST\",\n" +
                "                    \"mongoType\": \"array\"\n" +
                "                }\n" +
                "            ]\n" +
                "        }\n" +
                "    ]\n" +
                "}";
        DummyExaIterator iter = new DummyExaIterator();
        SchemaMetadataInfo schemaMetadataInfo = dummySchemaMetadata(mappingSpec);
        ReadCollectionMapped.readMapped(iter, dummyPushdownRequest(getTestSqlNode("TEST", schemaMetadataInfo), schemaMetadataInfo));
        assertEquals(1, iter.getEmittedRows().size());
        assertEquals(Lists.newArrayList("val1", "val2", "[ { \"attr\" : \"val3\"} , { \"attr\" : \"val4\"}]"), iter.getEmittedRows().get(0));
    }

    @Test
    public void testReadMappedSimpleAutoMapped() throws Exception {
        String mappingSpec = "{\n" +
                "    \"tables\": [\n" +
                "        {\n" +
                "            \"collectionName\": \"test\",\n" +
                "            \"tableName\": \"TEST\",\n" +
                "            \"columns\": [\n" +
                "                {\n" +
                "                    \"jsonPath\": \"field1\",\n" +
                "                    \"columnName\": \"FIELD1\",\n" +
                "                    \"mongoType\": \"string\"\n" +
                "                },\n" +
                "                {\n" +
                "                    \"jsonPath\": \"field2.subfield1\",\n" +
                "                    \"columnName\": \"SUBFIELD1\",\n" +
                "                    \"mongoType\": \"string\"\n" +
                "                },\n" +
                "                {\n" +
                "                    \"jsonPath\": \"field2.subfield2\",\n" +
                "                    \"columnName\": \"SUBFIELD1_FIRST\",\n" +
                "                    \"mongoType\": \"array\"\n" +
                "                }\n" +
                "            ]\n" +
                "        }\n" +
                "    ]\n" +
                "}";
        DummyExaIterator iter = new DummyExaIterator();
        SchemaMetadataInfo schemaMetadataInfo = dummySchemaMetadataAutoMapped(mappingSpec);
        ReadCollectionMapped.readMapped(iter, dummyPushdownRequestAutoMapped(getTestSqlNodeAutoMapped("TEST", schemaMetadataInfo, mappingSpec), schemaMetadataInfo, mappingSpec));
        assertEquals(1, iter.getEmittedRows().size());
        assertEquals(Lists.newArrayList("val1", "val2", "[ { \"attr\" : \"val3\"} , { \"attr\" : \"val4\"}]"), iter.getEmittedRows().get(0));
    }

    @Test
    public void testReadMappedSimpleProjection() throws Exception {
        String mappingSpec = "{\n" +
                "    \"tables\": [\n" +
                "        {\n" +
                "            \"collectionName\": \"test\",\n" +
                "            \"tableName\": \"TEST\",\n" +
                "            \"columns\": [\n" +
                "                {\n" +
                "                    \"jsonPath\": \"field1\",\n" +
                "                    \"columnName\": \"FIELD1\",\n" +
                "                    \"mongoType\": \"string\"\n" +
                "                },\n" +
                "                {\n" +
                "                    \"jsonPath\": \"field2.subfield1\",\n" +
                "                    \"columnName\": \"SUBFIELD1\",\n" +
                "                    \"mongoType\": \"string\"\n" +
                "                },\n" +
                "                {\n" +
                "                    \"jsonPath\": \"field2.subfield2\",\n" +
                "                    \"columnName\": \"SUBFIELD1_FIRST\",\n" +
                "                    \"mongoType\": \"array\"\n" +
                "                }\n" +
                "            ]\n" +
                "        }\n" +
                "    ]\n" +
                "}";
        DummyExaIterator iter = new DummyExaIterator();
        SchemaMetadataInfo schemaMetadataInfo = dummySchemaMetadata(mappingSpec);
        ReadCollectionMapped.readMapped(iter, dummyPushdownRequest(getTestSqlNodeWithProjection("TEST", schemaMetadataInfo), schemaMetadataInfo));
        assertEquals(1, iter.getEmittedRows().size());
        assertEquals(Lists.newArrayList("val1", "[ { \"attr\" : \"val3\"} , { \"attr\" : \"val4\"}]"), iter.getEmittedRows().get(0));
    }

    @Test
    public void testReadMappedSimpleWithObjectId() throws Exception {
        String mappingSpec = "{\n" +
                "    \"tables\": [\n" +
                "        {\n" +
                "            \"collectionName\": \"test\",\n" +
                "            \"tableName\": \"TEST\",\n" +
                "            \"columns\": [\n" +
                "                {\n" +
                "                    \"jsonPath\": \"_id\",\n" +
                "                    \"columnName\": \"_id\",\n" +
                "                    \"mongoType\": \"string\"\n" +
                "                },\n" +
                "                {\n" +
                "                    \"jsonPath\": \"_id\",\n" +
                "                    \"columnName\": \"objectid\",\n" +
                "                    \"mongoType\": \"string\"\n" +
                "                },\n" +
                "                {\n" +
                "                    \"jsonPath\": \"field1\",\n" +
                "                    \"columnName\": \"FIELD1\",\n" +
                "                    \"mongoType\": \"string\"\n" +
                "                }\n" +
                "            ]\n" +
                "        }\n" +
                "    ]\n" +
                "}";
        DummyExaIterator iter = new DummyExaIterator();
        SchemaMetadataInfo schemaMetadataInfo = dummySchemaMetadata(mappingSpec);
        ReadCollectionMapped.readMapped(iter, dummyPushdownRequest(getTestSqlNode("TEST", schemaMetadataInfo), schemaMetadataInfo));
        assertEquals(1, iter.getEmittedRows().size());
        assertEquals("val1", iter.getEmittedRows().get(0).get(2));
        assertEquals(24, ((String) iter.getEmittedRows().get(0).get(0)).length());
        assertEquals(iter.getEmittedRows().get(0).get(0), iter.getEmittedRows().get(0).get(1));
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
                "                    \"jsonPath\": \"field1\",\n" +
                "                    \"columnName\": \"FIELD1\",\n" +
                "                    \"mongoType\": \"string\"\n" +
                "                },\n" +
                "                {\n" +
                "                    \"jsonPath\": \"field2.subfield1\",\n" +
                "                    \"columnName\": \"SUBFIELD1\",\n" +
                "                    \"mongoType\": \"string\"\n" +
                "                },\n" +
                "                {\n" +
                "                    \"jsonPath\": \"$\",\n" +
                "                    \"columnName\": \"ALL\",\n" +
                "                    \"mongoType\": \"document\"\n" +
                "                }\n" +
                "            ]\n" +
                "        }\n" +
                "    ]\n" +
                "}";
        DummyExaIterator iter = new DummyExaIterator();
        SchemaMetadataInfo schemaMetadataInfo = dummySchemaMetadata(mappingSpec);
        ReadCollectionMapped.readMapped(iter, dummyPushdownRequest(getTestSqlNode("TEST", schemaMetadataInfo), schemaMetadataInfo));
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
                "                    \"jsonPath\": \"field2.subfield1\",\n" +
                "                    \"columnName\": \"SUBFIELD1\",\n" +
                "                    \"mongoType\": \"string\"\n" +
                "                },\n" +
                "                {\n" +
                "                    \"jsonPath\": \"field2.subfield2[*].attr\",\n" +
                "                    \"columnName\": \"SUBFIELD1_ALL_ATTR\",\n" +
                "                    \"mongoType\": \"string\"\n" +
                "                }\n" +
                "            ]\n" +
                "        }\n" +
                "    ]\n" +
                "}";
        DummyExaIterator iter = new DummyExaIterator();
        SchemaMetadataInfo schemaMetadataInfo = dummySchemaMetadata(mappingSpec);
        ReadCollectionMapped.readMapped(iter, dummyPushdownRequest(getTestSqlNode("TEST", schemaMetadataInfo), schemaMetadataInfo));
        assertEquals(2, iter.getEmittedRows().size());
        assertEquals(Lists.newArrayList("val2", "val3"), iter.getEmittedRows().get(0));
        assertEquals(Lists.newArrayList("val2", "val4"), iter.getEmittedRows().get(1));
    }

    @Test
    public void testReadMappedListWildcardEmptyList() throws Exception {
        String mappingSpec = "{\n" +
                "    \"tables\": [\n" +
                "        {\n" +
                "            \"collectionName\": \"test\",\n" +
                "            \"tableName\": \"TEST\",\n" +
                "            \"columns\": [\n" +
                "                {\n" +
                "                    \"jsonPath\": \"field2.subfield1\",\n" +
                "                    \"columnName\": \"SUBFIELD1\",\n" +
                "                    \"mongoType\": \"string\"\n" +
                "                },\n" +
                "                {\n" +
                "                    \"jsonPath\": \"field2.nonexisting[*]\",\n" +
                "                    \"columnName\": \"NONEXISTING\",\n" +
                "                    \"mongoType\": \"string\"\n" +
                "                }\n" +
                "            ]\n" +
                "        }\n" +
                "    ]\n" +
                "}";
        DummyExaIterator iter = new DummyExaIterator();
        SchemaMetadataInfo schemaMetadataInfo = dummySchemaMetadata(mappingSpec);
        ReadCollectionMapped.readMapped(iter, dummyPushdownRequest(getTestSqlNode("TEST", schemaMetadataInfo), schemaMetadataInfo));
        assertEquals(1, iter.getEmittedRows().size());
        assertEquals(Lists.newArrayList("val2", null), iter.getEmittedRows().get(0));
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
                "                    \"jsonPath\": \"field2.subfield1\",\n" +
                "                    \"columnName\": \"SUBFIELD1\",\n" +
                "                    \"mongoType\": \"string\"\n" +
                "                },\n" +
                "                {\n" +
                "                    \"jsonPath\": \"field2.subfield2[*].attr\",\n" +
                "                    \"columnName\": \"SUBFIELD1_ALL_ATTR\",\n" +
                "                    \"mongoType\": \"string\"\n" +
                "                },\n" +
                "                {\n" +
                "                    \"jsonPath\": \"field3[*]\",\n" +
                "                    \"columnName\": \"FIELD3\",\n" +
                "                    \"mongoType\": \"string\"\n" +
                "                }\n" +
                "            ]\n" +
                "        }\n" +
                "    ]\n" +
                "}";
        DummyExaIterator iter = new DummyExaIterator();
        SchemaMetadataInfo schemaMetadataInfo = dummySchemaMetadata(mappingSpec);
        ReadCollectionMapped.readMapped(iter, dummyPushdownRequest(getTestSqlNode("TEST", schemaMetadataInfo), schemaMetadataInfo));
        assertEquals(3, iter.getEmittedRows().size());
        assertEquals(Lists.newArrayList("val2", "val3", "val5"), iter.getEmittedRows().get(0));
        assertEquals(Lists.newArrayList("val2", "val4", "val6"), iter.getEmittedRows().get(1));
        assertEquals(Lists.newArrayList("val2", null, "val7"), iter.getEmittedRows().get(2));
    }

    @Test
    public void testReadMappedListWildcardProjection() throws Exception {
        String mappingSpec = "{\n" +
                "    \"tables\": [\n" +
                "        {\n" +
                "            \"collectionName\": \"test\",\n" +
                "            \"tableName\": \"TEST\",\n" +
                "            \"columns\": [\n" +
                "                {\n" +
                "                    \"jsonPath\": \"field2.subfield1\",\n" +
                "                    \"columnName\": \"SUBFIELD1\",\n" +
                "                    \"mongoType\": \"string\"\n" +
                "                },\n" +
                "                {\n" +
                "                    \"jsonPath\": \"field2.subfield2[*].attr\",\n" +
                "                    \"columnName\": \"SUBFIELD1_ALL_ATTR\",\n" +
                "                    \"mongoType\": \"string\"\n" +
                "                },\n" +
                "                {\n" +
                "                    \"jsonPath\": \"field3[*]\",\n" +
                "                    \"columnName\": \"FIELD3\",\n" +
                "                    \"mongoType\": \"string\"\n" +
                "                }\n" +
                "            ]\n" +
                "        }\n" +
                "    ]\n" +
                "}";
        DummyExaIterator iter = new DummyExaIterator();
        SchemaMetadataInfo schemaMetadataInfo = dummySchemaMetadata(mappingSpec);
        ReadCollectionMapped.readMapped(iter, dummyPushdownRequest(getTestSqlNodeWithProjection("TEST", schemaMetadataInfo), schemaMetadataInfo));
        assertEquals(3, iter.getEmittedRows().size());
        assertEquals(Lists.newArrayList("val2", "val5"), iter.getEmittedRows().get(0));
        assertEquals(Lists.newArrayList("val2", "val6"), iter.getEmittedRows().get(1));
        assertEquals(Lists.newArrayList("val2", "val7"), iter.getEmittedRows().get(2));
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
                "                    \"jsonPath\": \"any1[*].any2[*].attr\",\n" +
                "                    \"columnName\": \"ANY\",\n" +
                "                    \"mongoType\": \"string\"\n" +
                "                }\n" +
                "            ]\n" +
                "        }\n" +
                "    ]\n" +
                "}";
        DummyExaIterator iter = new DummyExaIterator();
        SchemaMetadataInfo schemaMetadataInfo = dummySchemaMetadata(mappingSpec);
        ReadCollectionMapped.readMapped(iter, dummyPushdownRequest(getTestSqlNode("TEST", schemaMetadataInfo), schemaMetadataInfo));
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
                "                    \"jsonPath\": \"field1\",\n" +
                "                    \"columnName\": \"FIELD1\",\n" +
                "                    \"mongoType\": \"integer\"\n" +         // wrong type (1st level)
                "                },\n" +
                "                {\n" +
                "                    \"jsonPath\": \"field2.subfield2\",\n" + // wrong type (leaf of nested)
                "                    \"columnName\": \"SUBFIELD2\",\n" +
                "                    \"mongoType\": \"string\"\n" +
                "                },\n" +
                "                {\n" +
                "                    \"jsonPath\": \"field2.subfield1.nonexisting\",\n" +  // wrong type (in middle of path)
                "                    \"columnName\": \"NONEXISTING1\",\n" +
                "                    \"mongoType\": \"string\"\n" +
                "                }\n" +
                "            ]\n" +
                "        }\n" +
                "    ]\n" +
                "}";
        DummyExaIterator iter = new DummyExaIterator();
        SchemaMetadataInfo schemaMetadataInfo = dummySchemaMetadata(mappingSpec);
        ReadCollectionMapped.readMapped(iter, dummyPushdownRequest(getTestSqlNode("TEST", schemaMetadataInfo), schemaMetadataInfo));
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
                "                    \"jsonPath\": \"field2.subfield2\",\n" + // wrong type (leaf of nested)
                "                    \"columnName\": \"SUBFIELD2\",\n" +
                "                    \"mongoType\": \"array\"\n" +
                "                },\n" +
                "                {\n" +
                "                    \"jsonPath\": \"field2.subfield2[0]\",\n" +  // wrong type (in middle of path)
                "                    \"columnName\": \"NONEXISTING1\",\n" +
                "                    \"mongoType\": \"string\"\n" +
                "                }\n" +
                "            ]\n" +
                "        }\n" +
                "    ]\n" +
                "}";
        DummyExaIterator iter = new DummyExaIterator();
        SchemaMetadataInfo schemaMetadataInfo = dummySchemaMetadata(mappingSpec);
        ReadCollectionMapped.readMapped(iter, dummyPushdownRequest(getTestSqlNode("TEST", schemaMetadataInfo), schemaMetadataInfo));
        assertEquals(1, iter.getEmittedRows().size());
        assertEquals(Lists.newArrayList("[ { \"attr\" : \"val3\"} , { \"attr\" : \"val4\"}]", "{ \"attr\" : \"val3\" }"), iter.getEmittedRows().get(0));
    }

    @Test
    public void testOrderBy() throws Exception {
        String mappingSpec = "{\n" +
                "    \"tables\": [\n" +
                "        {\n" +
                "            \"collectionName\": \"testMulti\",\n" +
                "            \"tableName\": \"TEST_MULTI\",\n" +
                "            \"columns\": [\n" +
                "                {\n" +
                "                    \"jsonPath\": \"field2.subfield1\",\n" +
                "                    \"columnName\": \"SUBFIELD1\",\n" +
                "                    \"mongoType\": \"string\"\n" +
                "                }\n" +
                "            ]\n" +
                "        }\n" +
                "    ]\n" +
                "}";
        DummyExaIterator iter = new DummyExaIterator();
        SchemaMetadataInfo schemaMetadataInfo = dummySchemaMetadata(mappingSpec);
        ReadCollectionMapped.readMapped(iter, dummyPushdownRequest(getTestSqlNodeWithOrderBy("TEST_MULTI", schemaMetadataInfo), schemaMetadataInfo));
        assertEquals(4, iter.getEmittedRows().size());
        List<String> nullList = new ArrayList<>();
        nullList.add(null);
        assertEquals(nullList, iter.getEmittedRows().get(0));
        assertEquals(Lists.newArrayList("subval1"), iter.getEmittedRows().get(1));
        assertEquals(Lists.newArrayList("subval2"), iter.getEmittedRows().get(2));
    }

    public static void createTestData() {
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

        if (database.listCollectionNames().into(new ArrayList<>()).contains("testMulti")) {
            database.getCollection("testMulti").drop();
        }
        database.createCollection("testMulti");
        database.getCollection("testMulti").insertOne(Document.parse("{\n" +
                "    \"field1\": \"val1\",\n" +
                "    \"field2\": {\n" +
                "        \"subfield1\": \"subval1\"\n" +
                "    }\n" +
                "}"));
        database.getCollection("testMulti").insertOne(Document.parse("{\n" +
                "    \"field1\": \"val2\",\n" +
                "    \"field2\": {\n" +
                "        \"subfield1\": \"subval2\"\n" +
                "    }\n" +
                "}"));
        database.getCollection("testMulti").insertOne(Document.parse("{\n" +
                "    \"field1\": \"val2\",\n" +
                "    \"field2\": {\n" +
                "        \"subfield1\": \"subval3\"\n" +
                "    }\n" +
                "}"));
        database.getCollection("testMulti").insertOne(Document.parse("{\n" +
                "    \"field1\": \"val3\"\n" +
                "}"));
    }

    private SchemaMetadataInfo dummySchemaMetadata(String mappingSpec) {
        Map<String, String> properties = new HashMap<>();
        properties.put(MongoAdapterProperties.PROP_MONGO_HOST, MONGO_HOST);
        properties.put(MongoAdapterProperties.PROP_MONGO_PORT, MONGO_PORT);
        properties.put(MongoAdapterProperties.PROP_MONGO_DB, MONGO_DB);
        properties.put(MongoAdapterProperties.PROP_MODE, "mapped");
        properties.put(MongoAdapterProperties.PROP_MAPPING, mappingSpec);
        return new SchemaMetadataInfo("VS_MONGO", "", properties);
    }

    private SchemaMetadataInfo dummySchemaMetadataAutoMapped(String mappingSpec) {
        Map<String, String> properties = new HashMap<>();
        properties.put(MongoAdapterProperties.PROP_MONGO_HOST, MONGO_HOST);
        properties.put(MongoAdapterProperties.PROP_MONGO_PORT, MONGO_PORT);
        properties.put(MongoAdapterProperties.PROP_MONGO_DB, MONGO_DB);
        properties.put(MongoAdapterProperties.PROP_MODE, "auto_mapped");
        properties.put(MongoAdapterProperties.PROP_AUTO_MAPPING_SAMPLE_SIZE, "100");
        return new SchemaMetadataInfo("VS_MONGO", mappingSpec, properties);
    }

    private PushdownRequest dummyPushdownRequest(SqlStatement select, SchemaMetadataInfo schemaMetadataInfo) throws AdapterException {
        List<TableMetadata> tablesMetadata = MongoAdapter.getTableMetadataForMapping(new MongoAdapterProperties(schemaMetadataInfo.getProperties()).getMapping());
        return new PushdownRequest(schemaMetadataInfo, select, tablesMetadata);
    }

    private PushdownRequest dummyPushdownRequestAutoMapped(SqlStatement select, SchemaMetadataInfo schemaMetadataInfo, String mapping) throws AdapterException {
        List<TableMetadata> tablesMetadata = MongoAdapter.getTableMetadataForMapping(MongoDBMappingParser.parse(mapping));
        return new PushdownRequest(schemaMetadataInfo, select, tablesMetadata);
    }

    public static SqlStatement getTestSqlNodeAutoMapped(String tableName, SchemaMetadataInfo schemaMetadataInfo, String mapping) throws AdapterException {
        TableMetadata testTableMetadata = MongoAdapter.getTableMetadataForMapping(MongoDBMappingParser.parse(mapping)).get(0);
        SqlTable fromClause = new SqlTable(tableName, testTableMetadata);
        SqlSelectList selectList = SqlSelectList.createSelectStarSelectList();
        SqlNode whereClause = null;
        SqlExpressionList groupBy = null;
        SqlNode having = null;
        SqlOrderBy orderBy = null;
        SqlLimit limit = null;
        return new SqlStatementSelect(fromClause, selectList, whereClause, groupBy, having, orderBy, limit);
    }

    public static SqlStatement getTestSqlNode(String tableName, SchemaMetadataInfo schemaMetadataInfo) throws AdapterException {
        TableMetadata testTableMetadata = MongoAdapter.getTableMetadataForMapping(new MongoAdapterProperties(schemaMetadataInfo.getProperties()).getMapping()).get(0);
        SqlTable fromClause = new SqlTable(tableName, testTableMetadata);
        SqlSelectList selectList = SqlSelectList.createSelectStarSelectList();
        SqlNode whereClause = null;
        SqlExpressionList groupBy = null;
        SqlNode having = null;
        SqlOrderBy orderBy = null;
        SqlLimit limit = null;
        return new SqlStatementSelect(fromClause, selectList, whereClause, groupBy, having, orderBy, limit);
    }

    public static SqlStatement getTestSqlNodeWithOrderBy(String tableName, SchemaMetadataInfo schemaMetadataInfo) throws AdapterException {
        TableMetadata testTableMetadata = MongoAdapter.getTableMetadataForMapping(new MongoAdapterProperties(schemaMetadataInfo.getProperties()).getMapping()).get(0);
        SqlTable fromClause = new SqlTable(tableName, testTableMetadata);
        SqlSelectList selectList = SqlSelectList.createSelectStarSelectList();
        SqlNode whereClause = null;
        SqlExpressionList groupBy = null;
        SqlNode having = null;
        SqlOrderBy orderBy = new SqlOrderBy(ImmutableList.of(new SqlColumn(0, testTableMetadata.getColumns().get(0))), ImmutableList.of(true), ImmutableList.of(true));
        SqlLimit limit = null;
        return new SqlStatementSelect(fromClause, selectList, whereClause, groupBy, having, orderBy, limit);
    }

    public static SqlStatement getTestSqlNodeWithProjection(String tableName, SchemaMetadataInfo schemaMetadataInfo) throws AdapterException {
        TableMetadata testTableMetadata = MongoAdapter.getTableMetadataForMapping(new MongoAdapterProperties(schemaMetadataInfo.getProperties()).getMapping()).get(0);
        SqlTable fromClause = new SqlTable(tableName, testTableMetadata);
        SqlSelectList selectList = SqlSelectList.createRegularSelectList(ImmutableList.of(
                new SqlColumn(0, testTableMetadata.getColumns().get(0)),
                new SqlColumn(1, testTableMetadata.getColumns().get(2))
                ));
        SqlNode whereClause = null;
        SqlExpressionList groupBy = null;
        SqlNode having = null;
        SqlOrderBy orderBy = new SqlOrderBy(ImmutableList.of(new SqlColumn(0, testTableMetadata.getColumns().get(0))), ImmutableList.of(true), ImmutableList.of(true));
        SqlLimit limit = null;
        return new SqlStatementSelect(fromClause, selectList, whereClause, groupBy, having, orderBy, limit);
    }

}