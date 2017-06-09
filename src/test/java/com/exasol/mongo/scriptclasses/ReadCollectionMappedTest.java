package com.exasol.mongo.scriptclasses;

import com.exasol.ExaDataTypeException;
import com.exasol.ExaIterationException;
import com.exasol.ExaIterator;
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

import java.math.BigDecimal;
import java.sql.Date;
import java.sql.Timestamp;
import java.util.*;
import java.util.regex.Pattern;

import static junit.framework.TestCase.assertEquals;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertTrue;


/**
 * Start Mongo first: sudo docker run -p 27017:27017 --name some-mongo -d mongo
 */
public class ReadCollectionMappedTest {

    private static final String MONGO_HOST = "127.0.0.1";
    private static final String MONGO_PORT = "27017";
    private static final String MONGO_DB = "automatic_test_db";

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
        assertTrue(iter.getEmittedRows().get(0).get(2).toString().contains(", \"field1\" : \"val1\", \"field2\" : { \"subfield1\" : \"val2\", \"subfield2\" : [{ \"attr\" : \"val3\" }, { \"attr\" : \"val4\" }] }, \"field3\" : [\"val5\", \"val6\"] }"));
    }

    @Test
    public void testReadMappedListStar() throws Exception {
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
//                "                {\n" +
//                "                    \"jsonpath\": \"field2.subfield2[0]\",\n" +
//                "                    \"columnName\": \"SUBFIELD1_FIRST\",\n" +
//                "                    \"type\": \"document\"\n" +
//                "                },\n" +
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
        assertEquals(Lists.newArrayList("val1", "val2", "val3"), iter.getEmittedRows().get(0));
        assertEquals(Lists.newArrayList("val1", "val2", "val4"), iter.getEmittedRows().get(1));
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
                "        \"val6\"\n" +
                "    ]\n" +
                "}";

        database.getCollection("test").insertOne(Document.parse(json));
    }

    public static SqlStatement getTestSqlNode() throws MetadataException {
        // SELECT USER_ID, count(URL) FROM CLICKS
        // WHERE 1 < USER_ID
        // GROUP BY USER_ID
        // HAVING 1 < COUNT(URL)
        // ORDER BY USER_ID
        // LIMIT 10;
        TableMetadata testTableMetadata = getTestTableMetadata();
        SqlTable fromClause = new SqlTable("TEST", testTableMetadata);
        SqlSelectList selectList = SqlSelectList.createSelectStarSelectList();
        SqlNode whereClause = null;
        SqlExpressionList groupBy = null;
        SqlNode countUrl = null;
        SqlNode having = null;
        SqlOrderBy orderBy = null;
        SqlLimit limit = null;
        return new SqlStatementSelect(fromClause, selectList, whereClause, groupBy, having, orderBy, limit);
    }

    public static TableMetadata getTestTableMetadata() throws MetadataException {
        List<ColumnMetadata> columns = new ArrayList<>();
        columns.add(new ColumnMetadata("FIELD1", "", DataType.createVarChar(10000, DataType.ExaCharset.UTF8), true, false, "", ""));
        columns.add(new ColumnMetadata("SUBFIELD1", "", DataType.createVarChar(10000, DataType.ExaCharset.UTF8), true, false, "", ""));
        columns.add(new ColumnMetadata("SUBFIELD1_FIRST", "", DataType.createVarChar(10000, DataType.ExaCharset.UTF8), true, false, "", ""));
        columns.add(new ColumnMetadata("SUBFIELD1_ALL_ATTR", "", DataType.createVarChar(10000, DataType.ExaCharset.UTF8), true, false, "", ""));
        TableMetadata tableMetadata = new TableMetadata("TEST", "", columns, "");
        return tableMetadata;
    }

    private class DummyExaIterator implements ExaIterator {

        private List<List<Object>> emittedRows = new ArrayList<>();

        public List<List<Object>> getEmittedRows() {
            return emittedRows;
        }

        @Override
        public void emit(Object... objects) throws ExaIterationException, ExaDataTypeException {
            emittedRows.add(Arrays.asList(objects.clone()));
        }

        @Override
        public long size() throws ExaIterationException {
            return 0;
        }

        @Override
        public boolean next() throws ExaIterationException {
            return false;
        }

        @Override
        public void reset() throws ExaIterationException {

        }

        @Override
        public Integer getInteger(int i) throws ExaIterationException, ExaDataTypeException {
            return null;
        }

        @Override
        public Integer getInteger(String s) throws ExaIterationException, ExaDataTypeException {
            return null;
        }

        @Override
        public Long getLong(int i) throws ExaIterationException, ExaDataTypeException {
            return null;
        }

        @Override
        public Long getLong(String s) throws ExaIterationException, ExaDataTypeException {
            return null;
        }

        @Override
        public BigDecimal getBigDecimal(int i) throws ExaIterationException, ExaDataTypeException {
            return null;
        }

        @Override
        public BigDecimal getBigDecimal(String s) throws ExaIterationException, ExaDataTypeException {
            return null;
        }

        @Override
        public Double getDouble(int i) throws ExaIterationException, ExaDataTypeException {
            return null;
        }

        @Override
        public Double getDouble(String s) throws ExaIterationException, ExaDataTypeException {
            return null;
        }

        @Override
        public String getString(int i) throws ExaIterationException, ExaDataTypeException {
            return null;
        }

        @Override
        public String getString(String s) throws ExaIterationException, ExaDataTypeException {
            return null;
        }

        @Override
        public Boolean getBoolean(int i) throws ExaIterationException, ExaDataTypeException {
            return null;
        }

        @Override
        public Boolean getBoolean(String s) throws ExaIterationException, ExaDataTypeException {
            return null;
        }

        @Override
        public Date getDate(int i) throws ExaIterationException, ExaDataTypeException {
            return null;
        }

        @Override
        public Date getDate(String s) throws ExaIterationException, ExaDataTypeException {
            return null;
        }

        @Override
        public Timestamp getTimestamp(int i) throws ExaIterationException, ExaDataTypeException {
            return null;
        }

        @Override
        public Timestamp getTimestamp(String s) throws ExaIterationException, ExaDataTypeException {
            return null;
        }

        @Override
        public Object getObject(int i) throws ExaIterationException, ExaDataTypeException {
            return null;
        }

        @Override
        public Object getObject(String s) throws ExaIterationException, ExaDataTypeException {
            return null;
        }
    }
}