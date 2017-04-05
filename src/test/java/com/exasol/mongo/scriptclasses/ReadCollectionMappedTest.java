package com.exasol.mongo.scriptclasses;

import com.exasol.ExaDataTypeException;
import com.exasol.ExaIterationException;
import com.exasol.ExaIterator;
import com.exasol.adapter.AdapterException;
import com.exasol.mongo.MongoColumnMapping;
import com.exasol.mongo.MongoMappingParser;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import org.junit.Test;

import java.math.BigDecimal;
import java.sql.Date;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.*;


public class ReadCollectionMappedTest {

    @Test
    public void testReadMapped() throws Exception {
        String mappingSpec = "[\n" +
                "        {\n" +
                "          \"jsonpath\": \"shortid\",\n" +
                "          \"columnName\": \"TESTSET_ID\",\n" +
                "          \"type\": \"string\"\n" +
                "        },\n" +
                "        {\n" +
                "          \"jsonpath\": \"uuid\",\n" +
                "          \"columnName\": \"TESTSET_UUID\",\n" +
                "          \"type\": \"string\"\n" +
                "        },\n" +
                "        {\n" +
                "          \"jsonpath\": \"nightly_owner\",\n" +
                "          \"columnName\": \"NIGHTLY_OWNER\",\n" +
                "          \"type\": \"string\"\n" +
                "        },\n" +
                "        {\n" +
                "          \"jsonpath\": \"nightly_wip\",\n" +
                "          \"columnName\": \"NIGHTLY_WIP\",\n" +
                "          \"type\": \"boolean\"\n" +
                "        },\n" +
                "        {\n" +
                "          \"jsonpath\": \"testobject_name\",\n" +
                "          \"columnName\": \"TESTOBJECT_NAME\",\n" +
                "          \"type\": \"string\"\n" +
                "        },\n" +
                "        {\n" +
                "          \"jsonpath\": \"time_created\",\n" +
                "          \"columnName\": \"TIME_CREATED\",\n" +
                "          \"type\": \"double\"\n" +
                "        },\n" +
                "        {\n" +
                "          \"jsonpath\": \"time_updated\",\n" +
                "          \"columnName\": \"TIME_UPDATED\",\n" +
                "          \"type\": \"double\"\n" +
                "        },\n" +
                "        {\n" +
                "          \"jsonpath\": \"$.user\",\n" +
                "          \"columnName\": \"SUBMIT_USER\",\n" +
                "          \"type\": \"string\"\n" +
                "        },\n" +
                "        {\n" +
                "          \"jsonpath\": \"buildconfig.Alias\",\n" +
                "          \"columnName\": \"BUILDCONFIG_ALIAS\",\n" +
                "          \"type\": \"string\"\n" +
                "        },\n" +
                "        {\n" +
                "          \"jsonpath\": \"buildconfig\",\n" +
                "          \"columnName\": \"BUILDCONFIG_JSON\",\n" +
                "          \"type\": \"document\"\n" +
                "        },\n" +
                "        {\n" +
                "          \"jsonpath\": \"jobs\",\n" +
                "          \"columnName\": \"JOBS\",\n" +
                "          \"type\": \"array\"\n" +
                "        }\n" +
                "    ]";
        List<MongoColumnMapping> mapping = MongoMappingParser.parseColumnMappings(mappingSpec);
        DummyExaIterator iter = new DummyExaIterator();
        ReadCollectionMapped.readMapped(iter, "localhost", 27017, "test", "testsets", mapping, 1000);

        System.out.println("Emmited rows: ");
        for (List<Object> row : iter.getEmittedRows()) {
            System.out.println("- " + row);
        }
    }

    private class DummyExaIterator implements ExaIterator {

        List<List<Object>> emittedRows = new ArrayList<>();

        public List<List<Object>> getEmittedRows() {
            return emittedRows;
        }

        @Override
        public void emit(Object... objects) throws ExaIterationException, ExaDataTypeException {
            emittedRows.add(Arrays.asList(objects));
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