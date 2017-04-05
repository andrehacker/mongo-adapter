package com.exasol.mongo;

import org.junit.Test;

import static org.junit.Assert.assertEquals;


public class MongoMappingParserTest {

    @Test
    public void testParseMongoMapping() throws Exception {
        String test = "{\n" +
                "\"tables\": [\n" +
                "  {\n" +
                "    \"collectionName\": \"comments\",\n" +
                "    \"columns\": [\n" +
                "        {\n" +
                "          \"jsonpath\": \"author\",\n" +
                "          \"columnName\": \"author_renamed\",\n" +
                "          \"type\": \"string\"\n" +
                "        },\n" +
                "        {\n" +
                "          \"jsonpath\": \"jobid\",\n" +
                "          \"type\": \"string\"\n" +
                "        }\n" +
                "    ]\n" +
                "  }\n" +
                "]\n" +
                "}";
        MongoDBMapping mapping = MongoMappingParser.parse(test);
        assertEquals(1,mapping.getCollectionMappings().size());
        assertEquals("{\"collectionName\":\"comments\",\"columns\":[{\"jsonpath\":\"author\",\"columnName\":\"author_renamed\",\"type\":\"string\"},{\"jsonpath\":\"jobid\",\"type\":\"string\"}]}",mapping.getCollectionMappings().get(0).getJsonMappingSpec());
        assertEquals("comments",mapping.getCollectionMappings().get(0).getCollectionName());
        assertEquals("comments",mapping.getCollectionMappings().get(0).getTableName());
        assertEquals(2 ,mapping.getCollectionMappings().get(0).getColumnMappings().size());
        assertEquals("author" ,mapping.getCollectionMappings().get(0).getColumnMappings().get(0).getJsonPath());
        assertEquals("author_renamed" ,mapping.getCollectionMappings().get(0).getColumnMappings().get(0).getColumnName());
        assertEquals(MongoColumnMapping.MongoType.STRING ,mapping.getCollectionMappings().get(0).getColumnMappings().get(0).getType());
    }

}