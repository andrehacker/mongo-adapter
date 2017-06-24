package com.exasol.mongo.mapping;

import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static com.exasol.mongo.mapping.MongoColumnMapping.MongoType.STRING;
import static org.junit.Assert.assertEquals;

public class MongoDBMappingSerializerTest {
    @Test
    public void serializeMongoMapping() throws Exception {
        List<MongoCollectionMapping> collectionMapping = new ArrayList<>();
        List<MongoColumnMapping> columnMappings = new ArrayList<>();
        columnMappings.add(new MongoColumnMapping("author", "AUTHOR_CAPITAL", STRING));
        columnMappings.add(new MongoColumnMapping("jobid.sub", "JOBID_SUB", STRING));
        collectionMapping.add(new MongoCollectionMapping("comments", "COMMENTS", columnMappings));
        MongoDBMapping dbMapping = new MongoDBMapping(collectionMapping);
        String actual = MongoDBMappingSerializer.serialize(dbMapping);
        assertEquals("{\"tables\":[{\"collectionName\":\"comments\",\"tableName\":\"COMMENTS\",\"columns\":[{\"jsonpath\":\"author\",\"columnName\":\"AUTHOR_CAPITAL\",\"mongoType\":\"STRING\"},{\"jsonpath\":\"jobid.sub\",\"columnName\":\"JOBID_SUB\",\"mongoType\":\"STRING\"}]}]}", actual);
    }
}
