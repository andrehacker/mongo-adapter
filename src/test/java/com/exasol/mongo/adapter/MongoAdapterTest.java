package com.exasol.mongo.adapter;

import com.exasol.adapter.metadata.SchemaMetadata;
import com.exasol.adapter.metadata.SchemaMetadataInfo;
import org.junit.Ignore;
import org.junit.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;


public class MongoAdapterTest
{

    @Ignore
    @Test
    public void testApp() throws Exception {
        Map<String, String> properties = Collections.unmodifiableMap(new HashMap<String, String>() {
            {
                put("MONGO_HOST", "localhost");
                put("MONGO_PORT", "27017");
                put("MONGO_DB", "tests_database");
            }
        });
        SchemaMetadataInfo info = new SchemaMetadataInfo("mongo-test", "", properties);
        //SchemaMetadata metadata = MongoAdapter.readMetadata(info, null);
        //System.out.println("Schema Metadata: " + metadata.getTables());
    }
}
