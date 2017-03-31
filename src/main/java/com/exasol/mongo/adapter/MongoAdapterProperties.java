package com.exasol.mongo.adapter;


import com.exasol.adapter.AdapterException;

import java.util.Map;

public class MongoAdapterProperties {

    static final String PROP_MONGO_HOST = "MONGO_HOST";
    static final String PROP_MONGO_PORT = "MONGO_PORT";
    static final String PROP_MONGO_DB = "MONGO_DB";
    static final String PROP_MODE = "MODE";
    static final String PROP_IGNORE_COLLECTION_CASE = "IGNORE_COLLECTION_CASE";
    static final String PROP_MAX_RESULT_ROWS = "MAX_RESULT_ROWS";
    static final String PROP_MAPPING = "MAPPING";

    private Map<String, String> properties;

    public MongoAdapterProperties(Map<String, String> properties) {
        this.properties = properties;
    }

    private String getProperty(String name, String defaultValue) {
        if (properties.containsKey(name)) {
            return properties.get(name);
        } else {
            return defaultValue;
        }
    }

    public String getMongoHost() {
        return getProperty(PROP_MONGO_HOST, "");
    }

    public int getMongoPort() {
        String port = getProperty(PROP_MONGO_PORT, "");
        return Integer.parseInt(port);
    }

    public int getMaxResultRows() {
        String maxResultRows = getProperty(PROP_MAX_RESULT_ROWS, "");
        return Integer.parseInt(maxResultRows);
    }

    public static enum MongoMappingMode {
        JSON,   // Single column with whole document as json
        MAPPED
    }

    public MongoMappingMode getMappingMode() throws AdapterException {
        String mode = getProperty(PROP_MODE, "");
        if (mode.equalsIgnoreCase("json")) {
            return MongoMappingMode.JSON;
        } else if (mode.equalsIgnoreCase("mapped")) {
            return MongoMappingMode.MAPPED;
        } else {
            throw new AdapterException("Unsupported Mode: " + mode);
        }
    }

    public String getMongoDB() {
        return getProperty(PROP_MONGO_DB, "");
    }

    public boolean getIgnoreCollectionCase() {
        return getProperty(PROP_IGNORE_COLLECTION_CASE, "false").equalsIgnoreCase("true");
    }

    public String getMapping() {
        return getProperty(PROP_MAPPING, "");
    }

}
