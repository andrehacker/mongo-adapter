package com.exasol.mongo.adapter;


import com.exasol.adapter.AdapterException;
import com.exasol.mongo.MongoDBMapping;
import com.exasol.mongo.MongoMappingParser;

import java.util.Map;

/**
 * TODO Check valid input: e.g. mapping is a valid json
 */
public class MongoAdapterProperties {

    static final String PROP_MONGO_HOST = "MONGO_HOST";
    static final String PROP_MONGO_PORT = "MONGO_PORT";
    static final String PROP_MONGO_DB = "MONGO_DB";
    static final String PROP_MODE = "MODE";
    static final String PROP_IGNORE_COLLECTION_CASE = "IGNORE_COLLECTION_CASE";
    static final String PROP_MAX_RESULT_ROWS = "MAX_RESULT_ROWS";
    static final String PROP_MAPPING = "MAPPING";
    static final String PROP_SCHEMA_ENFORCEMENT = "SCHEMA_ENFORCEMENT";

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

    public static final int UNLIMITED_RESULT_ROWS = -1;

    public int getMaxResultRows() {
        String maxResultRows = getProperty(PROP_MAX_RESULT_ROWS, Integer.toString(UNLIMITED_RESULT_ROWS));
        return Integer.parseInt(maxResultRows);
    }

    public enum MongoMappingMode {
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

    public enum SchemaEnforcementLevel {

        /**
         * Ignore if fields do not exist or have a different type than specified in the mapping (i.e. emit null)
         */
        NONE,

        /**
         * If the field exists, the type must match the type specified in the mapping
         */
        CHECK_TYPE;

        /**
         * The fields (structure) defined in the mapping must exist for every document, and must have the specified type
         */
        // CHECK_TYPE_AND_STRUCTURE;


        public static SchemaEnforcementLevel fromString(String schemaEnforcementLevel) throws AdapterException {
            try {
                return valueOf(SchemaEnforcementLevel.class, schemaEnforcementLevel.toUpperCase());
            } catch (IllegalArgumentException e) {
                throw new AdapterException("Unsupported Schema Enforcement Level defined: " + schemaEnforcementLevel);
            }
        }
    }

    public SchemaEnforcementLevel getSchemaEnforcementLevel() throws AdapterException {
        String level = getProperty(PROP_SCHEMA_ENFORCEMENT, SchemaEnforcementLevel.NONE.name());
        return SchemaEnforcementLevel.fromString(level);
    }

    public String getMongoDB() {
        return getProperty(PROP_MONGO_DB, "");
    }

    public boolean getIgnoreCollectionCase() {
        return getProperty(PROP_IGNORE_COLLECTION_CASE, "false").equalsIgnoreCase("true");
    }

    public MongoDBMapping getMapping() throws Exception {
        assert(getMappingMode() == MongoMappingMode.MAPPED);
        return MongoMappingParser.parse(getProperty(PROP_MAPPING, ""));
    }

}
