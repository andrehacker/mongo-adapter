package com.exasol.mongo.adapter;


import com.exasol.adapter.AdapterException;
import com.exasol.mongo.MongoDBMapping;
import com.exasol.mongo.MongoMappingParser;

import java.util.Map;

/**
 * TODO Check valid input: e.g. mapping is a valid json
 */
public class MongoAdapterProperties {

    public static final String PROP_MONGO_HOST = "MONGO_HOST";
    public static final String PROP_MONGO_PORT = "MONGO_PORT";
    public static final String PROP_MONGO_DB = "MONGO_DB";
    public static final String PROP_MODE = "MODE";
    public static final String PROP_MAPPING = "MAPPING";
    public static final String PROP_IGNORE_COLLECTION_CASE = "IGNORE_COLLECTION_CASE";
    public static final String PROP_MAX_RESULT_ROWS = "MAX_RESULT_ROWS";
    public static final String PROP_SCHEMA_ENFORCEMENT = "SCHEMA_ENFORCEMENT";

    private Map<String, String> properties;

    public MongoAdapterProperties(Map<String, String> properties) throws AdapterException {
        this.properties = properties;
        checkPropertyConsistency();
    }

    public static boolean isRefreshNeeded(Map<String, String> newProperties) {
        return newProperties.containsKey(PROP_MODE)
                || newProperties.containsKey(PROP_MONGO_HOST)
                || newProperties.containsKey(PROP_MONGO_PORT)
                || newProperties.containsKey(PROP_MONGO_DB)
                || newProperties.containsKey(PROP_MAPPING)
                || newProperties.containsKey(PROP_IGNORE_COLLECTION_CASE);
    }

    private void checkPropertyConsistency() throws AdapterException {
        getMongoHost();
        getMongoPort();
        getMongoDB();
        if (getMappingMode() == MongoMappingMode.MAPPED) {
            getMapping();
        }
        getSchemaEnforcementLevel();
    }

    private String getProperty(String name, String defaultValue) {
        if (properties.containsKey(name)) {
            return properties.get(name);
        } else {
            return defaultValue;
        }
    }

    public String getMongoHost() throws InvalidPropertyException {
        String host = getProperty(PROP_MONGO_HOST, "");
        if (host.trim().isEmpty()) {
            throw new InvalidPropertyException("You have to specify the MongoDB host using the property " + PROP_MONGO_HOST);
        }
        return host;
    }

    public int getMongoPort() throws InvalidPropertyException {
        String port = getProperty(PROP_MONGO_PORT, "");
        try {
            return Integer.parseInt(port);
        } catch (NumberFormatException ex) {
            throw new InvalidPropertyException("You have to specify the property " + PROP_MONGO_PORT + " and it has to be a valid number.");
        }
    }

    public String getMongoDB() throws InvalidPropertyException {
        String db = getProperty(PROP_MONGO_DB, "");
        if (db.trim().isEmpty()) {
            throw new InvalidPropertyException("You have to specify the MongoDB database using the property " + PROP_MONGO_DB);
        }
        return db;
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

    public MongoMappingMode getMappingMode() throws InvalidPropertyException {
        String mode = getProperty(PROP_MODE, "");
        if (mode.equalsIgnoreCase("json")) {
            return MongoMappingMode.JSON;
        } else if (mode.equalsIgnoreCase("mapped")) {
            return MongoMappingMode.MAPPED;
        } else {
            throw new InvalidPropertyException("You have to specify the " + PROP_MAPPING + " with the value either 'JSON' or 'MAPPED'");
        }
    }

    public enum SchemaEnforcementLevel {

        /**
         * Ignore if fields do not exist or have a different type than specified in the mapping (i.e. emit null)
         */
        NONE;

        /**
         * If the field exists, the type must match the type specified in the mapping
         */
        // CHECK_TYPE;

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

    public boolean getIgnoreCollectionCase() {
        return getProperty(PROP_IGNORE_COLLECTION_CASE, "false").equalsIgnoreCase("true");
    }

    public MongoDBMapping getMapping() throws AdapterException {
        String mapping = getProperty(PROP_MAPPING, "").trim();
        if (mapping.isEmpty()) {
            throw new InvalidPropertyException("You have to specify a mapping via the property " + PROP_MAPPING + " because you are using the mapping mode");
        }
        assert(getMappingMode() == MongoMappingMode.MAPPED);
        return MongoMappingParser.parse(mapping);
    }

}
