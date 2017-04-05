package com.exasol.mongo;

import com.exasol.adapter.AdapterException;
import com.exasol.jsonpath.JsonPathElement;
import com.exasol.jsonpath.JsonPathParser;
import org.bson.types.ObjectId;

import java.util.Date;
import java.util.List;

public class MongoColumnMapping {

    private String jsonPath;
    private List<JsonPathElement> jsonPathParsed;
    private String columnName; // column name in EXASOL virtual table
    private MongoType type;

    public MongoColumnMapping(String jsonPath, String columnName, MongoType type) {
        this.jsonPath = jsonPath;
        this.jsonPathParsed = JsonPathParser.parseJsonPath(jsonPath);
        this.columnName = columnName;
        this.type = type;
    }

    public String getJsonPath() {
        return jsonPath;
    }

    public List<JsonPathElement> getJsonPathParsed() {
        return jsonPathParsed;
    }

    public String getColumnName() {
        return columnName;
    }

    public MongoType getType() {
        return type;
    }

    public enum MongoType {
        STRING(true, String.class),
        BOOLEAN(true, Boolean.class),
        INTEGER(true, Integer.class),
        LONG(true, Long.class),
        DOUBLE(true, Double.class),
        OBJECTID(true, ObjectId.class),
        DATE(true, Date.class),
        DOCUMENT(false, String.class),  // will be retrieved as json string
        ARRAY(false, String.class);     // will be retrieved as json string

        private boolean isPrimitive;
        private Class<?> clazz;

        MongoType(boolean isPrimitive, Class<?> clazz) {
            this.isPrimitive = isPrimitive;
            this.clazz = clazz;
        }

        public static MongoType fromString(String mongoType) throws AdapterException {
            try {
                return valueOf(MongoType.class, mongoType.toUpperCase());
            } catch (IllegalArgumentException e) {
                throw new AdapterException("Your mapping contains an unsupported type: " + mongoType);
            }
        }

        public boolean isPrimitive() {
            return isPrimitive;
        }

        public Class<?> getClazz() {
            return clazz;
        }
    }

}
