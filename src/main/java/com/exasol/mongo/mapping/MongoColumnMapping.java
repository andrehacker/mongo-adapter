package com.exasol.mongo.mapping;

import com.exasol.adapter.AdapterException;
import com.exasol.jsonpath.JsonPathElement;
import com.exasol.jsonpath.JsonPathParser;
import org.bson.Document;
import org.bson.types.ObjectId;

import java.util.Date;
import java.util.List;
import java.util.stream.Collectors;

public class MongoColumnMapping {

    private String jsonPath;
    private List<JsonPathElement> jsonPathParsed;
    private String columnName; // column name in EXASOL virtual table
    private MongoType type;

    public MongoColumnMapping(String jsonPath, String columnName, MongoType type) throws AdapterException {
        this.jsonPath = jsonPath;
        this.jsonPathParsed = JsonPathParser.parseJsonPath(jsonPath);
        this.columnName = columnName;
        this.type = type;
        checkForValidJsonPath();
    }

    private void checkForValidJsonPath() throws AdapterException {
        if (jsonPathParsed.stream().filter(jsonPathElement -> jsonPathElement.getType() == JsonPathElement.Type.LIST_WILDCARD).collect(Collectors.toList()).size() > 1) {
            throw new AdapterException("Invalid JSONPath defined: Not allowed to specify more than one list wildcard: " + jsonPath);
        }
        if (jsonPathParsed.size() == 0 && type != MongoType.DOCUMENT) {
            throw new AdapterException("The root field '$' is of type DOCUMENT, but was specified as type " + type.name() + " in the mapping.");
        }
    }

    public String getJsonPath() {
        return jsonPath;
    }

    public List<JsonPathElement> getJsonPathParsed() {
        return jsonPathParsed;
    }

    public boolean jsonPathHasListWildcard() {
        return getJsonPathParsed().stream().anyMatch(jsonPathElement -> jsonPathElement.getType() == JsonPathElement.Type.LIST_WILDCARD);
    }

    public String getColumnName() {
        return columnName;
    }

    public MongoType getType() {
        return type;
    }

    public enum MongoType {
        STRING(true, String.class, String.class),
        BOOLEAN(true, Boolean.class, Boolean.class),
        INTEGER(true, Integer.class, Integer.class),
        LONG(true, Long.class, Long.class),
        DOUBLE(true, Double.class, Double.class),
        DATE(true, Date.class, Date.class),
        OBJECTID(true, String.class, ObjectId.class),
        DOCUMENT(false, String.class, Document.class),  // will be emitted as json string
        ARRAY(false, String.class, List.class);     // will be emitted as json string

        private boolean isPrimitive;
        private Class<?> classToEmit;
        private Class<?> classFromMongo;

        MongoType(boolean isPrimitive, Class<?> classToEmit, Class<?> classFromMongo) {
            this.isPrimitive = isPrimitive;
            this.classToEmit = classToEmit;
            this.classFromMongo = classFromMongo;
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

        public Class<?> getClassToEmit() {
            return classToEmit;
        }

        public Class<?> getClassFromMongo() {
            return classFromMongo;
        }
    }

}
