package com.exasol.mongo;

import com.exasol.jsonpath.JsonPathElement;
import com.exasol.jsonpath.JsonPathParser;
import org.bson.types.ObjectId;

import java.util.Date;
import java.util.List;

public class MongoColumnMapping {

    public enum MongoType {
        STRING(String.class),
        BOOLEAN(Boolean.class),
        INTEGER(Integer.class),
        LONG(Long.class),
        DOUBLE(Double.class),
        OBJECTID(ObjectId.class),
        DATE(Date.class);

        private Class<?> clazz;

        MongoType(Class<?> clazz) {
            this.clazz = clazz;
        }

        public Class<?> getClazz() {
            return clazz;
        }
    }

    public MongoColumnMapping(String mongoJsonPath, String columnName, MongoType type) {
        this.mongoJsonPath = mongoJsonPath;
        this.mongoJsonPathParsed = JsonPathParser.parseJsonPath(mongoJsonPath);
        this.columnName = columnName;
        this.type = type;
    }

    private String mongoJsonPath;
    private List<JsonPathElement> mongoJsonPathParsed;
    private String columnName; // column name in EXASOL virtual table
    private MongoType type;

    public String getMongoJsonPath() {
        return mongoJsonPath;
    }

    public List<JsonPathElement> getMongoJsonPathParsed() {
        return mongoJsonPathParsed;
    }

    public String getColumnName() {
        return columnName;
    }

    public MongoType getType() {
        return type;
    }

    public static MongoType mongoTypeFromString(String mongoType) {
        return Enum.valueOf(MongoColumnMapping.MongoType.class, mongoType.toUpperCase());
    }

}
