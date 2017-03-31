package com.exasol.mongo;

public class MongoColumnMapping {

    public enum MongoType {
        STRING,
        BOOLEAN,
        INTEGER,
        LONG,
        DOUBLE,
        OBJECTID,
        DATE
    }

    public MongoColumnMapping(String mongoJsonPath, String columnName, MongoType type) {
        this.mongoJsonPath = mongoJsonPath;
        this.columnName = columnName;
        this.type = type;
    }

    private String mongoJsonPath;
    private String columnName; // column name in EXASOL virtual table
    private MongoType type;

    public String getMongoJsonPath() {
        return mongoJsonPath;
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
