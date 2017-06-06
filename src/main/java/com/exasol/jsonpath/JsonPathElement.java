package com.exasol.jsonpath;

public interface JsonPathElement {

    enum Type {
        FIELD,
        LIST_INDEX,
        LIST_STAR
    }

    String toJsonPathString();

    Type getType();
}
