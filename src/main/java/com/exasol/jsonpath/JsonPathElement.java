package com.exasol.jsonpath;

public interface JsonPathElement {

    enum Type {
        FIELD,
        LIST_INDEX,
        LIST_WILDCARD
    }

    String toJsonPathString();

    Type getType();
}
