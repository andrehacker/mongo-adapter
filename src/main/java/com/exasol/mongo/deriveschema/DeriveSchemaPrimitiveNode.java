package com.exasol.mongo.deriveschema;

import com.exasol.jsonpath.JsonPathElement;
import com.exasol.mongo.mapping.MongoColumnMapping;

public class DeriveSchemaPrimitiveNode extends DeriveSchemaNode {

    DeriveSchemaPrimitiveNode(JsonPathElement jsonPathElement, MongoColumnMapping.MongoType mongoType, Integer firstSeenIndex) {
        super(jsonPathElement, mongoType, firstSeenIndex);
        if (!mongoType.isPrimitive()) {
            throw new RuntimeException("Internal error: Tried creating primitive node for non primitive type: " + mongoType.toString());
        }
    }

    @Override
    void updateDerivedSchemaRecursive(Object element) {
        return;
    }
}
