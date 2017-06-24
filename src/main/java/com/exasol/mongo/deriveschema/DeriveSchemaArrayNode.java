package com.exasol.mongo.deriveschema;

import com.exasol.jsonpath.JsonPathElement;
import com.exasol.jsonpath.JsonPathListWildcardOperatorElement;

import java.util.List;

import static com.exasol.mongo.mapping.MongoColumnMapping.MongoType.ARRAY;

public class DeriveSchemaArrayNode extends DeriveSchemaNode {

    DeriveSchemaArrayNode(JsonPathElement jsonPathElement, Integer firstSeenIndex) {
        super(jsonPathElement, ARRAY, firstSeenIndex);
    }

    @Override
    void updateDerivedSchemaRecursive(Object element) {
        int i = 0;
        for (Object val : (List) element) {
            createOrUpdateChildAndRecurse(new JsonPathListWildcardOperatorElement(), val, i);
            i++;
        }
    }
}
