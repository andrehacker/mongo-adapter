package com.exasol.mongo.deriveschema;

import com.exasol.jsonpath.JsonPathElement;
import com.exasol.jsonpath.JsonPathFieldElement;
import org.bson.Document;

import static com.exasol.mongo.mapping.MongoColumnMapping.MongoType.DOCUMENT;

public class DeriveSchemaDocumentNode extends DeriveSchemaNode {

    DeriveSchemaDocumentNode(JsonPathElement jsonPathElement, Integer firstSeenIndex) {
        super(jsonPathElement, DOCUMENT, firstSeenIndex);
    }

    @Override
    void updateDerivedSchemaRecursive(Object element) {
        int i = 0;
        for (String key : ((Document) element).keySet()) {
            createOrUpdateChildAndRecurse(new JsonPathFieldElement(key), ((Document) element).get(key), i);
            i++;
        }
    }
}
