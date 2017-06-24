package com.exasol.mongo.deriveschema;

import com.exasol.adapter.AdapterException;
import com.exasol.jsonpath.JsonPathElement;
import com.exasol.jsonpath.JsonPathFieldElement;
import com.exasol.mongo.mapping.MongoColumnMapping;
import org.bson.Document;

import java.util.ArrayList;
import java.util.List;

public class DeriveSchemaRootNode extends DeriveSchemaDocumentNode {

    public DeriveSchemaRootNode() {
        super(new JsonPathFieldElement("ROOT_DUMMY_IGNORE"), 0);
    }

    public List<MongoColumnMapping> mergeCompatibleAndGetBestCollectionMapping() throws AdapterException {
        mergeChildrenRecursive();
        List<JsonPathElement> emptyJsonPath = new ArrayList<>();
        List<MongoColumnMapping> allColumnMappings = new ArrayList<>();
        for (DeriveSchemaNode child : getChildren()) {
            child.addColumnMappingRecursive(emptyJsonPath, allColumnMappings);
        }
        allColumnMappings.sort((mapping1, mapping2) -> {
            if (mapping1.getColumnName().equals("_id")) {
                return -1;
            } else if (mapping2.getColumnName().equals("_id")) {
                return 1;
            } else {
                return mapping1.getColumnName().compareTo(mapping2.getColumnName());
            }
        });
        return allColumnMappings;
    }

    public void updateDerivedSchema(Document doc) {
        updateDerivedSchemaRecursive(doc);
    }
}
