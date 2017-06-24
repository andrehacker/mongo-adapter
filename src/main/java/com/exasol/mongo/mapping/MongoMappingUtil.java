package com.exasol.mongo.mapping;

import com.exasol.jsonpath.JsonPathElement;
import com.exasol.jsonpath.JsonPathFieldElement;
import com.exasol.jsonpath.JsonPathListIndexElement;

import java.util.List;

public class MongoMappingUtil {

    // EXASOL does not allow column names like "field1.subfield1", so we need any other separator
    private static final String SEPARATOR_STRING = "--";

    public static String getAutoMappedColumnNameForJsonPath(List<JsonPathElement> jsonPath) {
        String pathString = "";
        for (JsonPathElement element : jsonPath) {
            switch (element.getType()) {
                case FIELD:
                    if (!pathString.isEmpty()) {
                        pathString += SEPARATOR_STRING;
                    }
                    pathString += ((JsonPathFieldElement) element).getFieldName();
                    break;
                case LIST_INDEX:
                    if (!pathString.isEmpty()) {
                        pathString += SEPARATOR_STRING;
                    }
                    pathString += "[" + ((JsonPathListIndexElement) element).getListIndex() + "]";
                    break;
                case LIST_WILDCARD:
                    pathString += "[*]";
                    break;
            }
        }
        return pathString;
    }
}
