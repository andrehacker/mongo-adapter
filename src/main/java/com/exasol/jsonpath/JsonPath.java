package com.exasol.jsonpath;


import java.util.List;

/**
 * Added late, will replace List<JsonPathElement> sometime
 */
public class JsonPath {

    public static String getJsonPathString(List<JsonPathElement> path) {
        String pathString = "";
        for (JsonPathElement element : path) {
            switch (element.getType()) {
                case FIELD:
                    if (!pathString.isEmpty()) {
                        pathString += ".";
                    }
                    pathString += ((JsonPathFieldElement) element).getFieldName();
                    break;
                case LIST_INDEX:
                    if (!pathString.isEmpty()) {
                        pathString += ".";
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
