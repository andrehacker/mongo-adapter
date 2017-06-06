package com.exasol.jsonpath;

import org.junit.Test;

import java.util.List;

import static org.junit.Assert.*;

public class JsonPathParserTest {

    @Test
    public void parseJsonPath() throws Exception {
        List<JsonPathElement> path = JsonPathParser.parseJsonPath("a.b[*].c");
        assertEquals(JsonPathElement.Type.FIELD, path.get(0).getType());
        assertEquals(JsonPathElement.Type.FIELD, path.get(1).getType());
        assertEquals(JsonPathElement.Type.LIST_STAR, path.get(2).getType());
        assertEquals(JsonPathElement.Type.FIELD, path.get(3).getType());
    }

}