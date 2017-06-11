package com.exasol.jsonpath;


public class JsonPathListWildcardOperatorElement implements JsonPathElement {

	public JsonPathListWildcardOperatorElement() {

	}

	@Override
	public String toJsonPathString() {
		return "[*]";
	}

	@Override
	public Type getType() {
		return Type.LIST_WILDCARD;
	}
	
	@Override
	public boolean equals(Object obj) {
        if (!(obj instanceof JsonPathListWildcardOperatorElement)) {
    	    return false;
        }
        return true;
	}
	
	@Override
	public String toString() {
	    return this.getClass().getName() + "[*]";
	}

}
