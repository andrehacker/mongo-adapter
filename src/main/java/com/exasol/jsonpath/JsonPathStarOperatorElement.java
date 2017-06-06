package com.exasol.jsonpath;


public class JsonPathStarOperatorElement implements JsonPathElement {

	public JsonPathStarOperatorElement() {

	}

	@Override
	public String toJsonPathString() {
		return "[*]";
	}

	@Override
	public Type getType() {
		return Type.LIST_STAR;
	}
	
	@Override
	public boolean equals(Object obj) {
        if (!(obj instanceof JsonPathStarOperatorElement)) {
    	    return false;
        }
        return true;
	}
	
	@Override
	public String toString() {
	    return this.getClass().getName() + "[*]";
	}

}
