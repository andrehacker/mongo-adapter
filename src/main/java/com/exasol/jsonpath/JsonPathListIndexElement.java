package com.exasol.jsonpath;


public class JsonPathListIndexElement implements JsonPathElement {
	
	private int listIndex;
	
	public JsonPathListIndexElement(int listIndex) {
		this.listIndex = listIndex;
	}
	
	public int getListIndex() {
		return listIndex;
	}

	@Override
	public String toJsonPathString() {
		return "[" + listIndex + "]";
	}

	@Override
	public Type getType() {
		return Type.LIST_INDEX;
	}
	
	@Override
	public boolean equals(Object obj) {
        if (!(obj instanceof JsonPathListIndexElement)) {
    	    return false;
        }
        if (obj == this) {
    	    return true;
        }
		return ((JsonPathListIndexElement)obj).getListIndex() == listIndex;
	}
	
	@Override
	public int hashCode() {
		return new Long(listIndex).hashCode();
	}
	
	@Override
	public String toString() {
	    return this.getClass().getName() + "[" + listIndex + "]";
	}

}
