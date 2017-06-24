package com.exasol.mongo.deriveschema;


import com.exasol.adapter.AdapterException;
import com.exasol.jsonpath.JsonPath;
import com.exasol.jsonpath.JsonPathElement;
import com.exasol.mongo.mapping.MongoColumnMapping;
import com.exasol.mongo.mapping.MongoColumnMapping.MongoType;
import com.exasol.mongo.mapping.MongoMappingUtil;
import org.bson.Document;
import org.bson.types.ObjectId;

import java.util.*;

import static com.exasol.mongo.mapping.MongoColumnMapping.MongoType.*;
import static java.util.Comparator.comparing;

public abstract class DeriveSchemaNode {

    private JsonPathElement jsonPathElement;
    private MongoType mongoType;
    private int count = 1;
    private Integer firstSeenIndex;
    private List<DeriveSchemaNode> children = new ArrayList<>();

    DeriveSchemaNode(JsonPathElement jsonPathElement, MongoType mongoType, Integer firstSeenIndex) {
        this.jsonPathElement = jsonPathElement;
        this.mongoType = mongoType;
        this.firstSeenIndex = firstSeenIndex;
    }

    void createOrUpdateChildAndRecurse(JsonPathElement jsonPathElement, Object o, int indexOfObject) {
        if (o == null) {
            // mongo supports null values, ignore these in schema
            return;
        }
        MongoType mongoType = getMongoType(o);
        Optional<DeriveSchemaNode> optional = children.stream()
                .filter(child -> child.getMongoType().equals(mongoType) && child.getJsonPathElement().equals(jsonPathElement)).findAny();
        DeriveSchemaNode child = (optional.isPresent()) ? optional.get() : createDerivedSchemaNode(jsonPathElement, mongoType, indexOfObject);
        if (optional.isPresent()) {
            child.incrementCounter();
        } else {
            children.add(child);
        }
        child.updateDerivedSchemaRecursive(o);
    }

    private DeriveSchemaNode createDerivedSchemaNode(JsonPathElement jsonPathElement, MongoType mongoType, int indexOfObject) {
        switch (mongoType) {
            case DOCUMENT:
                return new DeriveSchemaDocumentNode(jsonPathElement, indexOfObject);
            case ARRAY:
                return new DeriveSchemaArrayNode(jsonPathElement, indexOfObject);
            case STRING:
            case INTEGER:
            case LONG:
            case DOUBLE:
            case BOOLEAN:
            case DATE:
            case OBJECTID:
                return new DeriveSchemaPrimitiveNode(jsonPathElement, mongoType, indexOfObject);
            default:
                throw new RuntimeException("Internal error: Unsupported mongo type: " + mongoType);
        }
    }

    void addColumnMappingRecursive(List<JsonPathElement> jsonPathPrefix, List<MongoColumnMapping> allColumnMappings) throws AdapterException {
        List<JsonPathElement> curJsonPath = new ArrayList<>(jsonPathPrefix);
        curJsonPath.add(getJsonPathElement());
        if (getMongoType().isPrimitive()) {
            assert(children.isEmpty());
            allColumnMappings.add(new MongoColumnMapping(JsonPath.getJsonPathString(curJsonPath), MongoMappingUtil.getAutoMappedColumnNameForJsonPath(curJsonPath), getMongoType()));
        } else {
            assert(!children.isEmpty());
            for (DeriveSchemaNode child : children) {
                child.addColumnMappingRecursive(curJsonPath, allColumnMappings);
            }
        }
    }

    void mergeChildrenRecursive() {
        if (children.isEmpty()) {
            return;
        }
        Map<String, DeriveSchemaNode> mergedChildrenByName = new HashMap<>();
        mergedChildrenByName.put(children.get(0).getJsonPathElement().toJsonPathString(), children.get(0));
        for (int i=1; i<children.size(); i++) {
            DeriveSchemaNode curChild = children.get(i);
            String jsonPathElementName = curChild.getJsonPathElement().toJsonPathString();
            if (mergedChildrenByName.containsKey(jsonPathElementName)) {
                DeriveSchemaNode existingChild = mergedChildrenByName.get(jsonPathElementName);
                MongoType mergedType = getCompatiblePrimitiveMongoTypeOrNull(existingChild.getMongoType(), curChild.getMongoType());
                if (mergedType != null) {
                    DeriveSchemaNode mergedNode = new DeriveSchemaPrimitiveNode(existingChild.getJsonPathElement(), mergedType, existingChild.getFirstSeenIndex());
                    mergedChildrenByName.put(jsonPathElementName, mergedNode);
                } else {
                    if (curChild.getCount() > existingChild.getCount()) {
                        mergedChildrenByName.put(jsonPathElementName, curChild);
                    } // else: existing child occurs more often
                }
            } else {
                mergedChildrenByName.put(jsonPathElementName, curChild);
            }
        }
        children = new ArrayList<>(mergedChildrenByName.values());
        children.forEach(DeriveSchemaNode::mergeChildrenRecursive);
    }

    private MongoType getCompatiblePrimitiveMongoTypeOrNull(MongoType type1, MongoType type2) {
        assert(!type1.equals(type2));
        if ((type1.equals(INTEGER) || type1.equals(LONG)) &&
                (type2.equals(INTEGER) || type2.equals(LONG))) {
            return LONG;
        } else if ((type1.equals(INTEGER) || type1.equals(LONG) || type1.equals(DOUBLE)) &&
                (type2.equals(INTEGER) || type1.equals(LONG) || type2.equals(DOUBLE))) {
            return DOUBLE;
        } else {
            return null;
        }
    }

    private void incrementCounter() {
        count = count + 1;
    }

    abstract void updateDerivedSchemaRecursive(Object element);

    private static MongoType getMongoType(Object element) {
        if (element instanceof Document) {
            return DOCUMENT;
        } else if (element instanceof List) {
            return MongoType.ARRAY;
        } else if (element instanceof ObjectId) {
            return MongoType.OBJECTID;
        } else if (element instanceof String) {
            return MongoType.STRING;
        } else if (element instanceof Integer) {
            return MongoType.INTEGER;
        } else if (element instanceof Long) {
            return MongoType.LONG;
        } else if (element instanceof Double) {
            return DOUBLE;
        } else if (element instanceof Boolean) {
            return MongoType.BOOLEAN;
        } else if (element instanceof Date) {
            return MongoType.DATE;
        } else {
            throw new RuntimeException("Internal Error: Unsupported Mongo Type: " + element.getClass().toString());
        }
    }

    JsonPathElement getJsonPathElement() {
        return jsonPathElement;
    }

    MongoType getMongoType() {
        return mongoType;
    }

    int getCount() {
        return count;
    }

    Integer getFirstSeenIndex() {
        return firstSeenIndex;
    }

    List<DeriveSchemaNode> getChildren() {
        return children;
    }
}
