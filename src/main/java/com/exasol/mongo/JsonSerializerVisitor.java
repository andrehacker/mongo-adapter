package com.exasol.mongo;


import com.exasol.adapter.AdapterException;
import com.exasol.adapter.sql.*;
import com.exasol.utils.JsonHelper;

import javax.json.JsonArrayBuilder;
import javax.json.JsonBuilderFactory;
import javax.json.JsonObjectBuilder;

public class JsonSerializerVisitor implements SqlNodeVisitor<JsonObjectBuilder> {

    private JsonBuilderFactory factory;

    public JsonSerializerVisitor() {
        this.factory = JsonHelper.getBuilderFactory();
    }

    @Override
    public JsonObjectBuilder visit(SqlPredicateEqual sqlPredicateEqual) throws AdapterException {
        JsonObjectBuilder root = factory.createObjectBuilder();
        JsonArrayBuilder expressionsBuilder = factory.createArrayBuilder();
        expressionsBuilder.add(sqlPredicateEqual.getLeft().accept(this));
        expressionsBuilder.add(sqlPredicateEqual.getRight().accept(this));
        root.add("expressions", expressionsBuilder);
        root.add("type", "predicate_and");
        return root;
    }

    @Override
    public JsonObjectBuilder visit(SqlColumn sqlColumn) throws AdapterException {
        JsonObjectBuilder root = factory.createObjectBuilder();
        root.add("type", "column");
        root.add("columnNr", sqlColumn.getId());
        root.add("name", sqlColumn.getName());
        // TODO tableName ?? Not stored directly in SqlColumn object
        root.add("tableName", "Unknown");
        return root;
    }

    @Override
    public JsonObjectBuilder visit(SqlLiteralString sqlLiteralString) throws AdapterException {
        JsonObjectBuilder root = factory.createObjectBuilder();
        root.add("type", "literal_string");
        root.add("value", sqlLiteralString.getValue());
        return root;
    }

    @Override
    public JsonObjectBuilder visit(SqlStatementSelect sqlStatementSelect) throws AdapterException {
        throw new RuntimeException("Internal error: visit for this type should never be called");
    }

    @Override
    public JsonObjectBuilder visit(SqlSelectList sqlSelectList) throws AdapterException {
        throw new RuntimeException("Internal error: visit for this type should never be called");
    }

    @Override
    public JsonObjectBuilder visit(SqlGroupBy sqlGroupBy) throws AdapterException {
        throw new RuntimeException("Internal error: visit for this type should never be called");
    }

    @Override
    public JsonObjectBuilder visit(SqlFunctionAggregate sqlFunctionAggregate) throws AdapterException {
        throw new RuntimeException("Internal error: visit for this type should never be called");
    }

    @Override
    public JsonObjectBuilder visit(SqlFunctionAggregateGroupConcat sqlFunctionAggregateGroupConcat) throws AdapterException {
        throw new RuntimeException("Internal error: visit for this type should never be called");
    }

    @Override
    public JsonObjectBuilder visit(SqlFunctionScalar sqlFunctionScalar) throws AdapterException {
        throw new RuntimeException("Internal error: visit for this type should never be called");
    }

    @Override
    public JsonObjectBuilder visit(SqlFunctionScalarCase sqlFunctionScalarCase) throws AdapterException {
        throw new RuntimeException("Internal error: visit for this type should never be called");
    }

    @Override
    public JsonObjectBuilder visit(SqlFunctionScalarCast sqlFunctionScalarCast) throws AdapterException {
        throw new RuntimeException("Internal error: visit for this type should never be called");
    }

    @Override
    public JsonObjectBuilder visit(SqlFunctionScalarExtract sqlFunctionScalarExtract) throws AdapterException {
        throw new RuntimeException("Internal error: visit for this type should never be called");
    }

    @Override
    public JsonObjectBuilder visit(SqlLimit sqlLimit) throws AdapterException {
        throw new RuntimeException("Internal error: visit for this type should never be called");
    }

    @Override
    public JsonObjectBuilder visit(SqlLiteralBool sqlLiteralBool) throws AdapterException {
        throw new RuntimeException("Internal error: visit for this type should never be called");
    }

    @Override
    public JsonObjectBuilder visit(SqlLiteralDate sqlLiteralDate) throws AdapterException {
        throw new RuntimeException("Internal error: visit for this type should never be called");
    }

    @Override
    public JsonObjectBuilder visit(SqlLiteralDouble sqlLiteralDouble) throws AdapterException {
        throw new RuntimeException("Internal error: visit for this type should never be called");
    }

    @Override
    public JsonObjectBuilder visit(SqlLiteralExactnumeric sqlLiteralExactnumeric) throws AdapterException {
        throw new RuntimeException("Internal error: visit for this type should never be called");
    }

    @Override
    public JsonObjectBuilder visit(SqlLiteralNull sqlLiteralNull) throws AdapterException {
        throw new RuntimeException("Internal error: visit for this type should never be called");
    }

    @Override
    public JsonObjectBuilder visit(SqlLiteralTimestamp sqlLiteralTimestamp) throws AdapterException {
        throw new RuntimeException("Internal error: visit for this type should never be called");
    }

    @Override
    public JsonObjectBuilder visit(SqlLiteralTimestampUtc sqlLiteralTimestampUtc) throws AdapterException {
        throw new RuntimeException("Internal error: visit for this type should never be called");
    }

    @Override
    public JsonObjectBuilder visit(SqlLiteralInterval sqlLiteralInterval) throws AdapterException {
        throw new RuntimeException("Internal error: visit for this type should never be called");
    }

    @Override
    public JsonObjectBuilder visit(SqlOrderBy sqlOrderBy) throws AdapterException {
        throw new RuntimeException("Internal error: visit for this type should never be called");
    }

    @Override
    public JsonObjectBuilder visit(SqlPredicateAnd sqlPredicateAnd) throws AdapterException {
        throw new RuntimeException("Internal error: visit for this type should never be called");
    }

    @Override
    public JsonObjectBuilder visit(SqlPredicateBetween sqlPredicateBetween) throws AdapterException {
        throw new RuntimeException("Internal error: visit for this type should never be called");
    }

    @Override
    public JsonObjectBuilder visit(SqlPredicateInConstList sqlPredicateInConstList) throws AdapterException {
        throw new RuntimeException("Internal error: visit for this type should never be called");
    }

    @Override
    public JsonObjectBuilder visit(SqlPredicateLess sqlPredicateLess) throws AdapterException {
        throw new RuntimeException("Internal error: visit for this type should never be called");
    }

    @Override
    public JsonObjectBuilder visit(SqlPredicateLessEqual sqlPredicateLessEqual) throws AdapterException {
        throw new RuntimeException("Internal error: visit for this type should never be called");
    }

    @Override
    public JsonObjectBuilder visit(SqlPredicateLike sqlPredicateLike) throws AdapterException {
        throw new RuntimeException("Internal error: visit for this type should never be called");
    }

    @Override
    public JsonObjectBuilder visit(SqlPredicateLikeRegexp sqlPredicateLikeRegexp) throws AdapterException {
        throw new RuntimeException("Internal error: visit for this type should never be called");
    }

    @Override
    public JsonObjectBuilder visit(SqlPredicateNot sqlPredicateNot) throws AdapterException {
        throw new RuntimeException("Internal error: visit for this type should never be called");
    }

    @Override
    public JsonObjectBuilder visit(SqlPredicateNotEqual sqlPredicateNotEqual) throws AdapterException {
        throw new RuntimeException("Internal error: visit for this type should never be called");
    }

    @Override
    public JsonObjectBuilder visit(SqlPredicateOr sqlPredicateOr) throws AdapterException {
        throw new RuntimeException("Internal error: visit for this type should never be called");
    }

    @Override
    public JsonObjectBuilder visit(SqlPredicateIsNotNull sqlPredicateIsNotNull) throws AdapterException {
        throw new RuntimeException("Internal error: visit for this type should never be called");
    }

    @Override
    public JsonObjectBuilder visit(SqlPredicateIsNull sqlPredicateIsNull) throws AdapterException {
        throw new RuntimeException("Internal error: visit for this type should never be called");
    }

    @Override
    public JsonObjectBuilder visit(SqlTable sqlTable) throws AdapterException {
        throw new RuntimeException("Internal error: visit for this type should never be called");
    }
}
