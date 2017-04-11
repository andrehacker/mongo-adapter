package com.exasol.mongo;


import com.exasol.adapter.AdapterException;
import com.exasol.adapter.sql.*;
import com.exasol.utils.JsonHelper;
import com.google.common.collect.ImmutableSet;
import org.bson.conversions.Bson;

import javax.json.JsonArrayBuilder;
import javax.json.JsonBuilderFactory;
import javax.json.JsonObjectBuilder;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import static com.mongodb.client.model.Filters.*;

public class MongoFilterGeneratorVisitor implements SqlNodeVisitor<Bson> {

    private List<MongoColumnMapping> columnsMapping;

    public MongoFilterGeneratorVisitor(List<MongoColumnMapping> columnsMapping) {
        this.columnsMapping = columnsMapping;
    }

    public static MongoColumnMapping getColumnMappingByName(List<MongoColumnMapping> columnsMapping, String columnName) {
        for (MongoColumnMapping mapping : columnsMapping) {
            if (mapping.getColumnName().equals(columnName)) {
                return mapping;
            }
        }
        throw new RuntimeException("Internal error: Could not find mapping for " + columnName);
    }

    // TODO Would be good to have a supertype SqlLiteral
    private Set<SqlNodeType> supportedLiterals = ImmutableSet.of(SqlNodeType.LITERAL_BOOL, SqlNodeType.LITERAL_DOUBLE, SqlNodeType.LITERAL_EXACTNUMERIC, SqlNodeType.LITERAL_STRING);

    @Override
    public Bson visit(SqlPredicateEqual sqlPredicateEqual) throws AdapterException {
        SqlColumn column;
        SqlNode literal;
        if (sqlPredicateEqual.getLeft().getType() == SqlNodeType.COLUMN) {
            column = (SqlColumn) sqlPredicateEqual.getLeft();
            literal = sqlPredicateEqual.getRight();
        } else if (supportedLiterals.contains(sqlPredicateEqual.getLeft().getType())) {
            if (!(sqlPredicateEqual.getRight().getType() == SqlNodeType.COLUMN)) {
                throw new AdapterException("Unsupported predicate: " + sqlPredicateEqual.toSimpleSql());
            }
            column = (SqlColumn) sqlPredicateEqual.getRight();
            literal = sqlPredicateEqual.getLeft();
        } else {
            throw new AdapterException("Unsupported predicate: " + sqlPredicateEqual.toSimpleSql());
        }
        String mongoField = getColumnMappingByName(columnsMapping, column.getName()).getJsonPath();
        // TODO Handle nested paths!
        if (mongoField.contains(".")) {
            throw new AdapterException("Filters on nested fields are not yet supported");
        }
        return eq(mongoField, getLiteralValueForFilter(literal));
    }

    private Object getLiteralValueForFilter(SqlNode literal) throws AdapterException {
        if (literal.getType() == SqlNodeType.LITERAL_STRING) {
            // TODO If the filter column is of ObjectId type, this must be a ObjectId! Otherwise filter does not work.
            return ((SqlLiteralString)literal).getValue();
        } else if (literal.getType() == SqlNodeType.LITERAL_BOOL) {
            return ((SqlLiteralBool)literal).getValue();
        } else if (literal.getType() == SqlNodeType.LITERAL_DOUBLE) {
            return ((SqlLiteralDouble)literal).getValue();
        } else if (literal.getType() == SqlNodeType.LITERAL_EXACTNUMERIC) {
            BigDecimal bigDecimal = ((SqlLiteralExactnumeric)literal).getValue();
            if (isIntegerValue(bigDecimal)) {
                if (isInt32(bigDecimal.toBigIntegerExact())) {
                    return bigDecimal.intValue();
                } else if (isLong(bigDecimal.toBigIntegerExact())) {
                    return bigDecimal.longValue();
                } else {
                    // TODO Find nicer solution
                    throw new AdapterException("Filter for too large value: " + bigDecimal.toString());
                }
            } else {
                // TODO Find nicer solution
                throw new AdapterException("Cannot filter for decimal value with fractional part: " + bigDecimal.toString());
            }
        } else {
            throw new RuntimeException("Unsupported Literal: " + literal.getType());
        }
    }

    private static boolean isIntegerValue(BigDecimal bd) {
        return bd.signum() == 0 || bd.scale() <= 0 || bd.stripTrailingZeros().scale() <= 0;
    }

    private static boolean isInt32(BigInteger value) {
        return BigInteger.valueOf(Integer.MAX_VALUE).compareTo(value) >= 0 &&
                BigInteger.valueOf(-Integer.MAX_VALUE).compareTo(value) <= 0;
    }

    private static boolean isLong(BigInteger value) {
        return BigInteger.valueOf(Long.MAX_VALUE).compareTo(value) >= 0 &&
                BigInteger.valueOf(-Long.MAX_VALUE).compareTo(value) <= 0;
    }

    @Override
    public Bson visit(SqlPredicateLess sqlPredicateLess) throws AdapterException {
        SqlColumn column;
        SqlNode literal;
        boolean invert = false;
        if (sqlPredicateLess.getLeft().getType() == SqlNodeType.COLUMN) {
            column = (SqlColumn) sqlPredicateLess.getLeft();
            literal = sqlPredicateLess.getRight();
        } else if (supportedLiterals.contains(sqlPredicateLess.getLeft().getType())) {
            invert = true;
            if (!(sqlPredicateLess.getRight().getType() == SqlNodeType.COLUMN)) {
                throw new AdapterException("Unsupported predicate: " + sqlPredicateLess.toSimpleSql());
            }
            column = (SqlColumn) sqlPredicateLess.getRight();
            literal = sqlPredicateLess.getLeft();
        } else {
            throw new AdapterException("Unsupported predicate: " + sqlPredicateLess.toSimpleSql());
        }
        String mongoField = getColumnMappingByName(columnsMapping, column.getName()).getJsonPath();
        // TODO Handle nested paths!
        if (mongoField.contains(".")) {
            throw new AdapterException("Filters on nested fields are not yet supported");
        }
        if (!invert) {
            return lt(mongoField, getLiteralValueForFilter(literal));
        } else {
            return gt(mongoField, getLiteralValueForFilter(literal));
        }
    }

    @Override
    public Bson visit(SqlPredicateLessEqual sqlPredicateLessEqual) throws AdapterException {
        SqlColumn column;
        SqlNode literal;
        boolean invert = false;
        if (sqlPredicateLessEqual.getLeft().getType() == SqlNodeType.COLUMN) {
            column = (SqlColumn) sqlPredicateLessEqual.getLeft();
            literal = sqlPredicateLessEqual.getRight();
        } else if (supportedLiterals.contains(sqlPredicateLessEqual.getLeft().getType())) {
            invert = true;
            if (!(sqlPredicateLessEqual.getRight().getType() == SqlNodeType.COLUMN)) {
                throw new AdapterException("Unsupported predicate: " + sqlPredicateLessEqual.toSimpleSql());
            }
            column = (SqlColumn) sqlPredicateLessEqual.getRight();
            literal = sqlPredicateLessEqual.getLeft();
        } else {
            throw new AdapterException("Unsupported predicate: " + sqlPredicateLessEqual.toSimpleSql());
        }
        String mongoField = getColumnMappingByName(columnsMapping, column.getName()).getJsonPath();
        // TODO Handle nested paths!
        if (mongoField.contains(".")) {
            throw new AdapterException("Filters on nested fields are not yet supported");
        }
        if (!invert) {
            return lte(mongoField, getLiteralValueForFilter(literal));
        } else {
            return gte(mongoField, getLiteralValueForFilter(literal));
        }
    }

    @Override
    public Bson visit(SqlPredicateAnd sqlPredicateAnd) throws AdapterException {
        List<Bson> predicates = new ArrayList<>();
        for (SqlNode node : sqlPredicateAnd.getAndedPredicates()) {
            predicates.add(node.accept(this));
        }
        return and(predicates);
    }

    @Override
    public Bson visit(SqlColumn sqlColumn) throws AdapterException {
        throw new RuntimeException("Internal error: visit for this type should never be called");
    }

    @Override
    public Bson visit(SqlLiteralString sqlLiteralString) throws AdapterException {
        throw new RuntimeException("Internal error: visit for this type should never be called");
    }

    @Override
    public Bson visit(SqlStatementSelect sqlStatementSelect) throws AdapterException {
        throw new RuntimeException("Internal error: visit for this type should never be called");
    }

    @Override
    public Bson visit(SqlSelectList sqlSelectList) throws AdapterException {
        throw new RuntimeException("Internal error: visit for this type should never be called");
    }

    @Override
    public Bson visit(SqlGroupBy sqlGroupBy) throws AdapterException {
        throw new RuntimeException("Internal error: visit for this type should never be called");
    }

    @Override
    public Bson visit(SqlFunctionAggregate sqlFunctionAggregate) throws AdapterException {
        throw new RuntimeException("Internal error: visit for this type should never be called");
    }

    @Override
    public Bson visit(SqlFunctionAggregateGroupConcat sqlFunctionAggregateGroupConcat) throws AdapterException {
        throw new RuntimeException("Internal error: visit for this type should never be called");
    }

    @Override
    public Bson visit(SqlFunctionScalar sqlFunctionScalar) throws AdapterException {
        throw new RuntimeException("Internal error: visit for this type should never be called");
    }

    @Override
    public Bson visit(SqlFunctionScalarCase sqlFunctionScalarCase) throws AdapterException {
        throw new RuntimeException("Internal error: visit for this type should never be called");
    }

    @Override
    public Bson visit(SqlFunctionScalarCast sqlFunctionScalarCast) throws AdapterException {
        throw new RuntimeException("Internal error: visit for this type should never be called");
    }

    @Override
    public Bson visit(SqlFunctionScalarExtract sqlFunctionScalarExtract) throws AdapterException {
        throw new RuntimeException("Internal error: visit for this type should never be called");
    }

    @Override
    public Bson visit(SqlLimit sqlLimit) throws AdapterException {
        throw new RuntimeException("Internal error: visit for this type should never be called");
    }

    @Override
    public Bson visit(SqlLiteralBool sqlLiteralBool) throws AdapterException {
        throw new RuntimeException("Internal error: visit for this type should never be called");
    }

    @Override
    public Bson visit(SqlLiteralDate sqlLiteralDate) throws AdapterException {
        throw new RuntimeException("Internal error: visit for this type should never be called");
    }

    @Override
    public Bson visit(SqlLiteralDouble sqlLiteralDouble) throws AdapterException {
        throw new RuntimeException("Internal error: visit for this type should never be called");
    }

    @Override
    public Bson visit(SqlLiteralExactnumeric sqlLiteralExactnumeric) throws AdapterException {
        throw new RuntimeException("Internal error: visit for this type should never be called");
    }

    @Override
    public Bson visit(SqlLiteralNull sqlLiteralNull) throws AdapterException {
        throw new RuntimeException("Internal error: visit for this type should never be called");
    }

    @Override
    public Bson visit(SqlLiteralTimestamp sqlLiteralTimestamp) throws AdapterException {
        throw new RuntimeException("Internal error: visit for this type should never be called");
    }

    @Override
    public Bson visit(SqlLiteralTimestampUtc sqlLiteralTimestampUtc) throws AdapterException {
        throw new RuntimeException("Internal error: visit for this type should never be called");
    }

    @Override
    public Bson visit(SqlLiteralInterval sqlLiteralInterval) throws AdapterException {
        throw new RuntimeException("Internal error: visit for this type should never be called");
    }

    @Override
    public Bson visit(SqlOrderBy sqlOrderBy) throws AdapterException {
        throw new RuntimeException("Internal error: visit for this type should never be called");
    }

    @Override
    public Bson visit(SqlPredicateBetween sqlPredicateBetween) throws AdapterException {
        throw new RuntimeException("Internal error: visit for this type should never be called");
    }

    @Override
    public Bson visit(SqlPredicateInConstList sqlPredicateInConstList) throws AdapterException {
        throw new RuntimeException("Internal error: visit for this type should never be called");
    }

    @Override
    public Bson visit(SqlPredicateLike sqlPredicateLike) throws AdapterException {
        throw new RuntimeException("Internal error: visit for this type should never be called");
    }

    @Override
    public Bson visit(SqlPredicateLikeRegexp sqlPredicateLikeRegexp) throws AdapterException {
        throw new RuntimeException("Internal error: visit for this type should never be called");
    }

    @Override
    public Bson visit(SqlPredicateNot sqlPredicateNot) throws AdapterException {
        throw new RuntimeException("Internal error: visit for this type should never be called");
    }

    @Override
    public Bson visit(SqlPredicateNotEqual sqlPredicateNotEqual) throws AdapterException {
        throw new RuntimeException("Internal error: visit for this type should never be called");
    }

    @Override
    public Bson visit(SqlPredicateOr sqlPredicateOr) throws AdapterException {
        throw new RuntimeException("Internal error: visit for this type should never be called");
    }

    @Override
    public Bson visit(SqlPredicateIsNotNull sqlPredicateIsNotNull) throws AdapterException {
        throw new RuntimeException("Internal error: visit for this type should never be called");
    }

    @Override
    public Bson visit(SqlPredicateIsNull sqlPredicateIsNull) throws AdapterException {
        throw new RuntimeException("Internal error: visit for this type should never be called");
    }

    @Override
    public Bson visit(SqlTable sqlTable) throws AdapterException {
        throw new RuntimeException("Internal error: visit for this type should never be called");
    }
}
