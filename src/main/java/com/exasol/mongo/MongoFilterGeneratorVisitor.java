package com.exasol.mongo;


import com.exasol.adapter.AdapterException;
import com.exasol.adapter.sql.*;
import com.exasol.jsonpath.JsonPathElement;
import com.google.common.collect.ImmutableSet;
import org.bson.conversions.Bson;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import static com.mongodb.client.model.Filters.*;
import static java.util.stream.Collectors.joining;

public class MongoFilterGeneratorVisitor implements SqlNodeVisitor<Bson> {

    private List<MongoColumnMapping> columnsMapping;

    public MongoFilterGeneratorVisitor(List<MongoColumnMapping> columnsMapping) {
        this.columnsMapping = columnsMapping;
    }

    // TODO COMMON Would be good to have a supertype SqlLiteral
    private Set<SqlNodeType> supportedLiterals = ImmutableSet.of(SqlNodeType.LITERAL_BOOL, SqlNodeType.LITERAL_DOUBLE, SqlNodeType.LITERAL_EXACTNUMERIC, SqlNodeType.LITERAL_STRING);

    public static MongoColumnMapping getColumnMappingByName(List<MongoColumnMapping> columnsMapping, String columnName) {
        for (MongoColumnMapping mapping : columnsMapping) {
            if (mapping.getColumnName().equals(columnName)) {
                return mapping;
            }
        }
        throw new RuntimeException("Internal error: Could not find mapping for " + columnName);
    }

    /**
     * Given a column name in the virtual table, it returns the key to be used for a filter.
     * If you want to filter a nested document value, e.g. artist.name, will return "artist.name",
     * which matches documents having a document with key "artist" containing a field "name", and
     * potentially other fields. There is also a way to search for an exact match, but this is not
     * supported by the Adapter.
     */
    private String getMongoFilterKeyByColumnName(String columnName) {
        MongoColumnMapping colMapping = getColumnMappingByName(columnsMapping, columnName);
        // JsonPath could look like "$.fieldname", but projection should look like "fieldname"
        return colMapping.getJsonPathParsed().stream().map(JsonPathElement::toJsonPathString).collect(joining(".")); //"";
    }

    private Object getLiteralValueForFilter(SqlNode literal) throws AdapterException {
        if (literal.getType() == SqlNodeType.LITERAL_STRING) {
            // TODO If the filter column is of ObjectId type, this must be a ObjectId! Otherwise filter does not work. But how do we know which column we filter for here?
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
    public Bson visit(SqlPredicateEqual sqlPredicateEqual) throws AdapterException {
        return getEqualNonEqual(sqlPredicateEqual.getLeft(), sqlPredicateEqual.getRight(), false);
    }

    @Override
    public Bson visit(SqlPredicateNotEqual sqlPredicateNotEqual) throws AdapterException {
        return getEqualNonEqual(sqlPredicateNotEqual.getLeft(), sqlPredicateNotEqual.getRight(), true);
    }

    private Bson getEqualNonEqual(SqlNode left, SqlNode right, boolean notEqual) throws AdapterException {
        SqlColumn column;
        SqlNode literal;
        if (left.getType() == SqlNodeType.COLUMN) {
            column = (SqlColumn) left;
            literal = right;
        } else if (supportedLiterals.contains(left.getType())) {
            if (!(right.getType() == SqlNodeType.COLUMN)) {
                throw new RuntimeException("Unsupported predicate: " + right.toString()); // TODO COMMON Make toSimpleSql public
            }
            column = (SqlColumn) right;
            literal = left;
        } else {
            throw new RuntimeException("Unsupported predicate: " + left.toString() + " " + right.toString()); // TODO COMMON Make toSimpleSql public
        }
        String mongoFilterKey = getMongoFilterKeyByColumnName(column.getName());
        if (notEqual) {
            return ne(mongoFilterKey, getLiteralValueForFilter(literal));
        } else {
            return eq(mongoFilterKey, getLiteralValueForFilter(literal));
        }
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
        String mongoFilterKey = getMongoFilterKeyByColumnName(column.getName());
        if (!invert) {
            return lt(mongoFilterKey, getLiteralValueForFilter(literal));
        } else {
            return gt(mongoFilterKey, getLiteralValueForFilter(literal));
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
        String mongoFilterKey = getMongoFilterKeyByColumnName(column.getName());
        if (!invert) {
            return lte(mongoFilterKey, getLiteralValueForFilter(literal));
        } else {
            return gte(mongoFilterKey, getLiteralValueForFilter(literal));
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
    public Bson visit(SqlPredicateOr sqlPredicateOr) throws AdapterException {
        List<Bson> predicates = new ArrayList<>();
        for (SqlNode node : sqlPredicateOr.getOrPredicates()) {
            predicates.add(node.accept(this));
        }
        return or(predicates);
    }

    @Override
    public Bson visit(SqlPredicateBetween sqlPredicateBetween) throws AdapterException {
        // TODO COMMON Make toSimpleSql public!!!
        if (sqlPredicateBetween.getExpression().getType() != SqlNodeType.COLUMN) {
            throw new RuntimeException("Internal error: Between with non-column expression should never be called: " + sqlPredicateBetween.getExpression().getType());
        }
        SqlColumn column = (SqlColumn) sqlPredicateBetween.getExpression();
        String mongoFilterKey = getMongoFilterKeyByColumnName(column.getName());
        SqlNode left = sqlPredicateBetween.getBetweenLeft();
        SqlNode right = sqlPredicateBetween.getBetweenRight();
        return and(gte(mongoFilterKey, getLiteralValueForFilter(left)), lte(mongoFilterKey, getLiteralValueForFilter(right)));
    }

    @Override
    public Bson visit(SqlPredicateNot sqlPredicateNot) throws AdapterException {
        return not(sqlPredicateNot.getExpression().accept(this));
    }

    @Override
    public Bson visit(SqlPredicateInConstList sqlPredicateInConstList) throws AdapterException {
        if (sqlPredicateInConstList.getExpression().getType() != SqlNodeType.COLUMN) {
            throw new RuntimeException("Internal error: In with non-column expression should never be called: " + sqlPredicateInConstList.getExpression().getType());
        }
        List<Object> inArgs = new ArrayList<>();
        for (SqlNode node : sqlPredicateInConstList.getInArguments()) {
            inArgs.add(getLiteralValueForFilter(node));
        }
        SqlColumn column = (SqlColumn) sqlPredicateInConstList.getExpression();
        String mongoFilterKey = getMongoFilterKeyByColumnName(column.getName());
        return in(mongoFilterKey, inArgs);
    }

    @Override
    public Bson visit(SqlPredicateIsNotNull sqlPredicateIsNotNull) throws AdapterException {
        if (sqlPredicateIsNotNull.getExpression().getType() != SqlNodeType.COLUMN) {
            throw new RuntimeException("Internal error: IsNotNull with non-column expression should never be called: " + sqlPredicateIsNotNull.getExpression().getType());
        }
        SqlColumn column = (SqlColumn)sqlPredicateIsNotNull.getExpression();
        String mongoFilterKey = getMongoFilterKeyByColumnName(column.getName());
        return and(exists(mongoFilterKey, true), ne(mongoFilterKey, ""));   // TODO Check if this is a problem if mongoFilterKey is not of type string
    }

    @Override
    public Bson visit(SqlPredicateIsNull sqlPredicateIsNull) throws AdapterException {
        if (sqlPredicateIsNull.getExpression().getType() != SqlNodeType.COLUMN) {
            throw new RuntimeException("Internal error: IsNull with non-column expression should never be called: " + sqlPredicateIsNull.getExpression().getType());
        }
        SqlColumn column = (SqlColumn)sqlPredicateIsNull.getExpression();
        String mongoFilterKey = getMongoFilterKeyByColumnName(column.getName());
        return or(exists(mongoFilterKey, false), eq(mongoFilterKey, ""));   // TODO Check if this is a problem if mongoFilterKey is not of type string
    }

    @Override
    public Bson visit(SqlPredicateLikeRegexp sqlPredicateLikeRegexp) throws AdapterException {
        if (sqlPredicateLikeRegexp.getLeft().getType() != SqlNodeType.COLUMN) {
            throw new RuntimeException("Internal error: Adapter only supports regexp like with column on left side: " + sqlPredicateLikeRegexp.getLeft().getType());
        }
        if (sqlPredicateLikeRegexp.getPattern().getType() != SqlNodeType.LITERAL_STRING) {
            throw new RuntimeException("Internal error: Adapter only supports regexp like with string as pattern: " + sqlPredicateLikeRegexp.getPattern().getType());
        }
        SqlColumn column = (SqlColumn) sqlPredicateLikeRegexp.getLeft();
        String mongoFilterKey = getMongoFilterKeyByColumnName(column.getName());
        return regex(mongoFilterKey, ((SqlLiteralString)sqlPredicateLikeRegexp.getPattern()).getValue());
    }

    /**
     * Handled by the other visit methods where columns can occur.
     */
    @Override
    public Bson visit(SqlColumn sqlColumn) throws AdapterException {
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
    public Bson visit(SqlLiteralDate sqlLiteralDate) throws AdapterException {
        throw new RuntimeException("Internal error: visit for this type should never be called");
    }

    @Override
    public Bson visit(SqlLiteralNull sqlLiteralNull) throws AdapterException {
        throw new RuntimeException("Internal error: visit for this type should never be called");
    }

    @Override
    public Bson visit(SqlLiteralString sqlLiteralString) throws AdapterException {
        throw new RuntimeException("Internal error: visit for this type should never be called");
    }

    @Override
    public Bson visit(SqlLiteralBool sqlLiteralBool) throws AdapterException {
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
    public Bson visit(SqlPredicateLike sqlPredicateLike) throws AdapterException {
        throw new RuntimeException("Internal error: visit for this type should never be called");
    }

    @Override
    public Bson visit(SqlTable sqlTable) throws AdapterException {
        throw new RuntimeException("Internal error: visit for this type should never be called");
    }
}
