package com.exasol.mongo.sql;

import com.exasol.adapter.AdapterException;
import com.exasol.adapter.sql.*;

import java.util.ArrayList;
import java.util.List;

public class FindNodeTypesVisitor<T extends SqlNode> implements SqlNodeVisitor<Void> {

    List<T> foundNodes = new ArrayList<>();
    private final Class<T> clazz;

    public FindNodeTypesVisitor(Class<T> clazz) {
        this.clazz = clazz;
    }

    private void addIfType(SqlNode node) {
        if (clazz.isInstance(node)) {
            foundNodes.add((T) node);
        }
    }

    public List<T> getFoundNodes() {
        return foundNodes;
    }

    @Override
    public Void visit(SqlStatementSelect sqlStatementSelect) throws AdapterException {
        addIfType(sqlStatementSelect);
        return null;
    }

    @Override
    public Void visit(SqlSelectList node) throws AdapterException {
        addIfType(node);
        if (node.getExpressions() != null) {
            for (SqlNode subNode : node.getExpressions()) {
                subNode.accept(this);
            }
        }
        return null;
    }

    @Override
    public Void visit(SqlGroupBy node) throws AdapterException {
        addIfType(node);
        if (node.getExpressions() != null) {
            for (SqlNode subNode : node.getExpressions()) {
                subNode.accept(this);
            }
        }
        return null;
    }

    @Override
    public Void visit(SqlColumn node) throws AdapterException {
        addIfType(node);
        return null;
    }

    @Override
    public Void visit(SqlFunctionAggregate node) throws AdapterException {
        addIfType(node);
        if (node.getArguments() != null) {
            for (SqlNode subNode : node.getArguments()) {
                subNode.accept(this);
            }
        }
        return null;
    }

    @Override
    public Void visit(SqlFunctionAggregateGroupConcat node) throws AdapterException {
        addIfType(node); node.getOrderBy();
        if (node.getArguments() != null) {
            for (SqlNode subNode : node.getArguments()) {
                subNode.accept(this);
            }
        }
        if (node.getOrderBy() != null) {
            for (SqlNode subNode : node.getOrderBy().getExpressions()) {
                subNode.accept(this);
            }
        }
        return null;
    }

    @Override
    public Void visit(SqlFunctionScalar node) throws AdapterException {
        addIfType(node);
        if (node.getArguments() != null) {
            for (SqlNode subNode : node.getArguments()) {
                subNode.accept(this);
            }
        }
        return null;
    }

    @Override
    public Void visit(SqlFunctionScalarCase node) throws AdapterException {
        addIfType(node);
        if (node.getArguments() != null) {
            for (SqlNode subNode : node.getArguments()) {
                subNode.accept(this);
            }
        }
        if (node.getResults() != null) {
            for (SqlNode subNode : node.getResults()) {
                subNode.accept(this);
            }
        }
        if (node.getBasis() != null) {
            node.getBasis().accept(this);
        }
        return null;
    }

    @Override
    public Void visit(SqlFunctionScalarCast node) throws AdapterException {
        addIfType(node);
        if (node.getArguments() != null) {
            for (SqlNode subNode : node.getArguments()) {
                subNode.accept(this);
            }
        }
        return null;
    }

    @Override
    public Void visit(SqlFunctionScalarExtract node) throws AdapterException {
        addIfType(node);
        if (node.getArguments() != null) {
            for (SqlNode subNode : node.getArguments()) {
                subNode.accept(this);
            }
        }
        return null;
    }

    @Override
    public Void visit(SqlLimit node) throws AdapterException {
        addIfType(node);
        return null;
    }

    @Override
    public Void visit(SqlLiteralBool node) throws AdapterException {
        addIfType(node);
        return null;
    }

    @Override
    public Void visit(SqlLiteralDate node) throws AdapterException {
        addIfType(node);
        return null;
    }

    @Override
    public Void visit(SqlLiteralDouble node) throws AdapterException {
        addIfType(node);
        return null;
    }

    @Override
    public Void visit(SqlLiteralExactnumeric node) throws AdapterException {
        addIfType(node);
        return null;
    }

    @Override
    public Void visit(SqlLiteralNull node) throws AdapterException {
        addIfType(node);
        return null;
    }

    @Override
    public Void visit(SqlLiteralString node) throws AdapterException {
        addIfType(node);
        return null;
    }

    @Override
    public Void visit(SqlLiteralTimestamp node) throws AdapterException {
        addIfType(node);
        return null;
    }

    @Override
    public Void visit(SqlLiteralTimestampUtc node) throws AdapterException {
        addIfType(node);
        return null;
    }

    @Override
    public Void visit(SqlLiteralInterval node) throws AdapterException {
        addIfType(node);
        return null;
    }

    @Override
    public Void visit(SqlOrderBy node) throws AdapterException {
        addIfType(node);
        if (node.getExpressions() != null) {
            for (SqlNode subNode : node.getExpressions()) {
                subNode.accept(this);
            }
        }
        return null;
    }

    @Override
    public Void visit(SqlPredicateAnd node) throws AdapterException {
        addIfType(node);
        for (SqlNode subNode : node.getAndedPredicates()) {
            subNode.accept(this);
        }
        return null;
    }

    @Override
    public Void visit(SqlPredicateBetween node) throws AdapterException {
        addIfType(node);
        node.getBetweenLeft().accept(this);
        node.getBetweenRight().accept(this);
        node.getExpression().accept(this);
        return null;
    }

    @Override
    public Void visit(SqlPredicateEqual node) throws AdapterException {
        addIfType(node);
        node.getLeft().accept(this);
        node.getRight().accept(this);
        return null;
    }

    @Override
    public Void visit(SqlPredicateInConstList node) throws AdapterException {
        addIfType(node);
        node.getExpression().accept(this);
        for (SqlNode subNode : node.getInArguments()) {
            subNode.accept(this);
        }
        return null;
    }

    @Override
    public Void visit(SqlPredicateLess node) throws AdapterException {
        addIfType(node);
        node.getLeft().accept(this);
        node.getRight().accept(this);
        return null;
    }

    @Override
    public Void visit(SqlPredicateLessEqual node) throws AdapterException {
        addIfType(node);
        node.getLeft().accept(this);
        node.getRight().accept(this);
        return null;
    }

    @Override
    public Void visit(SqlPredicateLike node) throws AdapterException {
        addIfType(node);
        node.getLeft().accept(this);
        node.getEscapeChar().accept(this);
        node.getPattern().accept(this);
        return null;
    }

    @Override
    public Void visit(SqlPredicateLikeRegexp node) throws AdapterException {
        node.getLeft().accept(this);
        node.getPattern().accept(this);
        addIfType(node);
        return null;
    }

    @Override
    public Void visit(SqlPredicateNot node) throws AdapterException {
        addIfType(node);
        node.getExpression().accept(this);
        return null;
    }

    @Override
    public Void visit(SqlPredicateNotEqual node) throws AdapterException {
        addIfType(node);
        node.getLeft().accept(this);
        node.getRight().accept(this);
        return null;
    }

    @Override
    public Void visit(SqlPredicateOr node) throws AdapterException {
        addIfType(node);
        for (SqlNode subNode : node.getOrPredicates()) {
            subNode.accept(this);
        }
        return null;
    }

    @Override
    public Void visit(SqlPredicateIsNotNull node) throws AdapterException {
        addIfType(node);
        node.getExpression().accept(this);
        return null;
    }

    @Override
    public Void visit(SqlPredicateIsNull node) throws AdapterException {
        addIfType(node);
        node.getExpression().accept(this);
        return null;
    }

    @Override
    public Void visit(SqlTable node) throws AdapterException {
        addIfType(node);
        return null;
    }
}
