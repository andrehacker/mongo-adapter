package com.exasol.mongo.sql;

import com.exasol.adapter.sql.*;
import com.google.common.collect.ImmutableList;
import org.junit.Test;

import java.util.List;

import static org.junit.Assert.*;

public class FindNodeTypesVisitorTest {

    @Test
    public void testFindColumns() throws Exception {
        SqlSelectList selectList = SqlSelectList.createRegularSelectList(ImmutableList.of(
                new SqlColumn(0, null),
                new SqlFunctionAggregate(AggregateFunction.COUNT, ImmutableList.<SqlNode>of(new SqlColumn(1, null)), false)));

        FindNodeTypesVisitor<SqlColumn> visitor = new FindNodeTypesVisitor<>(SqlColumn.class);
        selectList.accept(visitor);
        List<SqlColumn> columns = visitor.getFoundNodes();
        assertEquals(2, columns.size());
    }
}