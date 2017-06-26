package com.exasol.mongo.sql;

import com.exasol.adapter.AdapterException;
import com.exasol.adapter.metadata.ColumnMetadata;
import com.exasol.adapter.metadata.DataType;
import com.exasol.adapter.sql.*;

import java.util.ArrayList;
import java.util.List;

import static com.exasol.adapter.sql.AggregateFunction.COUNT;

public class SqlHelper {

    public static boolean hasSelectListExpressions(SqlSelectList selectList) {
        if (selectList.isRequestAnyColumn()) {
            return false;
        } else if (selectList.isSelectStar()) {
            return false;
        } else {
            return selectList.getExpressions().stream().anyMatch(sqlNode -> !sqlNode.getType().equals(SqlNodeType.COLUMN));
        }
    }

    public static boolean isCountStar(SqlSelectList selectList) throws AdapterException {
        return hasSelectListExpressions(selectList)
                && selectList.getExpressions().size() == 1
                && (selectList.getExpressions().get(0).getType() == SqlNodeType.FUNCTION_AGGREGATE)
                && ((SqlFunctionAggregate) selectList.getExpressions().get(0)).getFunction() == COUNT
                && ((SqlFunctionAggregate) selectList.getExpressions().get(0)).getArguments().size() == 0;
    }

    public static boolean isPushUpNeeded(SqlStatementSelect select) throws AdapterException {
        return hasSelectListExpressions(select.getSelectList()) && !isCountStar(select.getSelectList());
    }

    public static SqlStatementSelect pushUpSelectListExpressions(SqlStatementSelect select) throws AdapterException {
        assert(isPushUpNeeded(select));
        FindNodeTypesVisitor<SqlColumn> visitor = new FindNodeTypesVisitor<>(SqlColumn.class);
        select.getSelectList().accept(visitor);
        List<SqlColumn> columns = visitor.getFoundNodes();
        if (columns.isEmpty()) {
            ColumnMetadata firstColMeta = select.getFromClause().getMetadata().getColumns().get(0);
            columns.add(new SqlColumn(0, firstColMeta));
        }
        return new SqlStatementSelect(select.getFromClause(), SqlSelectList.createRegularSelectList(new ArrayList<>(columns)), select.getWhereClause(), select.getGroupBy(), select.getHaving(), select.getOrderBy(), select.getLimit());
    }

    public static SqlStatementSelect replaceRequestAnyColumnByFirstColumn(SqlStatementSelect select) {
        assert(select.getSelectList().isRequestAnyColumn());
        ColumnMetadata firstColMeta = select.getFromClause().getMetadata().getColumns().get(0);
        List<SqlColumn> columns = new ArrayList<>();
        columns.add(new SqlColumn(0, firstColMeta));
        return new SqlStatementSelect(select.getFromClause(), SqlSelectList.createRegularSelectList(new ArrayList<>(columns)), select.getWhereClause(), select.getGroupBy(), select.getHaving(), select.getOrderBy(), select.getLimit());
    }
}
