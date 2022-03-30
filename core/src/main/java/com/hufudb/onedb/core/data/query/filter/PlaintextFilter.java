package com.hufudb.onedb.core.data.query.filter;

import java.util.List;
import java.util.stream.Collectors;

import com.hufudb.onedb.core.data.Row;
import com.hufudb.onedb.core.data.query.QueryableDataSet;
import com.hufudb.onedb.core.sql.expression.ExpressionInterpreter;
import com.hufudb.onedb.core.sql.expression.OneDBExpression;

public class PlaintextFilter {

  public static QueryableDataSet apply(QueryableDataSet input, List<OneDBExpression> filters) {
    List<Row> rows = input.getRows();
    List<Row> newRows = rows.stream().filter(row -> filterRow(row, filters)).collect(Collectors.toList());
    rows.clear();
    rows.addAll(newRows);
    return input;
  }

  public static boolean filterRow(Row row, List<OneDBExpression> filters) {
    boolean result = true;
    for (OneDBExpression filter : filters) {
        result = result && (Boolean) ExpressionInterpreter.implement(row, filter);
        if (!result) {
            return false;
        }
    }
    return true;
  }
}
