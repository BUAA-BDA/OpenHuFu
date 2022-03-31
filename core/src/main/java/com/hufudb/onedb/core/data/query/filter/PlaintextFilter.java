package com.hufudb.onedb.core.data.query.filter;

import java.util.List;

import com.hufudb.onedb.core.data.Row;
import com.hufudb.onedb.core.data.query.QueryableDataSet;
import com.hufudb.onedb.core.sql.expression.ExpressionInterpreter;
import com.hufudb.onedb.core.sql.expression.OneDBExpression;

public class PlaintextFilter {

  public static QueryableDataSet apply(QueryableDataSet input, List<OneDBExpression> filters) {
    input.getRows().removeIf(row -> !filterRow(row, filters));
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
