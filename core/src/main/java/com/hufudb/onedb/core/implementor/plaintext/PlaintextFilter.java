package com.hufudb.onedb.core.implementor.plaintext;

import com.hufudb.onedb.core.data.Row;
import com.hufudb.onedb.core.implementor.QueryableDataSet;
import com.hufudb.onedb.core.sql.expression.OneDBExpression;
import java.util.List;

public class PlaintextFilter {

  public static QueryableDataSet apply(QueryableDataSet input, List<OneDBExpression> filters) {
    input.getRows().removeIf(row -> !filterRow(row, filters));
    return input;
  }

  public static boolean filterRow(Row row, List<OneDBExpression> filters) {
    boolean result = true;
    for (OneDBExpression filter : filters) {
      result = result && (Boolean) PlaintextInterpreter.implement(row, filter);
      if (!result) {
        return false;
      }
    }
    return true;
  }
}
