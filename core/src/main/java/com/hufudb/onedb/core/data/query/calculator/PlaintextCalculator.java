package com.hufudb.onedb.core.data.query.calculator;

import com.hufudb.onedb.core.data.FieldType;
import com.hufudb.onedb.core.data.Row;
import com.hufudb.onedb.core.data.Row.RowBuilder;
import com.hufudb.onedb.core.data.query.QueryableDataSet;
import com.hufudb.onedb.core.sql.expression.ExpressionInterpreter;
import com.hufudb.onedb.core.sql.expression.OneDBExpression;
import java.util.List;

public class PlaintextCalculator {
  public static QueryableDataSet apply(QueryableDataSet input, List<OneDBExpression> calcs) {
    List<FieldType> types = input.getTypeList();
    input.getRows().replaceAll(row -> calcRow(row, types, calcs));
    return input;
  }

  public static Row calcRow(Row row, List<FieldType> types, List<OneDBExpression> calcs) {
    final int length = calcs.size();
    RowBuilder builder = Row.newBuilder(length);
    for (int i = 0; i < length; ++i) {
      builder.set(
          i,
          ExpressionInterpreter.cast(
              ExpressionInterpreter.implement(row, calcs.get(i)), types.get(i)));
    }
    return builder.build();
  }
}
