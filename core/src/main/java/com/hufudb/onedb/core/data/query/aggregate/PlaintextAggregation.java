package com.hufudb.onedb.core.data.query.aggregate;

import com.hufudb.onedb.core.data.FieldType;
import com.hufudb.onedb.core.data.Row;
import com.hufudb.onedb.core.data.Row.RowBuilder;
import com.hufudb.onedb.core.data.query.QueryableDataSet;
import com.hufudb.onedb.core.sql.expression.ExpressionInterpreter;
import com.hufudb.onedb.core.sql.expression.OneDBExpression;
import java.util.ArrayList;
import java.util.List;

public class PlaintextAggregation {
  public static QueryableDataSet apply(QueryableDataSet input, List<OneDBExpression> aggs) {
    // build aggregate function list
    List<AggregateFunction<Row, Comparable>> aggFunctions = new ArrayList<>();
    List<FieldType> types = new ArrayList<>();
    for (OneDBExpression exp : aggs) {
      aggFunctions.add(PlaintextAggregateFunctions.getAggregateFunc(exp));
      types.add(exp.getOutType());
    }
    return applyAggregateFunctions(input, aggFunctions, types);
  }

  public static QueryableDataSet apply(QueryableDataSet input, List<Integer> groups, List<OneDBExpression> aggs) {
    List<AggregateFunction<Row, Comparable>> aggFunctions = new ArrayList<>();
    List<FieldType> types = new ArrayList<>();
    for (OneDBExpression exp : aggs) {
      aggFunctions.add(PlaintextAggregateFunctions.getAggregateFunc(exp));
      types.add(exp.getOutType());
    }
    return applyAggregateFunctions(input, aggFunctions, types);
  }

  public static QueryableDataSet applyAggregateFunctions(QueryableDataSet input,
      List<AggregateFunction<Row, Comparable>> aggFunctions, List<FieldType> types) {
    // aggregate input rows
    List<Row> rows = input.getRows();
    int length = aggFunctions.size();
    for (Row row : rows) {
      for (int i = 0; i < length; ++i) {
        aggFunctions.get(i).add(row);
      }
    }
    // get result
    RowBuilder builder = Row.newBuilder(length);
    for (int i = 0; i < length; ++i) {
      builder.set(i, ExpressionInterpreter.cast(aggFunctions.get(i).aggregate(), types.get(i)));
    }
    rows.clear();
    rows.add(builder.build());
    return input;
  }
}
