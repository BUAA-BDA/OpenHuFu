package com.hufudb.onedb.core.data.query.aggregate;

import com.hufudb.onedb.core.data.FieldType;
import com.hufudb.onedb.core.data.Header;
import com.hufudb.onedb.core.data.Row;
import com.hufudb.onedb.core.data.query.QueryableDataSet;
import com.hufudb.onedb.core.sql.expression.OneDBExpression;
import java.util.ArrayList;
import java.util.List;

public class PlaintextAggregation {
  public static QueryableDataSet apply(QueryableDataSet input, List<Integer> groups, List<OneDBExpression> selects,
      List<OneDBExpression> aggs) {
    List<AggregateFunction<Row, Comparable>> aggFunctions = new ArrayList<>();
    List<FieldType> types = new ArrayList<>();
    // for (Integer ref : groups) {
    //   types.add(selects.get(ref).getOutType());
    // }
    for (OneDBExpression exp : aggs) {
      aggFunctions.add(PlaintextAggregateFunctions.getAggregateFunc(exp));
      types.add(exp.getOutType());
    }
    Aggregator aggregator = Aggregator.create(groups, aggFunctions, types);
    return applyAggregateFunctions(input, aggregator);
  }

  public static QueryableDataSet applyAggregateFunctions(QueryableDataSet input,
      Aggregator aggregator) {
    // aggregate input rows
    Header.Builder builder = Header.newBuilder();
    aggregator.getOutputTypes().stream().forEach(type -> builder.add("", type));
    QueryableDataSet result = QueryableDataSet.fromHeader(builder.build());
    List<Row> rows = input.getRows();
    for (Row row : rows) {
      aggregator.add(row);
    }
    while (aggregator.hasNext()) {
      result.addRow(aggregator.aggregate());
    }
    return result;
  }
}
