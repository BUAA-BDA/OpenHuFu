package com.hufudb.onedb.core.implementor.plaintext;

import com.hufudb.onedb.core.data.ColumnType;
import com.hufudb.onedb.core.data.Schema;
import com.hufudb.onedb.core.data.Row;
import com.hufudb.onedb.core.implementor.QueryableDataSet;
import com.hufudb.onedb.core.implementor.aggregate.AggregateFunction;
import com.hufudb.onedb.core.implementor.aggregate.Aggregator;
import com.hufudb.onedb.core.sql.expression.OneDBExpression;
import java.util.ArrayList;
import java.util.List;

public class PlaintextAggregation {
  public static QueryableDataSet apply(QueryableDataSet input, List<Integer> groups,
      List<OneDBExpression> aggs, List<ColumnType> types) {
    List<AggregateFunction<Row, Comparable>> aggFunctions = new ArrayList<>();
    List<ColumnType> aggTypes = new ArrayList<>();
    for (OneDBExpression exp : aggs) {
      aggFunctions.add(PlaintextAggregateFunctions.createAggregateFunction(exp));
      aggTypes.add(exp.getOutType());
    }
    Aggregator aggregator = Aggregator.create(groups, aggFunctions, aggTypes);
    return applyAggregateFunctions(input, aggregator);
  }

  public static QueryableDataSet applyAggregateFunctions(QueryableDataSet input,
      Aggregator aggregator) {
    // aggregate input rows
    Schema.Builder builder = Schema.newBuilder();
    aggregator.getOutputTypes().stream().forEach(type -> builder.add("", type));
    QueryableDataSet result = PlaintextQueryableDataSet.fromHeader(builder.build());
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
