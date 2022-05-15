package com.hufudb.onedb.core.implementor.aggregate;

import java.util.List;
import com.hufudb.onedb.core.data.ColumnType;
import com.hufudb.onedb.core.data.Row;

public interface Aggregator extends AggregateFunction<Row, Row> {
  boolean hasNext();
  void reset();
  List<ColumnType> getOutputTypes();
  int size();

  public static Aggregator create(List<Integer> groups, List<AggregateFunction<Row, Comparable>> aggFunc, List<ColumnType> types) {
    if (groups.isEmpty()) {
      return new SingleAggregator(aggFunc, types);
    } else {
      return new GroupAggregator(groups, aggFunc, types);
    }
  }
}
